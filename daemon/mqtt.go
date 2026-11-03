package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// A minimal publish-only MQTT 3.1.1 client: CONNECT with a will, QoS 0
// PUBLISH, keepalive pings and DISCONNECT. Nothing is ever subscribed to.

const (
	mqttConnect    = 0x10
	mqttConnack    = 0x20
	mqttPublish    = 0x30
	mqttPingreq    = 0xC0
	mqttPingresp   = 0xD0
	mqttDisconnect = 0xE0

	mqttMaxQueue = 32 // non-retained messages held while the broker is unreachable
)

// MQTTMessage is one PUBLISH. Label names the topic kind for metrics.
type MQTTMessage struct {
	Topic   string
	Payload []byte
	Retain  bool
	Label   string
}

type MQTTClient struct {
	URL       *url.URL // mqtt://host[:1883] or mqtts://host[:8883]
	User      string
	Password  string
	ClientID  string
	WillTopic string        // gets "offline" (retained) as the will and on clean shutdown, "online" after connecting
	KeepAlive time.Duration // 0 -> 30s
	Metrics   *Metrics      // optional

	MinBackoff, MaxBackoff time.Duration // 0 -> 1s, 60s

	mu       sync.Mutex
	order    []string // retained topics, first-publish order
	retained map[string]MQTTMessage
	dirty    map[string]bool
	queue    []MQTTMessage // non-retained, oldest dropped when full
	notify   chan struct{}
}

func NewMQTTClient(u *url.URL, user, password, clientID, willTopic string) *MQTTClient {
	return &MQTTClient{
		URL: u, User: user, Password: password, ClientID: clientID, WillTopic: willTopic,
		retained: make(map[string]MQTTMessage),
		dirty:    make(map[string]bool),
		notify:   make(chan struct{}, 1),
	}
}

// parseMQTTURL accepts mqtt:// and mqtts:// URLs.
func parseMQTTURL(s string) (*url.URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "mqtt" && u.Scheme != "mqtts") || u.Hostname() == "" {
		return nil, fmt.Errorf("OHMYCLAWD_MQTT_URL: want mqtt://host:1883 or mqtts://host:8883, got %q", s)
	}
	return u, nil
}

// Publish queues m without blocking. A retained message replaces any pending
// value for its topic and is republished after every reconnect.
func (c *MQTTClient) Publish(m MQTTMessage) {
	c.mu.Lock()
	if m.Retain {
		if _, ok := c.retained[m.Topic]; !ok {
			c.order = append(c.order, m.Topic)
		}
		c.retained[m.Topic] = m
		c.dirty[m.Topic] = true
	} else {
		if len(c.queue) >= mqttMaxQueue {
			c.queue = c.queue[1:]
		}
		c.queue = append(c.queue, m)
	}
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// Run connects and keeps the session alive until ctx is cancelled, then
// publishes "offline" and disconnects. It reconnects with backoff.
func (c *MQTTClient) Run(ctx context.Context) {
	minB, maxB := c.MinBackoff, c.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB <= 0 {
		maxB = 60 * time.Second
	}
	backoff := minB
	for attempt := 0; ; attempt++ {
		if attempt > 0 && c.Metrics != nil {
			c.Metrics.mqttReconnects.Add(1)
		}
		conn, err := c.connect(ctx)
		if err == nil {
			log.Printf("mqtt: connected to %s", c.URL.Host)
			backoff = minB
			c.setConnected(true)
			err = c.session(ctx, conn)
			c.setConnected(false)
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("mqtt: %v; retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxB {
			backoff = maxB
		}
	}
}

func (c *MQTTClient) setConnected(v bool) {
	if c.Metrics != nil {
		c.Metrics.mqttConnected.Store(v)
	}
}

func (c *MQTTClient) keepAlive() time.Duration {
	if c.KeepAlive > 0 {
		return c.KeepAlive
	}
	return 30 * time.Second
}

func (c *MQTTClient) connect(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	host := c.URL.Host
	var conn net.Conn
	var err error
	if c.URL.Scheme == "mqtts" {
		if c.URL.Port() == "" {
			host = net.JoinHostPort(c.URL.Hostname(), "8883")
		}
		td := tls.Dialer{NetDialer: &d, Config: &tls.Config{ServerName: c.URL.Hostname()}}
		conn, err = td.DialContext(ctx, "tcp", host)
	} else {
		if c.URL.Port() == "" {
			host = net.JoinHostPort(c.URL.Hostname(), "1883")
		}
		conn, err = d.DialContext(ctx, "tcp", host)
	}
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(c.connectPacket()); err != nil {
		conn.Close()
		return nil, err
	}
	typ, body, err := readMQTTPacket(bufio.NewReader(conn))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connack: %w", err)
	}
	if typ != mqttConnack || len(body) != 2 {
		conn.Close()
		return nil, fmt.Errorf("connack: unexpected packet 0x%02x", typ)
	}
	if rc := body[1]; rc != 0 {
		conn.Close()
		return nil, fmt.Errorf("connack: refused (%s)", connackReason(rc))
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func connackReason(rc byte) string {
	switch rc {
	case 1:
		return "unacceptable protocol version"
	case 2:
		return "identifier rejected"
	case 3:
		return "server unavailable"
	case 4:
		return "bad username or password"
	case 5:
		return "not authorized"
	}
	return fmt.Sprintf("code %d", rc)
}

func (c *MQTTClient) connectPacket() []byte {
	flags := byte(0x02) // clean session
	if c.WillTopic != "" {
		flags |= 0x04 | 0x20 // will flag, will retain, QoS 0
	}
	if c.User != "" {
		flags |= 0x80
		if c.Password != "" {
			flags |= 0x40
		}
	}
	ka := int((c.keepAlive() + time.Second - 1) / time.Second)
	b := appendMQTTString(nil, "MQTT")
	b = append(b, 4, flags, byte(ka>>8), byte(ka))
	b = appendMQTTString(b, c.ClientID)
	if c.WillTopic != "" {
		b = appendMQTTString(b, c.WillTopic)
		b = appendMQTTString(b, "offline")
	}
	if c.User != "" {
		b = appendMQTTString(b, c.User)
		if c.Password != "" {
			b = appendMQTTString(b, c.Password)
		}
	}
	return mqttPacket(mqttConnect, b)
}

func publishPacket(m MQTTMessage) []byte {
	h := byte(mqttPublish)
	if m.Retain {
		h |= 0x01
	}
	return mqttPacket(h, append(appendMQTTString(nil, m.Topic), m.Payload...))
}

var errPingTimeout = errors.New("no PINGRESP from broker")

// session runs one connection: "online", then pending messages, pings, and
// on ctx cancellation "offline" + DISCONNECT.
func (c *MQTTClient) session(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	var pongs atomic.Int64
	readErr := make(chan error, 1)
	go func() {
		r := bufio.NewReader(conn)
		for {
			typ, _, err := readMQTTPacket(r)
			if err != nil {
				readErr <- err
				return
			}
			if typ == mqttPingresp {
				pongs.Add(1)
			}
		}
	}()

	write := func(b []byte) error {
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err := conn.Write(b)
		return err
	}
	status := func(s string) error {
		if c.WillTopic == "" {
			return nil
		}
		return c.write(write, MQTTMessage{Topic: c.WillTopic, Payload: []byte(s), Retain: true, Label: "status"})
	}

	c.markAllDirty() // a fresh session republishes every retained value
	if err := status("online"); err != nil {
		return err
	}
	ping := time.NewTicker(c.keepAlive() / 2)
	defer ping.Stop()
	var sent, seen int64
	for {
		if err := c.flush(write); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			_ = status("offline")
			_ = write([]byte{mqttDisconnect, 0})
			return ctx.Err()
		case err := <-readErr:
			return fmt.Errorf("connection lost: %w", err)
		case <-c.notify:
		case <-ping.C:
			if seen = pongs.Load(); sent > seen {
				return errPingTimeout
			}
			if err := write([]byte{mqttPingreq, 0}); err != nil {
				return err
			}
			sent++
		}
	}
}

func (c *MQTTClient) write(write func([]byte) error, m MQTTMessage) error {
	if err := write(publishPacket(m)); err != nil {
		return err
	}
	if c.Metrics != nil {
		c.Metrics.MQTTPublished(m.Label)
	}
	return nil
}

func (c *MQTTClient) markAllDirty() {
	c.mu.Lock()
	for _, t := range c.order {
		c.dirty[t] = true
	}
	c.mu.Unlock()
}

// flush writes dirty retained topics in first-publish order (discovery
// configs before state), then queued non-retained messages.
func (c *MQTTClient) flush(write func([]byte) error) error {
	c.mu.Lock()
	var out []MQTTMessage
	for _, t := range c.order {
		if c.dirty[t] {
			out = append(out, c.retained[t])
			delete(c.dirty, t)
		}
	}
	out = append(out, c.queue...)
	c.queue = nil
	c.mu.Unlock()
	for i, m := range out {
		if err := c.write(write, m); err != nil {
			// Retained values get republished on reconnect; requeue the rest.
			c.mu.Lock()
			for _, r := range out[i:] {
				if !r.Retain && len(c.queue) < mqttMaxQueue {
					c.queue = append(c.queue, r)
				}
			}
			c.mu.Unlock()
			return err
		}
	}
	return nil
}

func appendMQTTString(b []byte, s string) []byte {
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

// mqttPacket prefixes body with the fixed header and varint remaining length.
func mqttPacket(header byte, body []byte) []byte {
	b := []byte{header}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		b = append(b, d)
		if n == 0 {
			break
		}
	}
	return append(b, body...)
}

// readMQTTPacket reads one packet, returning the type nibble (flags masked
// off) and the body.
func readMQTTPacket(r *bufio.Reader) (byte, []byte, error) {
	h, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, mult := 0, 1
	for i := 0; ; i++ {
		if i == 4 {
			return 0, nil, errors.New("malformed remaining length")
		}
		d, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		n += int(d&0x7f) * mult
		if d&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return h & 0xF0, body, nil
}
