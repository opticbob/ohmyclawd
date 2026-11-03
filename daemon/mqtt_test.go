package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBroker accepts MQTT connections and records decoded packets.
type fakeBroker struct {
	ln      net.Listener
	connack func(n int) byte // return code for the nth CONNECT (1-based); nil -> 0
	pong    bool             // answer PINGREQ

	mu       sync.Mutex
	connects []connectInfo
	pubs     []MQTTMessage
	pings    int
	disc     int
	conns    []net.Conn
}

type connectInfo struct {
	proto              string
	level, flags       byte
	keepAlive          int
	clientID           string
	willTopic, willMsg string
	user, pass         string
}

func startBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{ln: ln, pong: true}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close(); b.dropAll() })
	return b
}

func (b *fakeBroker) url() *url.URL { return &url.URL{Scheme: "mqtt", Host: b.ln.Addr().String()} }

func (b *fakeBroker) dropAll() {
	b.mu.Lock()
	for _, c := range b.conns {
		c.Close()
	}
	b.conns = nil
	b.mu.Unlock()
}

func readStr(p []byte) (string, []byte) {
	n := int(binary.BigEndian.Uint16(p))
	return string(p[2 : 2+n]), p[2+n:]
}

func (b *fakeBroker) serve(c net.Conn) {
	defer c.Close()
	b.mu.Lock()
	b.conns = append(b.conns, c)
	b.mu.Unlock()
	r := bufio.NewReader(c)
	for {
		h, err := r.ReadByte()
		if err != nil {
			return
		}
		_ = r.UnreadByte()
		typ, body, err := readMQTTPacket(r)
		if err != nil {
			return
		}
		switch typ {
		case mqttConnect:
			var ci connectInfo
			ci.proto, body = readStr(body)
			ci.level, ci.flags = body[0], body[1]
			ci.keepAlive = int(binary.BigEndian.Uint16(body[2:4]))
			body = body[4:]
			ci.clientID, body = readStr(body)
			if ci.flags&0x04 != 0 {
				ci.willTopic, body = readStr(body)
				ci.willMsg, body = readStr(body)
			}
			if ci.flags&0x80 != 0 {
				ci.user, body = readStr(body)
			}
			if ci.flags&0x40 != 0 {
				ci.pass, _ = readStr(body)
			}
			b.mu.Lock()
			b.connects = append(b.connects, ci)
			n := len(b.connects)
			b.mu.Unlock()
			rc := byte(0)
			if b.connack != nil {
				rc = b.connack(n)
			}
			_, _ = c.Write([]byte{mqttConnack, 2, 0, rc})
			if rc != 0 {
				return
			}
		case mqttPublish:
			topic, payload := readStr(body)
			b.mu.Lock()
			b.pubs = append(b.pubs, MQTTMessage{Topic: topic, Payload: payload, Retain: h&0x01 != 0})
			b.mu.Unlock()
		case mqttPingreq:
			b.mu.Lock()
			b.pings++
			b.mu.Unlock()
			if b.pong {
				_, _ = c.Write([]byte{mqttPingresp, 0})
			}
		case mqttDisconnect:
			b.mu.Lock()
			b.disc++
			b.mu.Unlock()
			return
		}
	}
}

func (b *fakeBroker) snapshot() (cs []connectInfo, pubs []MQTTMessage, pings, disc int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]connectInfo{}, b.connects...), append([]MQTTMessage{}, b.pubs...), b.pings, b.disc
}

func (b *fakeBroker) published(topic, payload string) int {
	_, pubs, _, _ := b.snapshot()
	n := 0
	for _, p := range pubs {
		if p.Topic == topic && string(p.Payload) == payload {
			n++
		}
	}
	return n
}

func runClient(t *testing.T, c *MQTTClient) (cancel func(), done chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel, done
}

func TestMQTTPacketEncoding(t *testing.T) {
	for _, n := range []int{0, 127, 128, 16383, 16384, 2097151} {
		p := mqttPacket(mqttPublish, make([]byte, n))
		typ, body, err := readMQTTPacket(bufio.NewReader(strings.NewReader(string(p))))
		if err != nil || typ != mqttPublish || len(body) != n {
			t.Fatalf("n=%d: typ=%x len=%d err=%v", n, typ, len(body), err)
		}
	}
	got := publishPacket(MQTTMessage{Topic: "a/b", Payload: []byte("hi"), Retain: true})
	want := []byte{0x31, 7, 0, 3, 'a', '/', 'b', 'h', 'i'}
	if string(got) != string(want) {
		t.Fatalf("retained publish = % x, want % x", got, want)
	}
}

func TestMQTTConnectWillAndShutdown(t *testing.T) {
	b := startBroker(t)
	c := NewMQTTClient(b.url(), "ohmyclawd", "s3cret", "ohmyclawd-node", "omc/node/status")
	c.Publish(MQTTMessage{Topic: "omc/node/agents", Payload: []byte(`{"n":1}`), Retain: true})
	cancel, done := runClient(t, c)
	waitFor(t, "agents", func() bool { return b.published("omc/node/agents", `{"n":1}`) == 1 })

	cs, pubs, _, _ := b.snapshot()
	want := connectInfo{proto: "MQTT", level: 4, flags: 0x02 | 0x04 | 0x20 | 0x80 | 0x40, keepAlive: 30,
		clientID: "ohmyclawd-node", willTopic: "omc/node/status", willMsg: "offline", user: "ohmyclawd", pass: "s3cret"}
	if len(cs) != 1 || cs[0] != want {
		t.Fatalf("connect = %+v\nwant      %+v", cs, want)
	}
	if pubs[0].Topic != "omc/node/status" || string(pubs[0].Payload) != "online" || !pubs[0].Retain {
		t.Fatalf("first publish = %+v, want retained online", pubs[0])
	}

	cancel()
	<-done
	waitFor(t, "disconnect", func() bool { _, _, _, d := b.snapshot(); return d == 1 })
	_, pubs, _, _ = b.snapshot()
	if last := pubs[len(pubs)-1]; string(last.Payload) != "offline" || !last.Retain {
		t.Fatalf("last publish = %+v, want retained offline", last)
	}
}

func TestMQTTKeepalivePings(t *testing.T) {
	b := startBroker(t)
	c := NewMQTTClient(b.url(), "", "", "id", "")
	c.KeepAlive = 100 * time.Millisecond
	runClient(t, c)
	waitFor(t, "pings", func() bool { _, _, p, _ := b.snapshot(); return p >= 3 })
	if cs, _, _, _ := b.snapshot(); len(cs) != 1 || cs[0].flags != 0x02 || cs[0].keepAlive != 1 {
		t.Fatalf("connect = %+v (no user, no will, keepalive rounded up to 1s)", cs)
	}
}

func TestMQTTPingTimeoutReconnects(t *testing.T) {
	b := startBroker(t)
	b.pong = false
	c := NewMQTTClient(b.url(), "", "", "id", "")
	c.KeepAlive, c.MinBackoff = 100*time.Millisecond, 10*time.Millisecond
	runClient(t, c)
	waitFor(t, "reconnect", func() bool { cs, _, _, _ := b.snapshot(); return len(cs) >= 2 })
}

func TestMQTTReconnectRepublishesRetained(t *testing.T) {
	b := startBroker(t)
	m := NewMetrics()
	c := NewMQTTClient(b.url(), "", "", "id", "s/status")
	c.MinBackoff, c.Metrics = 10*time.Millisecond, m
	c.Publish(MQTTMessage{Topic: "cfg", Payload: []byte("c"), Retain: true, Label: "config"})
	c.Publish(MQTTMessage{Topic: "agents", Payload: []byte("1"), Retain: true, Label: "agents"})
	runClient(t, c)
	waitFor(t, "first session", func() bool { return b.published("agents", "1") == 1 })

	b.dropAll()
	// While disconnected, only the latest retained value per topic survives.
	c.Publish(MQTTMessage{Topic: "agents", Payload: []byte("2"), Retain: true, Label: "agents"})
	c.Publish(MQTTMessage{Topic: "agents", Payload: []byte("3"), Retain: true, Label: "agents"})
	waitFor(t, "republish", func() bool { return b.published("agents", "3") >= 1 && b.published("cfg", "c") == 2 })
	if b.published("agents", "2") != 0 {
		t.Fatal("superseded retained value was published")
	}
	if b.published("s/status", "online") != 2 {
		t.Fatal("online not republished after reconnect")
	}
	// Discovery configs go out before state on every session.
	_, pubs, _, _ := b.snapshot()
	var order []string
	for _, p := range pubs {
		order = append(order, p.Topic)
	}
	if s := strings.Join(order, ","); !strings.HasPrefix(s, "s/status,cfg,agents,") {
		t.Fatalf("order = %s", s)
	}
	m.mu.Lock()
	configs := m.mqttPub["config"]
	m.mu.Unlock()
	if m.mqttReconnects.Load() < 1 || configs != 2 {
		t.Fatalf("metrics: reconnects=%d config=%d", m.mqttReconnects.Load(), configs)
	}
}

func TestMQTTNonRetainedQueueDropsOldest(t *testing.T) {
	c := NewMQTTClient(&url.URL{Scheme: "mqtt", Host: "x"}, "", "", "id", "")
	for i := 0; i < mqttMaxQueue+5; i++ {
		c.Publish(MQTTMessage{Topic: "event", Payload: []byte(fmt.Sprint(i))})
	}
	if len(c.queue) != mqttMaxQueue || string(c.queue[0].Payload) != "5" {
		t.Fatalf("queue len=%d first=%s", len(c.queue), c.queue[0].Payload)
	}
}

func TestMQTTBadConnackBacksOff(t *testing.T) {
	b := startBroker(t)
	b.connack = func(n int) byte {
		if n <= 2 {
			return 5 // not authorized
		}
		return 0
	}
	c := NewMQTTClient(b.url(), "", "", "id", "st")
	c.MinBackoff = 50 * time.Millisecond
	start := time.Now()
	runClient(t, c)
	waitFor(t, "online after refusals", func() bool { return b.published("st", "online") == 1 })
	// Two refusals: waits of 50ms then 100ms.
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("connected after %v; backoff not applied", d)
	}
	if cs, _, _, _ := b.snapshot(); len(cs) != 3 {
		t.Fatalf("connect attempts = %d, want 3", len(cs))
	}
}

func TestParseMQTTURL(t *testing.T) {
	for in, ok := range map[string]bool{
		"mqtt://h:1883": true, "mqtts://h": true, "mqtt://h": true,
		"tcp://h:1883": false, "h:1883": false, "mqtt://": false,
	} {
		if _, err := parseMQTTURL(in); (err == nil) != ok {
			t.Errorf("%s: err=%v", in, err)
		}
	}
}

// TestMQTTMosquitto runs against a real broker when mosquitto is installed.
func TestMQTTMosquitto(t *testing.T) {
	bin, err := exec.LookPath("mosquitto")
	if err != nil {
		t.Skip("mosquitto not installed")
	}
	sub, err := exec.LookPath("mosquitto_sub")
	if err != nil {
		t.Skip("mosquitto_sub not installed")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	conf := t.TempDir() + "/m.conf"
	_ = os.WriteFile(conf, []byte(fmt.Sprintf("listener %d 127.0.0.1\nallow_anonymous true\n", port)), 0o600)
	srv := exec.Command(bin, "-c", conf)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()
	time.Sleep(300 * time.Millisecond)

	c := NewMQTTClient(&url.URL{Scheme: "mqtt", Host: fmt.Sprintf("127.0.0.1:%d", port)}, "", "", "omc-test", "omc/t/status")
	c.Publish(MQTTMessage{Topic: "omc/t/agents", Payload: []byte(`{"ok":1}`), Retain: true})
	runClient(t, c)
	time.Sleep(300 * time.Millisecond)
	out, err := exec.Command(sub, "-h", "127.0.0.1", "-p", fmt.Sprint(port), "-t", "omc/t/#", "-v", "-W", "1").Output()
	if !strings.Contains(string(out), `omc/t/agents {"ok":1}`) || !strings.Contains(string(out), "omc/t/status online") {
		t.Fatalf("mosquitto_sub: %q (%v)", out, err)
	}
}
