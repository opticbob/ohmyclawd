package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// HerdrWatcher counts agent panes via herdr's Unix-socket API (pane.list).
type HerdrWatcher struct {
	Sock          string
	Match         []string // agent names counted as sessions (substring, case-insensitive)
	WaitingStatus []string // agent_status values counted as waiting
	Interval      time.Duration
	Timeout       time.Duration
	HealthyFor    time.Duration    // Healthy() while the last success is newer than this
	Now           func() time.Time // injectable for tests; nil -> time.Now

	sessions atomic.Int32
	waiting  atomic.Int32
	lastOK   atomic.Int64 // unix nanos of last successful poll
	seq      atomic.Uint64
}

func NewHerdrWatcher(sock string, match, waiting []string) *HerdrWatcher {
	return &HerdrWatcher{
		Sock:          sock,
		Match:         match,
		WaitingStatus: waiting,
		Interval:      5 * time.Second,
		Timeout:       5 * time.Second,
		HealthyFor:    15 * time.Second,
	}
}

// defaultHerdrSock resolves the socket path from the environment.
func defaultHerdrSock() string {
	for _, k := range []string{"OHMYCLAWD_HERDR_SOCK", "HERDR_SOCK", "HERDR_SOCKET_PATH"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "herdr", "herdr.sock")
}

func (h *HerdrWatcher) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *HerdrWatcher) Healthy() bool {
	last := h.lastOK.Load()
	return last != 0 && h.now().Sub(time.Unix(0, last)) < h.HealthyFor
}

// Sessions and Waiting report 0 once the socket stops answering, rather than
// a stale count.
func (h *HerdrWatcher) Sessions() int {
	if !h.Healthy() {
		return 0
	}
	return int(h.sessions.Load())
}

func (h *HerdrWatcher) Waiting() int {
	if !h.Healthy() {
		return 0
	}
	return int(h.waiting.Load())
}

func (h *HerdrWatcher) Kind() string {
	if h.Healthy() {
		return "herdr"
	}
	return "none"
}

func (h *HerdrWatcher) Run(ctx context.Context) {
	for {
		_ = h.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.Interval):
		}
	}
}

type herdrPane struct {
	PaneID      string `json:"pane_id"`
	Agent       string `json:"agent"`
	AgentStatus string `json:"agent_status"`
}

type herdrResponse struct {
	ID     string `json:"id"`
	Result *struct {
		Panes []herdrPane `json:"panes"`
	} `json:"result"`
	Error json.RawMessage `json:"error"`
}

// poll performs one pane.list round trip and updates the counts.
func (h *HerdrWatcher) poll(ctx context.Context) error {
	d := net.Dialer{Timeout: h.Timeout}
	conn, err := d.DialContext(ctx, "unix", h.Sock)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(h.Timeout))

	req := fmt.Sprintf(`{"id":"o%d","method":"pane.list","params":{}}`+"\n", h.seq.Add(1))
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}
	var resp herdrResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if len(resp.Error) > 0 && string(resp.Error) != "null" {
		return fmt.Errorf("herdr pane.list: %s", resp.Error)
	}
	if resp.Result == nil {
		return errors.New("herdr pane.list: missing result")
	}
	sessions, waiting := h.count(resp.Result.Panes)
	h.sessions.Store(int32(sessions))
	h.waiting.Store(int32(waiting))
	h.lastOK.Store(h.now().UnixNano())
	return nil
}

func (h *HerdrWatcher) count(panes []herdrPane) (sessions, waiting int) {
	for _, p := range panes {
		if p.Agent == "" || !containsFold(h.Match, p.Agent) {
			continue
		}
		sessions++
		for _, s := range h.WaitingStatus {
			if strings.EqualFold(p.AgentStatus, s) {
				waiting++
				break
			}
		}
	}
	return sessions, waiting
}

// containsFold reports whether s contains any of subs, case-insensitively.
func containsFold(subs []string, s string) bool {
	s = strings.ToLower(s)
	for _, sub := range subs {
		if sub != "" && strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}
