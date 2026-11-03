package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHerdr serves pane.list over a Unix socket. reply builds the response
// line for each request; returning "" makes the server hang until closed.
type fakeHerdr struct {
	path  string
	ln    net.Listener
	reply func(req map[string]any) string
	reqs  atomic.Int32
}

// shortSockPath avoids t.TempDir(): macOS temp paths can exceed the
// 104-byte sun_path limit.
func shortSockPath(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets")
	}
	dir, err := os.MkdirTemp("", "omc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "h.sock")
}

func startFakeHerdr(t *testing.T, path string, reply func(map[string]any) string) *fakeHerdr {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeHerdr{path: path, ln: ln, reply: reply}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(f.Close)
	return f
}

func (f *fakeHerdr) serve(c net.Conn) {
	defer c.Close()
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	f.reqs.Add(1)
	var req map[string]any
	_ = json.Unmarshal([]byte(line), &req)
	resp := f.reply(req)
	if resp == "" {
		_, _ = c.Read(make([]byte, 1)) // hang until the client gives up
		return
	}
	_, _ = c.Write([]byte(resp + "\n"))
}

func (f *fakeHerdr) Close() { _ = f.ln.Close() }

func panesReply(panes ...herdrPane) func(map[string]any) string {
	return func(req map[string]any) string {
		b, _ := json.Marshal(map[string]any{
			"id":     req["id"],
			"result": map[string]any{"type": "pane_list", "panes": panes},
		})
		return string(b)
	}
}

func TestHerdrPoll(t *testing.T) {
	realFixture, err := os.ReadFile("testdata/herdr_pane_list_real.json")
	if err != nil {
		t.Fatal(err)
	}
	compact := strings.Join(strings.Fields(string(realFixture)), " ")

	cases := []struct {
		name         string
		reply        func(map[string]any) string
		wantErr      bool
		wantSessions int
		wantWaiting  int
	}{
		{
			name:         "real fixture",
			reply:        func(map[string]any) string { return compact },
			wantSessions: 5, // four idle + one working claude pane
			wantWaiting:  0,
		},
		{name: "no panes", reply: panesReply()},
		{
			name:  "non-claude agent",
			reply: panesReply(herdrPane{PaneID: "a", Agent: "codex", AgentStatus: "blocked"}, herdrPane{PaneID: "b", AgentStatus: "unknown"}),
		},
		{
			name: "mixed statuses",
			reply: panesReply(
				herdrPane{PaneID: "1", Agent: "claude", AgentStatus: "working"},
				herdrPane{PaneID: "2", Agent: "claude", AgentStatus: "blocked"},
				herdrPane{PaneID: "3", Agent: "Claude", AgentStatus: "done"},
				herdrPane{PaneID: "4", Agent: "claude", AgentStatus: "idle"},
				herdrPane{PaneID: "5", Agent: "kiro", AgentStatus: "BLOCKED"},
				herdrPane{PaneID: "6", Agent: "codex", AgentStatus: "blocked"},
			),
			wantSessions: 5,
			wantWaiting:  3,
		},
		{
			name:    "error response",
			reply:   func(map[string]any) string { return `{"id":"o1","error":{"code":"internal","message":"nope"}}` },
			wantErr: true,
		},
		{name: "malformed json", reply: func(map[string]any) string { return `{"id":` }, wantErr: true},
		{name: "slow server hits deadline", reply: func(map[string]any) string { return "" }, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := shortSockPath(t)
			startFakeHerdr(t, path, tc.reply)
			h := NewHerdrWatcher(path, []string{"claude", "kiro"}, []string{"blocked", "done"})
			h.Timeout = 200 * time.Millisecond

			start := time.Now()
			err := h.poll(context.Background())
			if time.Since(start) > 2*time.Second {
				t.Fatalf("poll took %v", time.Since(start))
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if h.Healthy() || h.Kind() != "none" {
					t.Fatal("unhealthy watcher reported healthy")
				}
				return
			}
			if !h.Healthy() || h.Kind() != "herdr" {
				t.Fatal("expected healthy after successful poll")
			}
			if h.Sessions() != tc.wantSessions || h.Waiting() != tc.wantWaiting {
				t.Fatalf("sessions=%d waiting=%d, want %d/%d", h.Sessions(), h.Waiting(), tc.wantSessions, tc.wantWaiting)
			}
		})
	}
}

func TestHerdrRequestShape(t *testing.T) {
	path := shortSockPath(t)
	var got []map[string]any
	reply := panesReply()
	startFakeHerdr(t, path, func(req map[string]any) string {
		got = append(got, req)
		return reply(req)
	})
	h := NewHerdrWatcher(path, nil, nil)
	for i := 0; i < 2; i++ {
		if err := h.poll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0]["method"] != "pane.list" || got[0]["id"] != "o1" || got[1]["id"] != "o2" {
		t.Fatalf("requests = %v", got)
	}
}

func TestHerdrGoesStale(t *testing.T) {
	path := shortSockPath(t)
	startFakeHerdr(t, path, panesReply(herdrPane{Agent: "claude", AgentStatus: "blocked"}))
	now := time.Unix(1000, 0)
	h := NewHerdrWatcher(path, []string{"claude"}, []string{"blocked"})
	h.Now = func() time.Time { return now }
	if err := h.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.Sessions() != 1 || h.Waiting() != 1 {
		t.Fatalf("sessions=%d waiting=%d", h.Sessions(), h.Waiting())
	}
	now = now.Add(16 * time.Second)
	if h.Healthy() || h.Sessions() != 0 || h.Waiting() != 0 {
		t.Fatal("stale herdr data still reported")
	}
}

// staticWatcher is a fallback with fixed counts.
type staticWatcher struct {
	kind           string
	healthy        bool
	sessions, wait int
}

func (s *staticWatcher) Sessions() int           { return s.sessions }
func (s *staticWatcher) Waiting() int            { return s.wait }
func (s *staticWatcher) Kind() string            { return s.kind }
func (s *staticWatcher) Healthy() bool           { return s.healthy }
func (s *staticWatcher) Run(ctx context.Context) { <-ctx.Done() }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAutoWatcherFailover(t *testing.T) {
	path := shortSockPath(t)
	reply := panesReply(herdrPane{Agent: "claude", AgentStatus: "blocked"}, herdrPane{Agent: "claude", AgentStatus: "working"})
	srv := startFakeHerdr(t, path, reply)

	h := NewHerdrWatcher(path, []string{"claude"}, []string{"blocked", "done"})
	h.Interval, h.Timeout, h.HealthyFor = 10*time.Millisecond, 50*time.Millisecond, 60*time.Millisecond
	tmux := &staticWatcher{kind: "tmux", healthy: true, sessions: 7, wait: 3}
	a := &AutoWatcher{Primary: h, Fallback: tmux}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx)

	waitFor(t, "herdr", func() bool { return a.Kind() == "herdr" })
	if a.Sessions() != 2 || a.Waiting() != 1 {
		t.Fatalf("herdr counts: %d/%d", a.Sessions(), a.Waiting())
	}

	srv.Close()
	os.Remove(path)
	waitFor(t, "tmux fallback", func() bool { return a.Kind() == "tmux" })
	if a.Sessions() != 7 || a.Waiting() != 3 {
		t.Fatalf("tmux counts: %d/%d", a.Sessions(), a.Waiting())
	}

	startFakeHerdr(t, path, reply)
	waitFor(t, "herdr recovery", func() bool { return a.Kind() == "herdr" })

	tmux.healthy = false
	if a.Sessions() != 2 {
		t.Fatalf("sessions = %d", a.Sessions())
	}
}

func TestAutoWatcherNeitherHealthy(t *testing.T) {
	a := &AutoWatcher{
		Primary:  &staticWatcher{kind: "herdr", sessions: 4},
		Fallback: &staticWatcher{kind: "tmux", sessions: 5},
	}
	if a.Kind() != "none" || a.Sessions() != 0 || a.Waiting() != 0 {
		t.Fatalf("kind=%s sessions=%d", a.Kind(), a.Sessions())
	}
}

func TestNewSessionWatcherModes(t *testing.T) {
	for mode, want := range map[string]string{
		"auto":  "*main.AutoWatcher",
		"herdr": "*main.HerdrWatcher",
		"tmux":  "*main.TmuxWatcher",
		"off":   "main.noopWatcher",
	} {
		w, err := newSessionWatcher(mode, "/nonexistent", nil, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if got := typeName(w); got != want {
			t.Errorf("%s -> %s, want %s", mode, got, want)
		}
	}
	if _, err := newSessionWatcher("bogus", "", nil, nil, 0); err == nil {
		t.Error("bogus mode: want error")
	}
}

func typeName(v any) string { return fmt.Sprintf("%T", v) }
