package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSortAgents(t *testing.T) {
	as := []Agent{
		{ID: "a", Status: "idle", Since: 1},
		{ID: "b", Status: "working", Since: 5},
		{ID: "c", Status: "blocked", Since: 9},
		{ID: "d", Status: "unknown", Since: 1},
		{ID: "e", Status: "done", Since: 3},
		{ID: "f", Status: "blocked", Since: 2},
		{ID: "g", Status: "weird", Since: 0},
	}
	sortAgents(as)
	var got []string
	for _, a := range as {
		got = append(got, a.ID)
	}
	if want := []string{"f", "c", "e", "b", "a", "d", "g"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestSetDisplayDirs(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		want  []string
	}{
		{"basenames", []string{"/h/x/vets-api", "/h/y/dewey"}, []string{"vets-api", "dewey"}},
		{"same path twice", []string{"/h/x/api", "/h/x/api"}, []string{"api", "api"}},
		{"collision", []string{"/h/work/api", "/h/play/api", "/h/z/web"}, []string{"work/api", "play/api", "web"}},
		{"home", []string{"/h", "/h/sub"}, []string{"~", "sub"}},
		{"empty", []string{""}, []string{"?"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			as := make([]Agent, len(tc.paths))
			for i, p := range tc.paths {
				as[i].path = p
			}
			setDisplayDirs(as, "/h")
			for i, a := range as {
				if a.Dir != tc.want[i] {
					t.Errorf("%s -> %q, want %q", tc.paths[i], a.Dir, tc.want[i])
				}
			}
		})
	}
}

func newTestWatcher(now *time.Time) *HerdrWatcher {
	h := NewHerdrWatcher("", []string{"claude"}, []string{"blocked"})
	h.Home = "/h"
	h.Now = func() time.Time { return *now }
	h.HealthyFor = 24 * time.Hour // tests jump the clock; health has its own tests
	return h
}

func drain(ch <-chan struct{}) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}

func TestSincePreservedAcrossReconcile(t *testing.T) {
	now := time.Unix(1000, 0)
	h := newTestWatcher(&now)
	h.applyList([]herdrPane{
		{PaneID: "p1", Agent: "claude", AgentStatus: "working", Cwd: "/h/a"},
		{PaneID: "p2", Agent: "claude", AgentStatus: "blocked", Cwd: "/h/b"},
	})
	now = now.Add(time.Minute)
	h.applyList([]herdrPane{
		{PaneID: "p1", Agent: "claude", AgentStatus: "working", Cwd: "/h/a"},
		{PaneID: "p2", Agent: "claude", AgentStatus: "done", Cwd: "/h/b"},
	})
	got := map[string]int64{}
	for _, a := range h.Agents() {
		got[a.ID] = a.Since
	}
	if got["p1"] != 1000 || got["p2"] != 1060 {
		t.Fatalf("since = %v", got)
	}
}

func TestChangedFiresOncePerRealChange(t *testing.T) {
	now := time.Unix(1000, 0)
	h := newTestWatcher(&now)
	panes := []herdrPane{{PaneID: "p1", Agent: "claude", AgentStatus: "working", Cwd: "/h/a"}}

	h.applyList(panes)
	if n := drain(h.Changed()); n != 1 {
		t.Fatalf("first list: %d fires", n)
	}
	now = now.Add(time.Second)
	h.applyList(panes) // identical
	h.applyStatus("p1", "claude", "working")
	if n := drain(h.Changed()); n != 0 {
		t.Fatalf("no-op updates fired %d times", n)
	}
	h.applyStatus("p1", "", "blocked")
	if n := drain(h.Changed()); n != 1 {
		t.Fatalf("status change: %d fires", n)
	}
	if a := h.Agents()[0]; a.Status != "blocked" || a.Agent != "claude" || a.Since != 1001 {
		t.Fatalf("agent = %+v", a)
	}
	h.applyStatus("nope", "", "blocked") // unknown pane without agent: ignored
	if n := drain(h.Changed()); n != 0 {
		t.Fatalf("unknown pane fired %d times", n)
	}
}

func TestAgentsIncludeEveryDetectedAgent(t *testing.T) {
	now := time.Unix(1000, 0)
	h := newTestWatcher(&now)
	h.applyList([]herdrPane{
		{PaneID: "1", Agent: "claude", AgentStatus: "blocked"},
		{PaneID: "2", Agent: "codex", AgentStatus: "blocked"},
		{PaneID: "3", AgentStatus: "unknown"}, // plain shell
	})
	if n := len(h.Agents()); n != 2 {
		t.Fatalf("agents = %d, want 2 (codex included)", n)
	}
	if h.Sessions() != 1 || h.Waiting() != 1 {
		t.Fatalf("legacy counts %d/%d use AGENT_MATCH", h.Sessions(), h.Waiting())
	}
	if c := h.Counts(); c["blocked"] != 2 || c["done"] != 0 || len(c) != 5 {
		t.Fatalf("counts = %v", c)
	}
}

func TestBuildAgentsPayload(t *testing.T) {
	now := time.Unix(1000, 0)
	h := newTestWatcher(&now)
	var panes []herdrPane
	for _, id := range []string{"a", "b", "c"} {
		panes = append(panes, herdrPane{PaneID: id, Agent: "claude", AgentStatus: "idle", Cwd: "/h/" + id})
	}
	h.applyList(panes)

	p := buildAgentsPayload(h, 2, now)
	b, _ := json.Marshal(p)
	want := `{"ts":1000,"sw":"herdr","n":{"blocked":0,"done":0,"idle":3,"unknown":0,"working":0},` +
		`"a":[{"id":"a","d":"a","g":"claude","s":"idle","t":1000},{"id":"b","d":"b","g":"claude","s":"idle","t":1000}],"more":1}`
	if string(b) != want {
		t.Fatalf("payload\n got %s\nwant %s", b, want)
	}

	tmuxOnly := &AutoWatcher{Primary: NewHerdrWatcher("", nil, nil), Fallback: &staticWatcher{kind: "tmux", healthy: true}}
	b, _ = json.Marshal(buildAgentsPayload(tmuxOnly, 12, now))
	if !strings.Contains(string(b), `"sw":"tmux"`) || !strings.Contains(string(b), `"a":[]`) {
		t.Fatalf("tmux payload = %s", b)
	}
}

// eventHerdr is a fake herdr that answers pane.list from a mutable pane set
// and holds events.subscribe connections open, streaming whatever is sent on
// the events channel.
type eventHerdr struct {
	ln       net.Listener
	mu       sync.Mutex
	panes    []herdrPane
	noEvents bool // reply to events.subscribe with an error
	events   chan string
	lists    atomic.Int32
	subs     atomic.Int32
	lastSub  atomic.Value // []any subscriptions of the latest subscribe
}

func startEventHerdr(t *testing.T, path string, panes ...herdrPane) *eventHerdr {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &eventHerdr{ln: ln, panes: panes, events: make(chan string, 16)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *eventHerdr) setPanes(p ...herdrPane) {
	f.mu.Lock()
	f.panes = p
	f.mu.Unlock()
}

func (f *eventHerdr) serve(c net.Conn) {
	defer c.Close()
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	var req struct {
		ID     string `json:"id"`
		Method string `json:"method"`
		Params struct {
			Subscriptions []any `json:"subscriptions"`
		} `json:"params"`
	}
	_ = json.Unmarshal([]byte(line), &req)
	switch req.Method {
	case "pane.list":
		f.lists.Add(1)
		f.mu.Lock()
		b, _ := json.Marshal(map[string]any{"id": req.ID, "result": map[string]any{"type": "pane_list", "panes": f.panes}})
		f.mu.Unlock()
		_, _ = c.Write(append(b, '\n'))
	case "events.subscribe":
		f.subs.Add(1)
		f.lastSub.Store(req.Params.Subscriptions)
		if f.noEvents {
			_, _ = c.Write([]byte(`{"id":"` + req.ID + `","error":{"code":"unknown_method","message":"no"}}` + "\n"))
			return
		}
		_, _ = c.Write([]byte(`{"id":"` + req.ID + `","result":{"type":"subscription_started"}}` + "\n"))
		for ev := range f.events {
			if _, err := c.Write([]byte(ev + "\n")); err != nil {
				return
			}
			if strings.Contains(ev, "events_lost") {
				return
			}
		}
	}
}

func runWatcher(t *testing.T, h *HerdrWatcher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
}

func statusOf(h *HerdrWatcher, id string) string {
	for _, a := range h.Agents() {
		if a.ID == id {
			return a.Status
		}
	}
	return ""
}

func fastWatcher(path string) *HerdrWatcher {
	h := NewHerdrWatcher(path, []string{"claude"}, []string{"blocked"})
	h.Interval, h.Timeout, h.Reconcile = 20*time.Millisecond, 200*time.Millisecond, time.Hour
	return h
}

func TestHerdrEventsCaptured(t *testing.T) {
	data, err := os.ReadFile("testdata/herdr_events_real.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	last := map[string]string{} // pane -> final status from the capture
	ids := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var ev herdrEvent
		if json.Unmarshal([]byte(line), &ev) != nil || ev.Event != "pane.agent_status_changed" {
			continue
		}
		events = append(events, line)
		last[ev.Data.PaneID] = ev.Data.AgentStatus
		ids[ev.Data.PaneID] = true
	}
	if len(events) == 0 {
		t.Fatal("fixture has no pane.agent_status_changed events")
	}
	var panes []herdrPane
	for id := range ids {
		panes = append(panes, herdrPane{PaneID: id, Agent: "claude", AgentStatus: "unknown", Cwd: "/h/" + id})
	}
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, panes...)
	h := fastWatcher(path)
	runWatcher(t, h)
	waitFor(t, "subscribe", func() bool { return srv.subs.Load() == 1 })

	for _, ev := range events {
		srv.events <- ev
	}
	waitFor(t, "final statuses", func() bool {
		for id, st := range last {
			if statusOf(h, id) != st {
				return false
			}
		}
		return true
	})
	// Lifecycle by type, status per pane (herdr requires pane_id for it).
	subs := srv.lastSub.Load().([]any)
	if len(subs) != len(lifecycleEvents)+len(ids) {
		t.Fatalf("subscriptions = %v", subs)
	}
}

func TestHerdrReconcileFixesMissedEvent(t *testing.T) {
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "working"})
	h := fastWatcher(path)
	h.Reconcile = 30 * time.Millisecond
	runWatcher(t, h)
	waitFor(t, "working", func() bool { return statusOf(h, "p1") == "working" })

	srv.setPanes(herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "blocked"}) // no event sent
	waitFor(t, "reconciled to blocked", func() bool { return statusOf(h, "p1") == "blocked" })
	if srv.subs.Load() != 1 {
		t.Fatalf("same pane set should keep the subscription, got %d subscribes", srv.subs.Load())
	}
}

func TestHerdrEventsLostResyncs(t *testing.T) {
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "working"})
	h := fastWatcher(path)
	runWatcher(t, h)
	waitFor(t, "subscribe", func() bool { return srv.subs.Load() == 1 })

	srv.setPanes(herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "done"})
	srv.events <- `{"event":"events_lost","data":{}}`
	waitFor(t, "resync", func() bool { return statusOf(h, "p1") == "done" })
	waitFor(t, "resubscribe", func() bool { return srv.subs.Load() == 2 })
}

func TestHerdrStaysHealthyBetweenReconciles(t *testing.T) {
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "idle"})
	h := fastWatcher(path) // Reconcile is an hour: only the open connection keeps it healthy
	h.HealthyFor = 100 * time.Millisecond
	runWatcher(t, h)
	waitFor(t, "subscribe", func() bool { return srv.subs.Load() == 1 })
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !h.Healthy() {
			t.Fatal("went unhealthy while subscribed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	srv.ln.Close()
	close(srv.events) // ends the subscription: EOF
	waitFor(t, "unhealthy after EOF", func() bool { return !h.Healthy() })
}

func TestHerdrPaneSetChangeResubscribes(t *testing.T) {
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "idle"})
	h := fastWatcher(path)
	runWatcher(t, h)
	waitFor(t, "subscribe", func() bool { return srv.subs.Load() == 1 })

	srv.setPanes(herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "idle"}, herdrPane{PaneID: "p2"})
	srv.events <- `{"event":"pane_created","data":{"type":"pane_created","pane":{"pane_id":"p2"}}}`
	waitFor(t, "resubscribe", func() bool { return srv.subs.Load() == 2 })
	subs := srv.lastSub.Load().([]any)
	if len(subs) != len(lifecycleEvents)+2 {
		t.Fatalf("subscriptions after new pane = %v", subs)
	}
}

func TestHerdrPollFallbackWithoutSubscribe(t *testing.T) {
	path := shortSockPath(t)
	srv := startEventHerdr(t, path, herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "working"})
	srv.noEvents = true
	h := fastWatcher(path)
	runWatcher(t, h)
	waitFor(t, "polling", func() bool { return srv.lists.Load() >= 4 })
	if s := srv.subs.Load(); s != 2 {
		t.Fatalf("subscribe attempts = %d, want 2 (full, then lifecycle-only)", s)
	}
	srv.setPanes(herdrPane{PaneID: "p1", Agent: "claude", AgentStatus: "blocked"})
	waitFor(t, "polled status", func() bool { return statusOf(h, "p1") == "blocked" })
}
