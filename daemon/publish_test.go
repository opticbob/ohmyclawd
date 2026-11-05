package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden files")

// golden compares got against testdata/golden/<name>, rewriting it with -update.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := "testdata/golden/" + name
	if *updateGolden {
		if err := os.MkdirAll("testdata/golden", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s mismatch\n got: %s\nwant: %s", name, got, want)
	}
}

type capturePub struct {
	mu   sync.Mutex
	msgs []MQTTMessage
}

func (c *capturePub) Publish(m MQTTMessage) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

func (c *capturePub) count(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if m.Topic == topic {
			n++
		}
	}
	return n
}

func (c *capturePub) last(topic string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.msgs) - 1; i >= 0; i-- {
		if c.msgs[i].Topic == topic {
			return string(c.msgs[i].Payload)
		}
	}
	return ""
}

func testPublisher(now *time.Time) (*Publisher, *HerdrWatcher) {
	h := newTestWatcher(now)
	h.applyList([]herdrPane{
		{PaneID: "w1:p2", Agent: "claude", AgentStatus: "blocked", Cwd: "/h/x/vets-api", Tokens: map[string]string{"ctx": "62"}},
		{PaneID: "w2:p1", Agent: "claude", AgentStatus: "done", Cwd: "/h/dewey"},
		{PaneID: "w3:p1", Agent: "kiro", AgentStatus: "working", Cwd: "/h/homelab"},
	})
	s := NewState()
	s.Store(&Usage{S: 58, SR: 130, W: 31, WR: 4000, ST: "allowed", OK: true, Ts: now.Unix(), OKTs: now.Unix() - 5, Src: "cli"})
	return &Publisher{
		Client: &capturePub{}, Prefix: "ohmyclawd", Node: "joshpc", Discovery: "homeassistant",
		SW: h, State: s, MaxAgents: 12, NotifyAfter: 30 * time.Second,
		Now: func() time.Time { return *now },
	}, h
}

func TestPublishPayloadsGolden(t *testing.T) {
	now := time.Unix(1793700000, 0)
	p, _ := testPublisher(&now)

	am, _ := p.agentsMessage(now)
	um := p.usageMessage(now)
	if am.Topic != "ohmyclawd/joshpc/agents" || !am.Retain || um.Topic != "ohmyclawd/joshpc/usage" || !um.Retain {
		t.Fatalf("topics: %+v %+v", am, um)
	}
	golden(t, "mqtt_agents.json", am.Payload)
	golden(t, "mqtt_usage.json", um.Payload)

	now = now.Add(31 * time.Second)
	agents, ok := p.allAgents()
	ev := p.dueEvents(agents, ok, now)
	if len(ev) != 2 || ev[0].Retain || ev[0].Topic != "ohmyclawd/joshpc/event" {
		t.Fatalf("events = %+v", ev)
	}
	golden(t, "mqtt_event.json", append(append(ev[0].Payload, '\n'), ev[1].Payload...))

	var buf bytes.Buffer
	for _, m := range p.discoveryMessages() {
		if !m.Retain {
			t.Errorf("%s not retained", m.Topic)
		}
		var v any
		if err := json.Unmarshal(m.Payload, &v); err != nil {
			t.Fatalf("%s: %v", m.Topic, err)
		}
		pretty, _ := json.MarshalIndent(v, "", "  ")
		buf.WriteString(m.Topic + "\n" + string(pretty) + "\n\n")
	}
	golden(t, "mqtt_discovery.txt", buf.Bytes())

	// An old /usage client still finds every original key.
	var old struct {
		S, SR, W, WR int
		ST           string
		OK           bool
		TS           int64
		CS, CW       int
	}
	if err := json.Unmarshal(um.Payload, &old); err != nil || old.S != 58 || old.CS != 2 || old.CW != 1 {
		t.Fatalf("usage payload not /usage-compatible: %+v %v", old, err)
	}
}

func TestDiscoveryDisabled(t *testing.T) {
	now := time.Unix(1793700000, 0)
	p, _ := testPublisher(&now)
	p.Discovery = ""
	if m := p.discoveryMessages(); m != nil {
		t.Fatalf("got %d configs with discovery off", len(m))
	}
}

func TestAgentsPayloadSize(t *testing.T) {
	now := time.Unix(1793700000, 0)
	h := newTestWatcher(&now)
	var panes []herdrPane
	for i := 0; i < 20; i++ {
		panes = append(panes, herdrPane{
			PaneID: "w10:p" + strings.Repeat("9", 2), Agent: "claude", AgentStatus: "blocked",
			Cwd: "/h/projects/a-rather-long-repository-name-" + string(rune('a'+i)),
		})
		panes[i].PaneID += string(rune('a' + i))
	}
	h.applyList(panes)
	b, _ := json.Marshal(buildAgentsPayload(h, 12, now))
	if len(b) > 1536 {
		t.Fatalf("12-agent payload is %d bytes, over the 1.5 KB budget", len(b))
	}
}

// dueOf runs dueEvents against h and returns "id:type" for each event.
func dueOf(p *Publisher, h *HerdrWatcher, ok bool, now time.Time) []string {
	var out []string
	for _, m := range p.dueEvents(h.Agents(), ok, now) {
		var e agentEvent
		_ = json.Unmarshal(m.Payload, &e)
		out = append(out, e.ID+":"+e.Type)
	}
	return out
}

func TestNotifyAfter(t *testing.T) {
	now := time.Unix(1000, 0)
	p, h := testPublisher(&now)
	due := func() []string { return dueOf(p, h, true, now) }

	now = now.Add(29 * time.Second)
	if got := due(); len(got) != 0 {
		t.Fatalf("short block fired: %v", got)
	}
	h.applyStatus("w1:p2", "", "working") // answered within NOTIFY_AFTER
	now = now.Add(2 * time.Second)
	if got := due(); len(got) != 1 || got[0] != "w2:p1:done" {
		t.Fatalf("after 31s: %v, want only the done pane", got)
	}
	now = now.Add(time.Minute)
	if got := due(); len(got) != 0 {
		t.Fatalf("long block fired twice: %v", got)
	}

	h.applyStatus("w1:p2", "", "blocked") // re-entered
	now = now.Add(30 * time.Second)
	if got := due(); len(got) != 1 || got[0] != "w1:p2:blocked" {
		t.Fatalf("re-entered status: %v", got)
	}
	h.applyStatus("w2:p1", "", "idle")
	h.applyStatus("w2:p1", "", "done")
	if got := due(); len(got) != 1 || got[0] != "w2:p1:cleared" {
		t.Fatalf("done re-entered, first tick: %v, want the old done cleared", got)
	}
	now = now.Add(30 * time.Second)
	if got := due(); len(got) != 1 || got[0] != "w2:p1:done" {
		t.Fatalf("done re-entered: %v", got)
	}
}

func TestEventCleared(t *testing.T) {
	now := time.Unix(1000, 0)
	p, h := testPublisher(&now)
	now = now.Add(31 * time.Second)
	dueOf(p, h, true, now) // announces w1:p2 blocked and w2:p1 done

	now = now.Add(5 * time.Second)
	h.applyStatus("w1:p2", "", "working")
	ev := p.dueEvents(h.Agents(), true, now)
	if len(ev) != 1 {
		t.Fatalf("got %d events, want 1 cleared", len(ev))
	}
	var e agentEvent
	_ = json.Unmarshal(ev[0].Payload, &e)
	if e.Type != "cleared" || e.ID != "w1:p2" || e.Since != 1000 || e.For != 36 || e.Dir != "vets-api" {
		t.Fatalf("cleared = %+v", e)
	}
	if got := dueOf(p, h, true, now); len(got) != 0 {
		t.Fatalf("cleared twice: %v", got)
	}

	// A closed pane clears too; a pane never announced does not.
	h.applyList([]herdrPane{{PaneID: "w3:p1", Agent: "kiro", AgentStatus: "idle", Cwd: "/h/homelab"}})
	if got := dueOf(p, h, true, now); len(got) != 1 || got[0] != "w2:p1:cleared" {
		t.Fatalf("closed pane: %v", got)
	}
}

func TestEventsHoldDuringHerdrGap(t *testing.T) {
	now := time.Unix(1000, 0)
	p, h := testPublisher(&now)
	now = now.Add(31 * time.Second)
	if got := dueOf(p, h, true, now); len(got) != 2 {
		t.Fatalf("initial: %v", got)
	}
	// herdr drops out: nothing fires, nothing clears, nothing is forgotten.
	now = now.Add(time.Minute)
	if got := dueOf(p, h, false, now); len(got) != 0 {
		t.Fatalf("during gap: %v", got)
	}
	// Back with the same panes still waiting: no repeat alerts.
	if got := dueOf(p, h, true, now); len(got) != 0 {
		t.Fatalf("after gap: %v, want no repeats", got)
	}
}

func TestPublisherRun(t *testing.T) {
	now := time.Unix(1793700000, 0)
	var mu sync.Mutex
	p, h := testPublisher(&now)
	p.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	h.Now = p.Now
	cp := p.Client.(*capturePub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	waitFor(t, "initial", func() bool {
		return cp.count("ohmyclawd/joshpc/agents") >= 1 && cp.count("ohmyclawd/joshpc/usage") >= 1 &&
			cp.count("homeassistant/sensor/ohmyclawd_joshpc/waiting/config") == 1
	})

	p.State.Store(&Usage{S: 60, Ts: now.Unix(), OK: true})
	waitFor(t, "usage on Store", func() bool { return strings.Contains(cp.last("ohmyclawd/joshpc/usage"), `"s":60`) })

	h.applyStatus("w1:p2", "", "working")
	waitFor(t, "agents on change", func() bool {
		return strings.Contains(cp.last("ohmyclawd/joshpc/agents"), `"blocked":0`)
	})

	mu.Lock()
	now = now.Add(45 * time.Second)
	mu.Unlock()
	waitFor(t, "done event", func() bool { return cp.count("ohmyclawd/joshpc/event") == 1 })
	if !strings.Contains(cp.last("ohmyclawd/joshpc/event"), `"event_type":"done"`) {
		t.Fatalf("event = %s", cp.last("ohmyclawd/joshpc/event"))
	}
}

func TestSanitizeNode(t *testing.T) {
	for in, want := range map[string]string{
		"josh-fike-windows-desktop": "josh_fike_windows_desktop",
		"JoshPC.local":              "joshpc_local",
		"--":                        "host",
		"ok_1":                      "ok_1",
	} {
		if got := sanitizeNode(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestMQTTPasswordCmd(t *testing.T) {
	path := t.TempDir() + "/sub/mqtt_password"
	var out bytes.Buffer
	if err := runMQTTPasswordCmd(path, strings.NewReader("  hunter2  \nignored\n"), &out); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %o", fi.Mode().Perm())
	}
	if got, _ := readSecretFile(path, "x", t.Logf); got != "hunter2" {
		t.Errorf("stored %q", got)
	}
	if err := runMQTTPasswordCmd(path, strings.NewReader("\n"), &out); err == nil {
		t.Error("empty password accepted")
	}
}
