package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HerdrWatcher tracks agent panes via herdr's Unix-socket API. It follows
// herdr's event stream and reconciles with pane.list every Reconcile; if
// herdr doesn't support events.subscribe it polls pane.list every Interval.
type HerdrWatcher struct {
	Sock          string
	Match         []string // agent names counted as sessions (substring, case-insensitive)
	WaitingStatus []string // agent_status values counted as waiting
	Interval      time.Duration
	Timeout       time.Duration
	Reconcile     time.Duration    // full pane.list resync while subscribed
	HealthyFor    time.Duration    // Healthy() while the last success is newer than this
	Home          string           // shown as "~"; "" -> os.UserHomeDir
	Now           func() time.Time // injectable for tests; nil -> time.Now

	sessions atomic.Int32
	waiting  atomic.Int32
	lastOK   atomic.Int64 // unix nanos of last successful contact
	seq      atomic.Uint64

	mu      sync.Mutex
	panes   map[string]*paneState // agent panes by pane_id
	sorted  []Agent
	hash    [32]byte
	changed chan struct{}
}

type paneState struct {
	agent, status, path string
	since               int64
}

func NewHerdrWatcher(sock string, match, waiting []string) *HerdrWatcher {
	home, _ := os.UserHomeDir()
	return &HerdrWatcher{
		Sock:          sock,
		Match:         match,
		WaitingStatus: waiting,
		Interval:      5 * time.Second,
		Timeout:       5 * time.Second,
		Reconcile:     30 * time.Second,
		HealthyFor:    15 * time.Second,
		Home:          home,
		panes:         make(map[string]*paneState),
		changed:       make(chan struct{}, 1),
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

func (h *HerdrWatcher) markOK() { h.lastOK.Store(h.now().UnixNano()) }

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

// Agents returns a sorted copy of the agent table, or nil while unhealthy.
func (h *HerdrWatcher) Agents() []Agent {
	if !h.Healthy() {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Agent{}, h.sorted...)
}

func (h *HerdrWatcher) Counts() map[string]int { return countStatuses(h.Agents()) }

// Changed fires (coalesced) whenever the sorted agent list changes, ignoring Since.
func (h *HerdrWatcher) Changed() <-chan struct{} { return h.changed }

type herdrPane struct {
	PaneID        string `json:"pane_id"`
	Agent         string `json:"agent"`
	AgentStatus   string `json:"agent_status"`
	Cwd           string `json:"cwd,omitempty"`
	ForegroundCwd string `json:"foreground_cwd,omitempty"`
	AgentSession  *struct {
		Agent string `json:"agent"`
	} `json:"agent_session,omitempty"`
}

func (p herdrPane) agentName() string {
	if p.Agent == "" && p.AgentSession != nil {
		return p.AgentSession.Agent
	}
	return p.Agent
}

func (p herdrPane) dir() string {
	if p.ForegroundCwd != "" {
		return p.ForegroundCwd
	}
	return p.Cwd
}

type herdrResponse struct {
	ID     string `json:"id"`
	Result *struct {
		Type  string      `json:"type"`
		Panes []herdrPane `json:"panes"`
	} `json:"result"`
	Error json.RawMessage `json:"error"`
}

// herdrAPIError is an error response from herdr itself, as opposed to a
// connection or decode failure.
type herdrAPIError struct {
	method string
	body   json.RawMessage
}

func (e *herdrAPIError) Error() string { return fmt.Sprintf("herdr %s: %s", e.method, e.body) }

func (r *herdrResponse) err(method string) error {
	if len(r.Error) > 0 && string(r.Error) != "null" {
		return &herdrAPIError{method, r.Error}
	}
	if r.Result == nil {
		return fmt.Errorf("herdr %s: missing result", method)
	}
	return nil
}

// poll performs one pane.list round trip and replaces the agent table.
func (h *HerdrWatcher) poll(ctx context.Context) error {
	_, err := h.list(ctx)
	return err
}

// list runs pane.list, applies it, and returns every pane id (agent or not).
func (h *HerdrWatcher) list(ctx context.Context) ([]string, error) {
	d := net.Dialer{Timeout: h.Timeout}
	conn, err := d.DialContext(ctx, "unix", h.Sock)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(h.Timeout))

	req := fmt.Sprintf(`{"id":"o%d","method":"pane.list","params":{}}`+"\n", h.seq.Add(1))
	if _, err := conn.Write([]byte(req)); err != nil {
		return nil, err
	}
	var resp herdrResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, err
	}
	if err := resp.err("pane.list"); err != nil {
		return nil, err
	}
	h.applyList(resp.Result.Panes)
	ids := make([]string, 0, len(resp.Result.Panes))
	for _, p := range resp.Result.Panes {
		ids = append(ids, p.PaneID)
	}
	sort.Strings(ids)
	return ids, nil
}

// applyList replaces the table with the agent panes in panes, keeping Since
// for panes whose status hasn't changed.
func (h *HerdrWatcher) applyList(panes []herdrPane) {
	now := h.now().Unix()
	sessions, waiting := h.count(panes)
	h.sessions.Store(int32(sessions))
	h.waiting.Store(int32(waiting))

	h.mu.Lock()
	next := make(map[string]*paneState, len(panes))
	for _, p := range panes {
		name := p.agentName()
		if name == "" {
			continue
		}
		st := &paneState{agent: name, status: p.AgentStatus, path: p.dir(), since: now}
		if old, ok := h.panes[p.PaneID]; ok && old.status == st.status {
			st.since = old.since
		}
		next[p.PaneID] = st
	}
	h.panes = next
	h.rebuildLocked()
	h.mu.Unlock()
	h.markOK()
}

// applyStatus records a pane.agent_status_changed event.
func (h *HerdrWatcher) applyStatus(id, agent, status string) {
	h.mu.Lock()
	st, ok := h.panes[id]
	switch {
	case !ok && agent == "":
		h.mu.Unlock()
		return
	case !ok:
		st = &paneState{since: h.now().Unix()}
		h.panes[id] = st
	case st.status != status:
		st.since = h.now().Unix()
	}
	if agent != "" {
		st.agent = agent
	}
	st.status = status
	h.recountLocked()
	h.rebuildLocked()
	h.mu.Unlock()
	h.markOK()
}

// recountLocked refreshes the legacy session/waiting counts from the table.
func (h *HerdrWatcher) recountLocked() {
	panes := make([]herdrPane, 0, len(h.panes))
	for _, p := range h.panes {
		panes = append(panes, herdrPane{Agent: p.agent, AgentStatus: p.status})
	}
	s, w := h.count(panes)
	h.sessions.Store(int32(s))
	h.waiting.Store(int32(w))
}

// rebuildLocked recomputes the sorted list and fires Changed if it differs.
func (h *HerdrWatcher) rebuildLocked() {
	as := make([]Agent, 0, len(h.panes))
	for id, p := range h.panes {
		as = append(as, Agent{ID: id, Agent: p.agent, Status: p.status, Since: p.since, path: p.path})
	}
	setDisplayDirs(as, h.Home)
	sortAgents(as)
	h.sorted = as
	if sum := agentsHash(as); sum != h.hash {
		h.hash = sum
		select {
		case h.changed <- struct{}{}:
		default:
		}
	}
}

func (h *HerdrWatcher) count(panes []herdrPane) (sessions, waiting int) {
	for _, p := range panes {
		name := p.agentName()
		if name == "" || !containsFold(h.Match, name) {
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

var (
	errNoSubscribe = errors.New("herdr does not support events.subscribe")
	errResubscribe = errors.New("pane set changed")
)

// lifecycleEvents are subscribed by type. pane.agent_status_changed needs a
// pane_id (herdr 0.9 schema), so it is subscribed per pane and the
// subscription is renewed whenever panes come or go.
var lifecycleEvents = []string{"pane.created", "pane.closed", "pane.exited", "pane.moved", "pane.agent_detected"}

func (h *HerdrWatcher) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := h.stream(ctx)
		switch {
		case errors.Is(err, errNoSubscribe):
			log.Printf("herdr: %v; polling pane.list every %s", err, h.Interval)
			h.runPoll(ctx)
			return
		case errors.Is(err, errResubscribe):
			continue
		}
		if time.Since(start) > h.Reconcile {
			backoff = time.Second // the stream was healthy for a while
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func (h *HerdrWatcher) runPoll(ctx context.Context) {
	for {
		_ = h.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.Interval):
		}
	}
}

// subscribe opens the event connection and waits for the acknowledgement.
func (h *HerdrWatcher) subscribe(ctx context.Context, paneIDs []string) (net.Conn, *bufio.Reader, error) {
	d := net.Dialer{Timeout: h.Timeout}
	conn, err := d.DialContext(ctx, "unix", h.Sock)
	if err != nil {
		return nil, nil, err
	}
	subs := make([]map[string]string, 0, len(lifecycleEvents)+len(paneIDs))
	for _, t := range lifecycleEvents {
		subs = append(subs, map[string]string{"type": t})
	}
	for _, id := range paneIDs {
		subs = append(subs, map[string]string{"type": "pane.agent_status_changed", "pane_id": id})
	}
	req, _ := json.Marshal(map[string]any{
		"id":     fmt.Sprintf("s%d", h.seq.Add(1)),
		"method": "events.subscribe",
		"params": map[string]any{"subscriptions": subs},
	})
	_ = conn.SetDeadline(time.Now().Add(h.Timeout))
	if _, err := conn.Write(append(req, '\n')); err != nil {
		conn.Close()
		return nil, nil, err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	var resp herdrResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err := resp.err("events.subscribe"); err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.Result.Type != "subscription_started" {
		conn.Close()
		return nil, nil, fmt.Errorf("%w (got %q)", errNoSubscribe, resp.Result.Type)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, r, nil
}

type herdrEvent struct {
	Event string `json:"event"`
	Data  struct {
		PaneID      string `json:"pane_id"`
		Agent       string `json:"agent"`
		AgentStatus string `json:"agent_status"`
	} `json:"data"`
}

// stream lists panes, subscribes, and applies events until something fails
// or the pane set changes (errResubscribe).
func (h *HerdrWatcher) stream(ctx context.Context) error {
	ids, err := h.list(ctx)
	if err != nil {
		return err
	}
	conn, r, err := h.subscribe(ctx, ids)
	if err != nil && !errors.Is(err, errNoSubscribe) {
		// A pane may have closed between list and subscribe, which rejects
		// the whole request. Try lifecycle events alone to tell that apart
		// from an herdr that can't subscribe at all.
		c2, _, err2 := h.subscribe(ctx, nil)
		if err2 != nil {
			var apiErr *herdrAPIError
			if errors.Is(err2, errNoSubscribe) || errors.As(err2, &apiErr) {
				return fmt.Errorf("%w: %v", errNoSubscribe, err2)
			}
			return err2
		}
		c2.Close()
		return err
	}
	if err != nil {
		return err
	}
	defer conn.Close()

	lines := make(chan []byte)
	readErr := make(chan error, 1)
	done := make(chan struct{})
	defer close(done) // unblocks a reader waiting to hand over a line
	go func() {
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				readErr <- err
				return
			}
			select {
			case lines <- line:
			case <-done:
				return
			}
		}
	}()

	tick := time.NewTicker(h.Reconcile)
	defer tick.Stop()
	resync := func() error {
		now, err := h.list(ctx)
		if err != nil {
			return err
		}
		if !equalStrings(now, ids) {
			return errResubscribe
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			_ = h.poll(ctx) // resync right away; Run resubscribes with backoff
			return fmt.Errorf("herdr events: %w", err)
		case <-tick.C:
			if err := resync(); err != nil {
				return err
			}
		case line := <-lines:
			var ev herdrEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				_ = h.poll(ctx)
				return fmt.Errorf("herdr events: %w", err)
			}
			h.markOK()
			// Lifecycle events arrive as pane_created, subscribed ones as
			// pane.agent_status_changed; normalize both.
			switch strings.ReplaceAll(ev.Event, ".", "_") {
			case "pane_agent_status_changed":
				h.applyStatus(ev.Data.PaneID, ev.Data.Agent, ev.Data.AgentStatus)
			case "events_lost":
				_ = h.poll(ctx)
				return errors.New("herdr events lost")
			default:
				if err := resync(); err != nil {
					return err
				}
			}
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
