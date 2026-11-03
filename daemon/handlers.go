package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	probe atomic.Uint64
	fail  atomic.Uint64
	idle  atomic.Bool

	mqttEnabled    atomic.Bool
	mqttConnected  atomic.Bool
	mqttReconnects atomic.Uint64

	mu      sync.Mutex
	sources map[string]*sourceStats
	active  string
	mqttPub map[string]uint64 // by topic label
}

type sourceStats struct{ fetch, fail uint64 }

func NewMetrics() *Metrics {
	return &Metrics{sources: make(map[string]*sourceStats), mqttPub: make(map[string]uint64)}
}
func (m *Metrics) IncProbe() { m.probe.Add(1) }
func (m *Metrics) IncFail()  { m.fail.Add(1) }

// SourceFetch counts one fetch attempt by the named source, and a failure if err != nil.
func (m *Metrics) SourceFetch(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.sources[name]
	if st == nil {
		st = &sourceStats{}
		m.sources[name] = st
	}
	st.fetch++
	if err != nil {
		st.fail++
	}
}

// MQTTPublished counts one PUBLISH by topic label (status, agents, usage, event, config).
func (m *Metrics) MQTTPublished(label string) {
	m.mu.Lock()
	m.mqttPub[label]++
	m.mu.Unlock()
}

func (m *Metrics) SetActive(name string) {
	m.mu.Lock()
	m.active = name
	m.mu.Unlock()
}

// usageResponse adds per-request fields to the stored Usage. The embedded
// pointer's fields are flattened, so the original keys are unchanged.
type usageResponse struct {
	*Usage
	Age int64  `json:"age"` // seconds since the last successful fetch; -1 if none yet
	SW  string `json:"sw"`  // session watcher answering: herdr | tmux | none
}

type Handler struct {
	state   *State
	metrics *Metrics
	sw      SessionWatcher
	now     func() time.Time
	mux     *http.ServeMux

	// Activity, when set, is touched on every authorized /usage request.
	// /metrics deliberately doesn't count, so scrapers don't keep the loop awake.
	Activity *Activity

	// MaxAgents caps the /agents list (0 = no cap), matching the MQTT payload.
	MaxAgents int
}

func NewHandler(s *State, m *Metrics, sw SessionWatcher, now func() time.Time, token string) *Handler {
	if now == nil {
		now = time.Now
	}
	h := &Handler{state: s, metrics: m, sw: sw, now: now, mux: http.NewServeMux()}

	protected := http.NewServeMux()
	protected.HandleFunc("/usage", h.usage)
	protected.HandleFunc("/metrics", h.prom)
	protected.HandleFunc("/agents", h.agents)

	h.mux.Handle("/usage", AuthMiddleware(token, protected))
	h.mux.Handle("/metrics", AuthMiddleware(token, protected))
	h.mux.Handle("/agents", AuthMiddleware(token, protected))
	h.mux.HandleFunc("/healthz", h.healthz)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) usage(w http.ResponseWriter, r *http.Request) {
	if h.Activity != nil {
		h.Activity.Touch(h.now())
	}
	u, _ := h.state.Load()
	body, _ := json.Marshal(buildUsageResponse(u, h.sw, h.now()))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

// agents serves the herdr agent table, the same JSON as the MQTT agents topic.
// It doesn't touch Activity: it carries no usage data.
func (h *Handler) agents(w http.ResponseWriter, r *http.Request) {
	body, _ := json.Marshal(buildAgentsPayload(h.sw, h.MaxAgents, h.now()))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

// buildUsageResponse fills the per-request fields of /usage (also the MQTT
// usage topic). u is a copy and gets the live session counts.
func buildUsageResponse(u *Usage, sw SessionWatcher, now time.Time) usageResponse {
	resp := usageResponse{Usage: u, Age: dataAge(u, now), SW: "none"}
	if sw != nil {
		u.CS = sw.Sessions()
		u.CW = sw.Waiting()
		resp.SW = sw.Kind()
	}
	return resp
}

// dataAge is seconds since the last successful fetch, or -1 if there was none.
func dataAge(u *Usage, now time.Time) int64 {
	if u == nil || u.OKTs == 0 {
		return -1
	}
	if age := now.Unix() - u.OKTs; age > 0 {
		return age
	}
	return 0
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) prom(w http.ResponseWriter, r *http.Request) {
	u, _ := h.state.Load()
	age := 0
	if u != nil && u.Ts > 0 {
		age = int(h.now().Unix() - u.Ts)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP ohmyclawd_probe_total Total probe attempts since start\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_probe_total counter\n")
	fmt.Fprintf(w, "ohmyclawd_probe_total %d\n", h.metrics.probe.Load())
	fmt.Fprintf(w, "# HELP ohmyclawd_probe_fail_total Probe failures since start\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_probe_fail_total counter\n")
	fmt.Fprintf(w, "ohmyclawd_probe_fail_total %d\n", h.metrics.fail.Load())
	fmt.Fprintf(w, "# HELP ohmyclawd_probe_last_age_seconds Seconds since last probe attempt\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_probe_last_age_seconds gauge\n")
	fmt.Fprintf(w, "ohmyclawd_probe_last_age_seconds %d\n", age)

	m := h.metrics
	m.mu.Lock()
	names := make([]string, 0, len(m.sources))
	for n := range m.sources {
		names = append(names, n)
	}
	sort.Strings(names)
	stats := make([]sourceStats, len(names))
	for i, n := range names {
		stats[i] = *m.sources[n]
	}
	active := m.active
	pubLabels := make([]string, 0, len(m.mqttPub))
	for l := range m.mqttPub {
		pubLabels = append(pubLabels, l)
	}
	sort.Strings(pubLabels)
	pubCounts := make([]uint64, len(pubLabels))
	for i, l := range pubLabels {
		pubCounts[i] = m.mqttPub[l]
	}
	m.mu.Unlock()

	fmt.Fprintf(w, "# HELP ohmyclawd_source_fetch_total Fetch attempts per usage source\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_source_fetch_total counter\n")
	for i, n := range names {
		fmt.Fprintf(w, "ohmyclawd_source_fetch_total{src=%q} %d\n", n, stats[i].fetch)
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_source_fail_total Failed fetches per usage source\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_source_fail_total counter\n")
	for i, n := range names {
		fmt.Fprintf(w, "ohmyclawd_source_fail_total{src=%q} %d\n", n, stats[i].fail)
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_source_active 1 for the source that produced the current data\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_source_active gauge\n")
	for _, n := range names {
		fmt.Fprintf(w, "ohmyclawd_source_active{src=%q} %d\n", n, b2i(n == active))
	}
	if dataAge := dataAge(u, h.now()); dataAge >= 0 {
		fmt.Fprintf(w, "# HELP ohmyclawd_data_age_seconds Seconds since the last successful fetch\n")
		fmt.Fprintf(w, "# TYPE ohmyclawd_data_age_seconds gauge\n")
		fmt.Fprintf(w, "ohmyclawd_data_age_seconds %d\n", dataAge)
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_idle 1 while fetching is paused for lack of /usage requests\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_idle gauge\n")
	fmt.Fprintf(w, "ohmyclawd_idle %d\n", b2i(m.idle.Load()))

	sessions, waiting, kind := 0, 0, "none"
	if h.sw != nil {
		sessions, waiting, kind = h.sw.Sessions(), h.sw.Waiting(), h.sw.Kind()
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_sessions Claude sessions seen by the session watcher\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_sessions gauge\n")
	fmt.Fprintf(w, "ohmyclawd_sessions %d\n", sessions)
	fmt.Fprintf(w, "# HELP ohmyclawd_sessions_waiting Claude sessions waiting on the user\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_sessions_waiting gauge\n")
	fmt.Fprintf(w, "ohmyclawd_sessions_waiting %d\n", waiting)
	fmt.Fprintf(w, "# HELP ohmyclawd_session_watcher 1 for the session watcher currently answering\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_session_watcher gauge\n")
	for _, k := range []string{"herdr", "tmux", "none"} {
		fmt.Fprintf(w, "ohmyclawd_session_watcher{kind=%q} %d\n", k, b2i(k == kind))
	}

	counts := buildAgentsPayload(h.sw, 0, h.now()).N
	fmt.Fprintf(w, "# HELP ohmyclawd_agents herdr agents by status\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_agents gauge\n")
	for _, st := range agentStatuses {
		fmt.Fprintf(w, "ohmyclawd_agents{status=%q} %d\n", st, counts[st])
	}

	if !m.mqttEnabled.Load() {
		return
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_mqtt_connected 1 while connected to the MQTT broker\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_mqtt_connected gauge\n")
	fmt.Fprintf(w, "ohmyclawd_mqtt_connected %d\n", b2i(m.mqttConnected.Load()))
	fmt.Fprintf(w, "# HELP ohmyclawd_mqtt_publish_total MQTT messages published, by topic\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_mqtt_publish_total counter\n")
	for i, l := range pubLabels {
		fmt.Fprintf(w, "ohmyclawd_mqtt_publish_total{topic=%q} %d\n", l, pubCounts[i])
	}
	fmt.Fprintf(w, "# HELP ohmyclawd_mqtt_reconnect_total MQTT connection attempts after the first\n")
	fmt.Fprintf(w, "# TYPE ohmyclawd_mqtt_reconnect_total counter\n")
	fmt.Fprintf(w, "ohmyclawd_mqtt_reconnect_total %d\n", m.mqttReconnects.Load())
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
