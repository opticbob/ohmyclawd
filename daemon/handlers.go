package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct {
	probe atomic.Uint64
	fail  atomic.Uint64
	idle  atomic.Bool
}

func NewMetrics() *Metrics   { return &Metrics{} }
func (m *Metrics) IncProbe() { m.probe.Add(1) }
func (m *Metrics) IncFail()  { m.fail.Add(1) }

type Handler struct {
	state   *State
	metrics *Metrics
	sw      SessionWatcher
	now     func() time.Time
	mux     *http.ServeMux

	// Activity, when set, is touched on every authorized /usage request.
	// /metrics deliberately doesn't count, so scrapers don't keep the loop awake.
	Activity *Activity
}

func NewHandler(s *State, m *Metrics, sw SessionWatcher, now func() time.Time, token string) *Handler {
	if now == nil {
		now = time.Now
	}
	h := &Handler{state: s, metrics: m, sw: sw, now: now, mux: http.NewServeMux()}

	protected := http.NewServeMux()
	protected.HandleFunc("/usage", h.usage)
	protected.HandleFunc("/metrics", h.prom)

	h.mux.Handle("/usage", AuthMiddleware(token, protected))
	h.mux.Handle("/metrics", AuthMiddleware(token, protected))
	h.mux.HandleFunc("/healthz", h.healthz)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) usage(w http.ResponseWriter, r *http.Request) {
	if h.Activity != nil {
		h.Activity.Touch(h.now())
	}
	u, _ := h.state.Load()
	if h.sw != nil {
		u.CS = h.sw.Sessions()
		u.CW = h.sw.Waiting()
	}
	body, _ := json.Marshal(u)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
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
}
