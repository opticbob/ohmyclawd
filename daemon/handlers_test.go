package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestHandler(t *testing.T) (*Handler, *State, *Metrics) {
	t.Helper()
	s := NewState()
	m := NewMetrics()
	return NewHandler(s, m, nil, func() time.Time { return time.Unix(1747353660, 0) }, ""), s, m
}

func TestUsageHandler200(t *testing.T) {
	h, s, _ := newTestHandler(t)
	s.Store(&Usage{S: 50, W: 25, ST: "allowed", OK: true, Ts: 100})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/usage", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"s":50`) {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

func TestHealthzHandler(t *testing.T) {
	h, _, _ := newTestHandler(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != 200 || rr.Body.String() != "ok" {
		t.Fatalf("body=%q code=%d", rr.Body.String(), rr.Code)
	}
}

func TestUsageHandler_BlockedWithoutToken(t *testing.T) {
	s := NewState()
	m := NewMetrics()
	h := NewHandler(s, m, nil, time.Now, "mysecret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/usage", nil))
	if rr.Code != 401 {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHealthz_PassesThroughWithToken(t *testing.T) {
	s := NewState()
	m := NewMetrics()
	h := NewHandler(s, m, nil, time.Now, "mysecret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestMetricsHandler(t *testing.T) {
	h, s, m := newTestHandler(t)
	s.Store(&Usage{Ts: 1747353600}) // 60 s ago given fixed Now
	m.IncProbe()
	m.IncFail()

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != 200 {
		t.Fatalf("code = %d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"ohmyclawd_probe_total 1",
		"ohmyclawd_probe_fail_total 1",
		"ohmyclawd_probe_last_age_seconds 60",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

func TestUsageHandlerObservabilityFields(t *testing.T) {
	h, s, _ := newTestHandler(t) // now = 1747353660
	h.sw = &staticWatcher{kind: "herdr", healthy: true, sessions: 3, wait: 1}
	s.Store(&Usage{
		S: 14, SR: 200, W: 8, WR: 7540, ST: "allowed", OK: false, Ts: 1747353650,
		Src: "cli", Err: "cli: claude timed out after 45s", OKTs: 1747353600,
		Lim: []Limit{{Label: "Fable", Pct: 12, ResetMin: 4870}},
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/usage", nil))

	const want = `{"s":14,"sr":200,"w":8,"wr":7540,"st":"allowed","ok":false,"ts":1747353650,"cs":3,"cw":1,` +
		`"src":"cli","err":"cli: claude timed out after 45s","lim":[{"l":"Fable","p":12,"r":4870}],"age":60,"sw":"herdr"}`
	if got := rr.Body.String(); got != want {
		t.Fatalf("body:\n%s\nwant:\n%s", got, want)
	}
}

func TestUsageHandlerAge(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	cases := []struct {
		name string
		okts int64
		want int64
	}{
		{"never succeeded", 0, -1},
		{"fresh", 1_000_000, 0},
		{"90s old", 1_000_000 - 90, 90},
		{"clock skew clamps to 0", 1_000_005, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewState()
			s.Store(&Usage{OKTs: tc.okts})
			h := NewHandler(s, NewMetrics(), nil, func() time.Time { return now }, "")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/usage", nil))
			var got struct {
				Age int64  `json:"age"`
				SW  string `json:"sw"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.Age != tc.want || got.SW != "none" {
				t.Fatalf("age=%d sw=%q, want %d/none", got.Age, got.SW, tc.want)
			}
		})
	}
}

// TestUsageOldClientCompat decodes the payload the way the firmware does:
// only the original nine keys, ignoring anything new.
func TestUsageOldClientCompat(t *testing.T) {
	h, s, _ := newTestHandler(t)
	h.sw = &staticWatcher{kind: "tmux", healthy: true, sessions: 2, wait: 1}
	s.Store(&Usage{S: 45, SR: 120, W: 28, WR: 7200, ST: "allowed", OK: true, Ts: 1747353600,
		Src: "cli", OKTs: 1747353600, Lim: []Limit{{"Sonnet only", 3, 10}}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/usage", nil))

	type oldUsage struct {
		S  int    `json:"s"`
		SR int    `json:"sr"`
		W  int    `json:"w"`
		WR int    `json:"wr"`
		ST string `json:"st"`
		OK bool   `json:"ok"`
		Ts int64  `json:"ts"`
		CS int    `json:"cs"`
		CW int    `json:"cw"`
	}
	var got oldUsage
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := oldUsage{S: 45, SR: 120, W: 28, WR: 7200, ST: "allowed", OK: true, Ts: 1747353600, CS: 2, CW: 1}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	// And the original keys keep their JSON types.
	var raw map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &raw)
	for k, typ := range map[string]string{"s": "float64", "sr": "float64", "w": "float64", "wr": "float64",
		"st": "string", "ok": "bool", "ts": "float64", "cs": "float64", "cw": "float64"} {
		if got := fmt.Sprintf("%T", raw[k]); got != typ {
			t.Errorf("%s: type %s, want %s", k, got, typ)
		}
	}
}

func TestMetricsHandlerNewSeries(t *testing.T) {
	h, s, m := newTestHandler(t) // now = 1747353660
	h.sw = &staticWatcher{kind: "herdr", healthy: true, sessions: 4, wait: 2}
	s.Store(&Usage{Ts: 1747353650, OKTs: 1747353630})
	m.IncProbe()
	m.SourceFetch("cli", errors.New("boom"))
	m.SourceFetch("probe", nil)
	m.SourceFetch("cli", nil)
	m.SetActive("cli")
	m.idle.Store(true)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		"ohmyclawd_probe_total 1\n",
		"ohmyclawd_probe_fail_total 0\n",
		"ohmyclawd_probe_last_age_seconds 10\n",
		`ohmyclawd_source_fetch_total{src="cli"} 2` + "\n",
		`ohmyclawd_source_fetch_total{src="probe"} 1` + "\n",
		`ohmyclawd_source_fail_total{src="cli"} 1` + "\n",
		`ohmyclawd_source_fail_total{src="probe"} 0` + "\n",
		`ohmyclawd_source_active{src="cli"} 1` + "\n",
		`ohmyclawd_source_active{src="probe"} 0` + "\n",
		"ohmyclawd_data_age_seconds 30\n",
		"ohmyclawd_idle 1\n",
		"ohmyclawd_sessions 4\n",
		"ohmyclawd_sessions_waiting 2\n",
		`ohmyclawd_session_watcher{kind="herdr"} 1` + "\n",
		`ohmyclawd_session_watcher{kind="tmux"} 0` + "\n",
		`ohmyclawd_session_watcher{kind="none"} 0` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	if t.Failed() {
		t.Log(body)
	}
}

func TestLoopSetsAndClearsErr(t *testing.T) {
	fs := &fakeSource{name: "cli", interval: 1, results: []fetchResult{
		{&Usage{S: 1, OK: true}, nil},
		{nil, errors.New("line one\nline two")},
	}}
	st, _, _ := runLoopFor(t, NewChain(fs), 2, LoopConfig{Backoff: []time.Duration{1}})
	if u, _ := st.Load(); u.Err != "cli: line one; line two" || u.S != 1 {
		t.Fatalf("after failure: %+v", u)
	}
	fs2 := &fakeSource{name: "cli", interval: 1, results: []fetchResult{
		{nil, errors.New("x")},
		{&Usage{S: 2, OK: true}, nil},
	}}
	st2, _, _ := runLoopFor(t, NewChain(fs2), 2, LoopConfig{Backoff: []time.Duration{1}})
	if u, _ := st2.Load(); u.Err != "" || u.S != 2 {
		t.Fatalf("after recovery: %+v", u)
	}
}

func TestChainRecordsSourceMetrics(t *testing.T) {
	a := &fakeSource{name: "cli", results: []fetchResult{{nil, errors.New("x")}}}
	b := &fakeSource{name: "probe", results: []fetchResult{{&Usage{OK: true}, nil}}}
	c := NewChain(a, b)
	c.Metrics = NewMetrics()
	_, _, _ = c.Fetch(context.Background())
	_, _, _ = c.Fetch(context.Background())
	m := c.Metrics
	if m.sources["cli"].fetch != 2 || m.sources["cli"].fail != 2 || m.sources["probe"].fetch != 2 ||
		m.sources["probe"].fail != 0 || m.active != "probe" {
		t.Fatalf("cli=%+v probe=%+v active=%q", *m.sources["cli"], *m.sources["probe"], m.active)
	}
}
