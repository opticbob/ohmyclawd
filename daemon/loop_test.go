package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fetchResult struct {
	u   *Usage
	err error
}

// fakeSource replays results in order, cycling when it runs out.
type fakeSource struct {
	name     string
	interval time.Duration

	mu      sync.Mutex
	calls   int
	results []fetchResult
}

func (f *fakeSource) Name() string            { return f.name }
func (f *fakeSource) Interval() time.Duration { return f.interval }

func (f *fakeSource) Fetch(ctx context.Context) (*Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.results[f.calls%len(f.results)]
	f.calls++
	if r.u != nil {
		cp := *r.u
		return &cp, r.err
	}
	return nil, r.err
}

func (f *fakeSource) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// runLoopFor runs RunLoop, recording sleeps, and cancels after n sleeps.
func runLoopFor(t *testing.T, c *Chain, n int, cfg LoopConfig) (*State, *Metrics, []time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sleeps []time.Duration
	cfg.Sleep = func(d time.Duration) {
		sleeps = append(sleeps, d)
		if len(sleeps) >= n {
			cancel()
		}
	}
	st, m := NewState(), NewMetrics()
	RunLoop(ctx, c, st, m, cfg)
	return st, m, sleeps
}

func TestLoopBackoffSchedule(t *testing.T) {
	fs := &fakeSource{name: "probe", interval: 7 * time.Millisecond, results: []fetchResult{
		{nil, errors.New("transient")},
		{nil, errors.New("transient")},
		{&Usage{S: 1, OK: true}, nil},
	}}
	cfg := LoopConfig{
		RateLimited: 5 * time.Millisecond,
		Backoff:     []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond},
	}
	st, m, sleeps := runLoopFor(t, NewChain(fs), 3, cfg)

	want := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 7 * time.Millisecond}
	if fmt.Sprint(sleeps) != fmt.Sprint(want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
	if u, _ := st.Load(); u.S != 1 || !u.OK || u.Src != "probe" {
		t.Fatalf("state not updated on success: %+v", u)
	}
	if m.probe.Load() != 3 || m.fail.Load() != 2 {
		t.Fatalf("metrics not updated: probe=%d fail=%d", m.probe.Load(), m.fail.Load())
	}
}

func TestLoopBackoffCapsAtLast(t *testing.T) {
	fs := &fakeSource{name: "probe", results: []fetchResult{{nil, errors.New("down")}}}
	cfg := LoopConfig{Backoff: []time.Duration{1, 2}}
	_, _, sleeps := runLoopFor(t, NewChain(fs), 4, cfg)
	if fmt.Sprint(sleeps) != fmt.Sprint([]time.Duration{1, 2, 2, 2}) {
		t.Fatalf("sleeps = %v", sleeps)
	}
}

func TestLoopFailureKeepsLastGood(t *testing.T) {
	fs := &fakeSource{name: "probe", interval: 1, results: []fetchResult{
		{&Usage{S: 42, W: 9, OK: true, Ts: 100}, nil},
		{nil, errors.New("down")},
	}}
	now := time.Unix(5000, 0)
	cfg := LoopConfig{Backoff: []time.Duration{3}, Now: func() time.Time { return now }}
	st, _, _ := runLoopFor(t, NewChain(fs), 2, cfg)
	u, _ := st.Load()
	if u.S != 42 || u.W != 9 || u.OK || u.Ts != 5000 || u.OKTs != 5000 {
		t.Fatalf("state = %+v", u)
	}
}

func TestLoopRateLimitedSleepsLongAndStoresPartial(t *testing.T) {
	fs := &fakeSource{name: "probe", results: []fetchResult{
		{&Usage{S: 100, ST: "limited"}, fmt.Errorf("status 429: %w", errRateLimited)},
	}}
	cfg := LoopConfig{RateLimited: 9 * time.Millisecond, Backoff: []time.Duration{1}}
	st, m, sleeps := runLoopFor(t, NewChain(fs), 1, cfg)
	if sleeps[0] != 9*time.Millisecond {
		t.Fatalf("sleep = %v, want RateLimited", sleeps[0])
	}
	if u, _ := st.Load(); u.S != 100 || u.ST != "limited" || u.Src != "probe" {
		t.Fatalf("partial not stored: %+v", u)
	}
	if m.fail.Load() != 1 {
		t.Fatalf("fail = %d", m.fail.Load())
	}
}

func TestLoopSleepIsContextAware(t *testing.T) {
	fs := &fakeSource{name: "probe", interval: time.Hour, results: []fetchResult{{&Usage{OK: true}, nil}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunLoop(ctx, NewChain(fs), NewState(), NewMetrics(), LoopConfig{Backoff: []time.Duration{time.Hour}})
		close(done)
	}()
	for fs.Calls() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not return promptly after cancel")
	}
}

func TestProbeSourceAuthRetryReloadsCreds(t *testing.T) {
	var calls atomic.Int32
	srv := newAuthFlipServer(t, &calls)
	defer srv.Close()

	reloaded := 0
	ps := &ProbeSource{
		P: &Prober{URL: srv.URL, Token: "test-token", HTTP: srv.Client()},
		ReloadCreds: func() (*Creds, error) {
			reloaded++
			return &Creds{AccessToken: "refreshed-token"}, nil
		},
	}
	u, err := ps.Fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if reloaded != 1 || ps.P.Token != "refreshed-token" || calls.Load() != 2 {
		t.Fatalf("reloaded=%d token=%q calls=%d", reloaded, ps.P.Token, calls.Load())
	}
	if u.S != 45 || !u.OK {
		t.Fatalf("usage = %+v", u)
	}
}

func TestProbeSourceAuthRetryFailsWhenReloadFails(t *testing.T) {
	var calls atomic.Int32
	srv := newAuthFlipServer(t, &calls)
	defer srv.Close()
	ps := &ProbeSource{
		P:           &Prober{URL: srv.URL, Token: "test-token", HTTP: srv.Client()},
		ReloadCreds: func() (*Creds, error) { return nil, errors.New("gone") },
	}
	if _, err := ps.Fetch(context.Background()); !IsAuthError(err) {
		t.Fatalf("err = %v, want auth error", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want no retry", calls.Load())
	}
}
