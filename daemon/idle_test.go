package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is safe to read from the loop goroutine while the test advances it.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t.UnixNano())
	return c
}
func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// clockSource records the fake time of each fetch.
type clockSource struct {
	clock *fakeClock
	mu    sync.Mutex
	at    []time.Time
}

func (s *clockSource) Name() string            { return "cli" }
func (s *clockSource) Interval() time.Duration { return time.Minute }
func (s *clockSource) Fetch(ctx context.Context) (*Usage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at = append(s.at, s.clock.Now())
	return &Usage{OK: true}, nil
}
func (s *clockSource) Fetches() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.at...)
}

type idleRig struct {
	clock *fakeClock
	src   *clockSource
	act   *Activity
	m     *Metrics
	done  chan struct{}
}

// startIdleRig runs RunLoop with a 1m source interval and the given IdleAfter.
// Each sleep advances the fake clock instead of waiting.
func startIdleRig(t *testing.T, idleAfter time.Duration, hook func(*idleRig)) *idleRig {
	t.Helper()
	t0 := time.Unix(1_800_000_000, 0)
	r := &idleRig{clock: newFakeClock(t0), act: NewActivity(t0), m: NewMetrics(), done: make(chan struct{})}
	r.src = &clockSource{clock: r.clock}
	ctx, cancel := context.WithCancel(context.Background())
	var sleeps atomic.Int32
	cfg := LoopConfig{
		Backoff:   []time.Duration{time.Minute},
		Now:       r.clock.Now,
		Activity:  r.act,
		IdleAfter: idleAfter,
		Sleep: func(d time.Duration) {
			if hook != nil && sleeps.Add(1) == 1 {
				hook(r)
			}
			r.clock.Advance(d)
			if len(r.src.Fetches()) > 100 {
				cancel() // safety net for the never-idle case
			}
		},
	}
	go func() {
		RunLoop(ctx, NewChain(r.src), NewState(), r.m, cfg)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
	})
	return r
}

func TestIdleStopsFetchingWithoutRequests(t *testing.T) {
	r := startIdleRig(t, 5*time.Minute, nil)
	waitFor(t, "idle", r.m.idle.Load)
	// Fetches at t0, +1m … +5m (5m is not *more* than IdleAfter), then idle.
	if n := len(r.src.Fetches()); n != 6 {
		t.Fatalf("fetches before idle = %d, want 6", n)
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(r.src.Fetches()); n != 6 {
		t.Fatalf("fetched while idle: %d", n)
	}
}

func TestIdleWakeFetchesImmediatelyThenNormalCadence(t *testing.T) {
	r := startIdleRig(t, 5*time.Minute, nil)
	waitFor(t, "idle", r.m.idle.Load)

	r.clock.Advance(time.Hour)
	wokeAt := r.clock.Now()
	r.act.Touch(wokeAt)
	waitFor(t, "idle again", func() bool { return len(r.src.Fetches()) == 12 && r.m.idle.Load() })

	f := r.src.Fetches()[6:]
	if !f[0].Equal(wokeAt) {
		t.Fatalf("first fetch after wake at %v, want immediately at %v", f[0], wokeAt)
	}
	for i := 1; i < len(f); i++ {
		if d := f[i].Sub(f[i-1]); d != time.Minute {
			t.Fatalf("cadence after wake: gap %d = %v, want 1m", i, d)
		}
	}
}

func TestIdleDisabledWhenZero(t *testing.T) {
	r := startIdleRig(t, 0, nil)
	<-r.done // runs until the safety-net cancel
	if r.m.idle.Load() || len(r.src.Fetches()) <= 100 {
		t.Fatalf("idle=%v fetches=%d", r.m.idle.Load(), len(r.src.Fetches()))
	}
}

func TestIdleIgnoresWakeDuringNormalSleep(t *testing.T) {
	// A request during the first sleep queues a wake but lastSeen stays at t0,
	// so the loop must still go idle at +6m rather than consume the stale wake.
	r := startIdleRig(t, 5*time.Minute, func(r *idleRig) { r.act.Touch(r.clock.Now()) })
	waitFor(t, "idle", r.m.idle.Load)
	time.Sleep(20 * time.Millisecond)
	if n := len(r.src.Fetches()); n != 6 {
		t.Fatalf("fetches = %d, want 6 (stale wake consumed?)", n)
	}
}

func TestHandlerOnlyUsageWakesLoop(t *testing.T) {
	t0 := time.Unix(1000, 0)
	now := t0
	cases := []struct {
		path, token, auth string
		wantWake          bool
	}{
		{"/usage", "", "", true},
		{"/metrics", "", "", false},
		{"/healthz", "", "", false},
		{"/usage", "s3cret", "", false}, // unauthorized requests don't count
		{"/usage", "s3cret", "Bearer s3cret", true},
	}
	for _, tc := range cases {
		t.Run(tc.path+" "+tc.auth, func(t *testing.T) {
			act := NewActivity(t0)
			h := NewHandler(NewState(), NewMetrics(), nil, func() time.Time { return now }, tc.token)
			h.Activity = act
			now = t0.Add(time.Hour)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)

			woke := false
			select {
			case <-act.wake:
				woke = true
			default:
			}
			if woke != tc.wantWake || act.LastSeen().Equal(now) != tc.wantWake {
				t.Fatalf("woke=%v lastSeen=%v", woke, act.LastSeen())
			}
		})
	}
}
