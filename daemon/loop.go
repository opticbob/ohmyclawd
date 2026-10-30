package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"
)

// Activity records when a client last requested /usage so the loop can stop
// fetching while nobody is looking.
type Activity struct {
	last atomic.Int64 // unix nanos
	wake chan struct{}
}

// NewActivity starts as if a client asked at start, so the daemon fetches
// once at boot before any client connects.
func NewActivity(start time.Time) *Activity {
	a := &Activity{wake: make(chan struct{}, 1)}
	a.last.Store(start.UnixNano())
	return a
}

func (a *Activity) Touch(now time.Time) {
	a.last.Store(now.UnixNano())
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *Activity) LastSeen() time.Time { return time.Unix(0, a.last.Load()) }

type LoopConfig struct {
	RateLimited time.Duration
	Backoff     []time.Duration
	Sleep       func(time.Duration) // injectable for tests; nil -> context-aware timer
	Now         func() time.Time    // injectable for tests; nil -> time.Now
	Activity    *Activity           // nil disables idle gating
	IdleAfter   time.Duration       // 0 disables idle gating
}

// waitActive blocks while no client has asked for /usage within IdleAfter.
// It returns false if ctx is cancelled while idle.
func waitActive(ctx context.Context, cfg LoopConfig, m *Metrics) bool {
	a := cfg.Activity
	if a == nil || cfg.IdleAfter <= 0 {
		return true
	}
	// A wake from a request served during a normal sleep is stale: the data
	// it saw was already fresh.
	select {
	case <-a.wake:
	default:
	}
	if cfg.Now().Sub(a.LastSeen()) <= cfg.IdleAfter {
		return true
	}
	log.Printf("idle: no /usage requests for %s, pausing fetches", cfg.IdleAfter)
	m.idle.Store(true)
	defer m.idle.Store(false)
	select {
	case <-a.wake:
		log.Printf("active: /usage requested, resuming fetches")
		return true
	case <-ctx.Done():
		return false
	}
}

func RunLoop(ctx context.Context, c *Chain, s *State, m *Metrics, cfg LoopConfig) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	sleep := func(d time.Duration) {
		if cfg.Sleep != nil {
			cfg.Sleep(d)
			return
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
	idx := -1 // -1 means "no failures yet"
	for ctx.Err() == nil {
		if !waitActive(ctx, cfg, m) {
			return
		}
		m.IncProbe()
		u, interval, err := c.Fetch(ctx)
		cur, _ := s.Load()
		switch {
		case err == nil:
			u.OKTs = cfg.Now().Unix()
			s.Store(u)
			idx = -1
			sleep(interval)
		case IsRateLimited(err):
			m.IncFail()
			if u != nil {
				u.OKTs = cur.OKTs
				s.Store(u)
			}
			sleep(cfg.RateLimited)
		default:
			m.IncFail()
			// keep last good Usage; flip OK=false copy
			if cur != nil {
				cur.OK = false
				cur.Ts = cfg.Now().Unix()
				s.Store(cur)
			}
			idx++
			if idx >= len(cfg.Backoff) {
				idx = len(cfg.Backoff) - 1
			}
			sleep(cfg.Backoff[idx])
		}
	}
}
