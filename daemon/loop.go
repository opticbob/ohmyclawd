package main

import (
	"context"
	"time"
)

type LoopConfig struct {
	RateLimited time.Duration
	Backoff     []time.Duration
	Sleep       func(time.Duration) // injectable for tests; nil -> context-aware timer
	Now         func() time.Time    // injectable for tests; nil -> time.Now
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
