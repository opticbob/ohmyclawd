package main

import (
	"context"
	"time"
)

// runFake stores a scripted Usage curve so the firmware can be exercised
// end-to-end without making real Anthropic calls. The curve cycles every 60 s
// through allowed/limited states and increasing/decreasing utilization.
func runFake(ctx context.Context, s *State) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	steps := []Usage{
		{S: 10, SR: 250, W: 5, WR: 6500, ST: "allowed", OK: true},
		{S: 35, SR: 240, W: 12, WR: 6490, ST: "allowed", OK: true},
		{S: 60, SR: 230, W: 30, WR: 6480, ST: "allowed", OK: true},
		{S: 88, SR: 220, W: 55, WR: 6470, ST: "allowed", OK: true},
		{S: 100, SR: 210, W: 78, WR: 6460, ST: "limited", OK: true},
	}
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cp := steps[i%len(steps)]
			cp.Ts = time.Now().Unix()
			cp.OKTs = cp.Ts
			cp.Src = "fake"
			s.Store(&cp)
			i++
		}
	}
}
