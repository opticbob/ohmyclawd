package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
)

// TmuxWatcher monitors tmux panes running Claude Code for idle state.
type TmuxWatcher struct {
	IdleThreshold time.Duration
	Interval      time.Duration

	sessions atomic.Int32
	waiting  atomic.Int32
	lastOK   atomic.Pointer[time.Time] // last successful list-panes (monotonic)
}

func NewTmuxWatcher() *TmuxWatcher {
	return &TmuxWatcher{
		IdleThreshold: 30 * time.Second,
		Interval:      5 * time.Second,
	}
}

func (tw *TmuxWatcher) Sessions() int { return int(tw.sessions.Load()) }
func (tw *TmuxWatcher) Waiting() int  { return int(tw.waiting.Load()) }

// Healthy reports whether tmux answered list-panes within the last three intervals.
func (tw *TmuxWatcher) Healthy() bool {
	last := tw.lastOK.Load()
	return last != nil && time.Since(*last) < 3*tw.Interval
}

func (tw *TmuxWatcher) Kind() string {
	if tw.Healthy() {
		return "tmux"
	}
	return "none"
}

func (tw *TmuxWatcher) Run(ctx context.Context) {
	type paneState struct {
		hash     string
		lastSeen time.Time
	}
	state := make(map[string]*paneState)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(tw.Interval):
		}

		panes, err := tw.findClaudePanes()
		tw.sessions.Store(int32(len(panes)))

		now := time.Now()
		if err == nil {
			tw.lastOK.Store(&now)
		}
		waiting := 0

		for _, pane := range panes {
			hash := tw.paneHash(pane)
			if s, ok := state[pane]; ok {
				if hash != s.hash {
					s.hash = hash
					s.lastSeen = now
				} else if now.Sub(s.lastSeen) >= tw.IdleThreshold {
					waiting++
				}
			} else {
				state[pane] = &paneState{hash: hash, lastSeen: now}
			}
		}

		// Clean up stale panes
		for k := range state {
			found := false
			for _, p := range panes {
				if p == k {
					found = true
					break
				}
			}
			if !found {
				delete(state, k)
			}
		}

		tw.waiting.Store(int32(waiting))
	}
}

func (tw *TmuxWatcher) findClaudePanes() ([]string, error) {
	out, err := exec.Command("tmux", "list-panes", "-a", "-F", "#{session_name}:#{window_index}.#{pane_index} #{pane_current_command}").Output()
	if err != nil {
		return nil, err
	}
	var panes []string
	for _, line := range strings.Split(string(out), "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "claude") || strings.Contains(lower, "kiro") {
			parts := strings.Fields(line)
			if len(parts) >= 1 {
				panes = append(panes, parts[0])
			}
		}
	}
	return panes, nil
}

func (tw *TmuxWatcher) paneHash(paneID string) string {
	out, err := exec.Command("tmux", "capture-pane", "-t", paneID, "-p").Output()
	if err != nil {
		return ""
	}
	content := string(out)
	// Check if Claude is actively working:
	// - ◼ = task in progress in task list
	// - ✢ = actively thinking/working spinner
	// - "N pending" pattern (digit + pending) = queued tasks
	if strings.Contains(content, "◼") ||
		strings.Contains(content, "✢") ||
		strings.Contains(content, " pending,") ||
		strings.Contains(content, " pending)") {
		return fmt.Sprintf("active-%d", time.Now().UnixNano())
	}
	// Hash last 10 lines for idle detection
	lines := bytes.Split(out, []byte("\n"))
	start := len(lines) - 10
	if start < 0 {
		start = 0
	}
	h := md5.Sum(bytes.Join(lines[start:], []byte("\n")))
	return fmt.Sprintf("%x", h)
}
