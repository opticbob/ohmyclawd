package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// SessionWatcher reports how many Claude sessions are open and how many are
// waiting on the user. Kind names the backend currently answering
// ("herdr", "tmux") or "none" when nothing is reachable.
type SessionWatcher interface {
	Sessions() int
	Waiting() int
	Kind() string
	Run(ctx context.Context)
}

// healthWatcher is a SessionWatcher that knows whether its backend answered recently.
type healthWatcher interface {
	SessionWatcher
	Healthy() bool
}

// AutoWatcher prefers Primary (herdr) while it is healthy and falls back to
// Fallback (tmux) otherwise. Both run continuously so failover is immediate.
type AutoWatcher struct {
	Primary  healthWatcher
	Fallback healthWatcher
}

func (a *AutoWatcher) active() healthWatcher {
	switch {
	case a.Primary.Healthy():
		return a.Primary
	case a.Fallback.Healthy():
		return a.Fallback
	}
	return nil
}

func (a *AutoWatcher) Sessions() int {
	if w := a.active(); w != nil {
		return w.Sessions()
	}
	return 0
}

func (a *AutoWatcher) Waiting() int {
	if w := a.active(); w != nil {
		return w.Waiting()
	}
	return 0
}

func (a *AutoWatcher) Kind() string {
	if w := a.active(); w != nil {
		return w.Kind()
	}
	return "none"
}

func (a *AutoWatcher) Run(ctx context.Context) {
	go a.Primary.Run(ctx)
	a.Fallback.Run(ctx)
}

// noopWatcher is installed for OHMYCLAWD_SESSIONS=off.
type noopWatcher struct{}

func (noopWatcher) Sessions() int           { return 0 }
func (noopWatcher) Waiting() int            { return 0 }
func (noopWatcher) Kind() string            { return "none" }
func (noopWatcher) Run(ctx context.Context) { <-ctx.Done() }

// newSessionWatcher builds the watcher for OHMYCLAWD_SESSIONS.
func newSessionWatcher(mode, herdrSock string, match, waiting []string) (SessionWatcher, error) {
	switch mode {
	case "auto", "":
		return &AutoWatcher{Primary: NewHerdrWatcher(herdrSock, match, waiting), Fallback: NewTmuxWatcher()}, nil
	case "herdr":
		return NewHerdrWatcher(herdrSock, match, waiting), nil
	case "tmux":
		return NewTmuxWatcher(), nil
	case "off":
		return noopWatcher{}, nil
	}
	return nil, fmt.Errorf("OHMYCLAWD_SESSIONS: unknown mode %q (want auto|herdr|tmux|off)", mode)
}

// logKindChanges logs whenever the watcher answering for sessions changes.
func logKindChanges(ctx context.Context, w SessionWatcher, every time.Duration) {
	prev := ""
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		// Check after the first tick so watchers have polled once.
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if k := w.Kind(); k != prev {
			if prev == "" {
				log.Printf("session watcher: %s", k)
			} else {
				log.Printf("session watcher changed: %s -> %s", prev, k)
			}
			prev = k
		}
	}
}
