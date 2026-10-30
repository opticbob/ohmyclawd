package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	listen := getenv("OHMYCLAWD_LISTEN", "127.0.0.1:8787")
	token := getenv("OHMYCLAWD_TOKEN", "")
	probeInterval := getenvDur("OHMYCLAWD_PROBE_INTERVAL", 60*time.Second)
	credsPath := getenv("OHMYCLAWD_CREDS_PATH", defaultCredsPath())
	anthropicURL := getenv("OHMYCLAWD_ANTHROPIC_URL", "https://api.anthropic.com/v1/messages")
	sources := splitList(getenv("OHMYCLAWD_SOURCES", "probe"))
	fakeMode := flag.Bool("fake", false, "serve a scripted Usage curve, no Anthropic calls")
	flag.Parse()

	state := NewState()
	metrics := NewMetrics()
	tmux := NewTmuxWatcher()
	handler := NewHandler(state, metrics, tmux, time.Now, token)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tmux.Run(ctx)

	if *fakeMode {
		go runFake(ctx, state)
		log.Printf("ohmyclawd-daemon listening on %s (fake mode)", listen)
	} else {
		chain, err := buildChain(sources, credsPath, anthropicURL, probeInterval)
		if err != nil {
			log.Fatalf("%v", err)
		}
		cfg := LoopConfig{
			RateLimited: 5 * time.Minute,
			Backoff:     []time.Duration{60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second, 600 * time.Second},
		}
		go RunLoop(ctx, chain, state, metrics, cfg)
		log.Printf("ohmyclawd-daemon listening on %s (sources %s)", listen, strings.Join(sources, ","))
	}

	srv := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

// buildChain assembles the ordered usage sources. Missing credentials are
// fatal only when the probe is the sole source; otherwise the probe is
// dropped from the chain with a warning.
func buildChain(names []string, credsPath, anthropicURL string, probeInterval time.Duration) (*Chain, error) {
	var srcs []Source
	for _, n := range names {
		switch n {
		case "probe":
			creds, err := LoadCreds(credsPath)
			if err != nil {
				if len(names) == 1 {
					return nil, fmt.Errorf("credentials: %w", err)
				}
				log.Printf("warning: credentials: %v; probe source disabled", err)
				continue
			}
			log.Printf("loaded OAuth token, expires unix-ms=%d", creds.ExpiresAt)
			srcs = append(srcs, &ProbeSource{
				P: &Prober{
					URL:   anthropicURL,
					Token: creds.AccessToken,
					HTTP:  &http.Client{Timeout: 30 * time.Second},
				},
				ReloadCreds: func() (*Creds, error) { return LoadCreds(credsPath) },
				Every:       probeInterval,
			})
		default:
			return nil, fmt.Errorf("OHMYCLAWD_SOURCES: unknown source %q", n)
		}
	}
	if len(srcs) == 0 {
		return nil, fmt.Errorf("OHMYCLAWD_SOURCES: no usable sources in %q", strings.Join(names, ","))
	}
	return NewChain(srcs...), nil
}

// splitList parses a comma-separated env value, trimming blanks and lowercasing.
func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.ToLower(strings.TrimSpace(f)); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func defaultCredsPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".credentials.json"
	}
	return filepath.Join(home, ".claude", ".credentials.json")
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getenvDur(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if dur, err := time.ParseDuration(v); err == nil {
			return dur
		}
	}
	return d
}
