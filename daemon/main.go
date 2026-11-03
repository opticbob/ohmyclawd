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
	tokenFile := getenv("OHMYCLAWD_TOKEN_FILE", defaultTokenFile())
	// Subcommands are dispatched before flag.Parse so `-fake` keeps working.
	if len(os.Args) > 1 && os.Args[1] == "token" {
		err := runTokenCmd(os.Args[2:], tokenFile, os.Stdout, os.Stderr)
		switch {
		case err == flag.ErrHelp:
		case err != nil:
			fmt.Fprintf(os.Stderr, "token: %v\n", err)
			os.Exit(1)
		}
		return
	}

	listen := getenv("OHMYCLAWD_LISTEN", "127.0.0.1:8787")
	token, err := resolveToken(os.Getenv("OHMYCLAWD_TOKEN"), tokenFile, log.Printf)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if token == "" && !isLoopback(listen) {
		log.Printf("warning: listening on %s with no token; anyone on the network can read /usage. Run `ohmyclawd-daemon token` to create one.", listen)
	}
	probeInterval := getenvDur("OHMYCLAWD_PROBE_INTERVAL", 60*time.Second)
	credsPath := getenv("OHMYCLAWD_CREDS_PATH", defaultCredsPath())
	anthropicURL := getenv("OHMYCLAWD_ANTHROPIC_URL", "https://api.anthropic.com/v1/messages")
	sources := splitList(getenv("OHMYCLAWD_SOURCES", "cli,probe"))
	opts := chainOpts{
		CredsPath:     credsPath,
		AnthropicURL:  anthropicURL,
		ProbeInterval: probeInterval,
		CLIInterval:   getenvDur("OHMYCLAWD_CLI_INTERVAL", 3*time.Minute),
		ClaudeBin:     getenv("OHMYCLAWD_CLAUDE_BIN", ""),
		ClaudeConfig:  getenv("OHMYCLAWD_CLAUDE_CONFIG_DIR", ""),
	}
	sessionsMode := strings.ToLower(getenv("OHMYCLAWD_SESSIONS", "auto"))
	agentMatch := splitList(getenv("OHMYCLAWD_AGENT_MATCH", "claude,kiro"))
	herdrWaiting := splitList(getenv("OHMYCLAWD_HERDR_WAITING", "blocked"))
	idleAfter := getenvDur("OHMYCLAWD_IDLE_AFTER", 5*time.Minute)
	fakeMode := flag.Bool("fake", false, "serve a scripted Usage curve, no Anthropic calls")
	flag.Parse()

	state := NewState()
	metrics := NewMetrics()
	sw, err := newSessionWatcher(sessionsMode, defaultHerdrSock(), agentMatch, herdrWaiting)
	if err != nil {
		log.Fatalf("%v", err)
	}
	handler := NewHandler(state, metrics, sw, time.Now, token)
	activity := NewActivity(time.Now())
	handler.Activity = activity

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sw.Run(ctx)
	go logKindChanges(ctx, sw, 5*time.Second)

	if *fakeMode {
		go runFake(ctx, state)
		log.Printf("ohmyclawd-daemon listening on %s (fake mode)", listen)
	} else {
		chain, err := buildChain(sources, opts)
		if err != nil {
			log.Fatalf("%v", err)
		}
		cfg := LoopConfig{
			RateLimited: 5 * time.Minute,
			Backoff:     []time.Duration{60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second, 600 * time.Second},
			Activity:    activity,
			IdleAfter:   idleAfter,
		}
		chain.Metrics = metrics
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
type chainOpts struct {
	CredsPath     string
	AnthropicURL  string
	ProbeInterval time.Duration
	CLIInterval   time.Duration
	ClaudeBin     string
	ClaudeConfig  string
}

func buildChain(names []string, o chainOpts) (*Chain, error) {
	var srcs []Source
	for _, n := range names {
		switch n {
		case "cli":
			srcs = append(srcs, &CLISource{Bin: o.ClaudeBin, ConfigDir: o.ClaudeConfig, Every: o.CLIInterval})
		case "probe":
			creds, err := LoadCreds(o.CredsPath)
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
					URL:   o.AnthropicURL,
					Token: creds.AccessToken,
					HTTP:  &http.Client{Timeout: 30 * time.Second},
				},
				ReloadCreds: func() (*Creds, error) { return LoadCreds(o.CredsPath) },
				Every:       o.ProbeInterval,
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
