package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// defaultTokenFile is $STATE_DIRECTORY/token under systemd, else /var/lib/ohmyclawd/token.
func defaultTokenFile() string {
	if sd := os.Getenv("STATE_DIRECTORY"); sd != "" {
		// systemd passes a colon-separated list when several dirs are configured.
		return filepath.Join(strings.Split(sd, ":")[0], "token")
	}
	return "/var/lib/ohmyclawd/token"
}

// resolveToken picks the bearer token: OHMYCLAWD_TOKEN (deprecated), then the
// token file, then none (open access).
func resolveToken(envToken, path string, logf func(string, ...any)) (string, error) {
	if envToken != "" {
		logf("warning: OHMYCLAWD_TOKEN is deprecated; move it to %s (see `ohmyclawd-daemon token`)", path)
		return envToken, nil
	}
	return readTokenFile(path, logf)
}

// readTokenFile returns the trimmed token, or "" if the file doesn't exist.
func readTokenFile(path string, logf func(string, ...any)) (string, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 {
		logf("warning: token file %s is readable by group/others (mode %04o); chmod 600 it", path, fi.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		logf("warning: token file %s is empty; serving without auth", path)
	}
	return tok, nil
}

// ensureToken returns the token in path, creating it (or replacing it when
// rotate is set) with 32 hex characters from crypto/rand, mode 0600.
func ensureToken(path string, rotate bool) (string, error) {
	if !rotate {
		if data, err := os.ReadFile(path); err == nil {
			if tok := strings.TrimSpace(string(data)); tok != "" {
				return tok, nil
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(buf)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".token-*") // created 0600
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name()) // no-op after a successful rename
	if _, err := f.WriteString(tok + "\n"); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return "", err
	}
	return tok, nil
}

// runTokenCmd implements `ohmyclawd-daemon token [--rotate]`.
func runTokenCmd(args []string, path string, stdout, stderr io.Writer) error {
	fsf := flag.NewFlagSet("token", flag.ContinueOnError)
	fsf.SetOutput(stderr)
	rotate := fsf.Bool("rotate", false, "replace the existing token with a new one")
	fsf.Usage = func() {
		fmt.Fprintf(stderr, "usage: ohmyclawd-daemon token [--rotate]\n\nPrints the bearer token from %s, creating it if missing.\n", path)
		fsf.PrintDefaults()
	}
	if err := fsf.Parse(args); err != nil {
		return err
	}
	tok, err := ensureToken(path, *rotate)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, tok)
	return nil
}

// isLoopback reports whether a listen address only accepts local connections.
func isLoopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		host = listen
	}
	if host == "" {
		return false // ":8787" binds every interface
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
