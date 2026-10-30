package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

type logSink struct{ lines []string }

func (l *logSink) logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *logSink) has(sub string) bool {
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil { // WriteFile honors umask
		t.Fatal(err)
	}
}

func TestResolveTokenPrecedence(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	missing := filepath.Join(dir, "missing")
	writeFile(t, file, "  from-file \n", 0o600)

	cases := []struct {
		name, env, path, want string
		wantDeprecation       bool
	}{
		{"env wins", "from-env", file, "from-env", true},
		{"file, trimmed", "", file, "from-file", false},
		{"none", "", missing, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var l logSink
			got, err := resolveToken(tc.env, tc.path, l.logf)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || l.has("deprecated") != tc.wantDeprecation {
				t.Fatalf("got %q, logs %v", got, l.lines)
			}
		})
	}
}

func TestReadTokenFilePermissionWarning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permissions")
	}
	cases := []struct {
		mode os.FileMode
		warn bool
	}{
		{0o600, false},
		{0o400, false},
		{0o640, true},
		{0o644, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%04o", tc.mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token")
			writeFile(t, path, "abc\n", tc.mode)
			var l logSink
			tok, err := readTokenFile(path, l.logf)
			if err != nil || tok != "abc" {
				t.Fatalf("tok=%q err=%v", tok, err)
			}
			if l.has("readable by group/others") != tc.warn {
				t.Fatalf("logs = %v", l.lines)
			}
		})
	}
}

var hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestEnsureTokenCreateAndRotate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "token")

	tok1, err := ensureToken(path, false)
	if err != nil || !hex32.MatchString(tok1) {
		t.Fatalf("create: %q %v", tok1, err)
	}
	if runtime.GOOS != "windows" {
		fi, _ := os.Stat(path)
		if fi.Mode().Perm() != 0o600 {
			t.Fatalf("mode = %04o", fi.Mode().Perm())
		}
		di, _ := os.Stat(filepath.Dir(path))
		if di.Mode().Perm() != 0o700 {
			t.Fatalf("dir mode = %04o", di.Mode().Perm())
		}
	}

	tok2, err := ensureToken(path, false)
	if err != nil || tok2 != tok1 {
		t.Fatalf("reuse: %q != %q (%v)", tok2, tok1, err)
	}

	tok3, err := ensureToken(path, true)
	if err != nil || tok3 == tok1 || !hex32.MatchString(tok3) {
		t.Fatalf("rotate: %q (%v)", tok3, err)
	}
	if got, _ := readTokenFile(path, (&logSink{}).logf); got != tok3 {
		t.Fatalf("file holds %q, want %q", got, tok3)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("leftover temp files: %v", entries)
	}
}

func TestEnsureTokenReplacesBlankFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	writeFile(t, path, " \n", 0o600)
	tok, err := ensureToken(path, false)
	if err != nil || !hex32.MatchString(tok) {
		t.Fatalf("%q %v", tok, err)
	}
}

func TestRunTokenCmd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	var out, errb bytes.Buffer
	if err := runTokenCmd(nil, path, &out, &errb); err != nil {
		t.Fatal(err)
	}
	first := strings.TrimSpace(out.String())
	out.Reset()
	if err := runTokenCmd([]string{"--rotate"}, path, &out, &errb); err != nil {
		t.Fatal(err)
	}
	if second := strings.TrimSpace(out.String()); second == first || !hex32.MatchString(second) {
		t.Fatalf("rotate printed %q (first %q)", second, first)
	}
	if err := runTokenCmd([]string{"--bogus"}, path, &out, &errb); err == nil {
		t.Fatal("unknown flag: want error")
	}
}

func TestIsLoopback(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8787":     true,
		"localhost:8787":     true,
		"[::1]:8787":         true,
		":8787":              false,
		"0.0.0.0:8787":       false,
		"[::]:8787":          false,
		"192.168.1.10:8787":  false,
		"ohmyclawd.lan:8787": false,
	}
	for in, want := range cases {
		if got := isLoopback(in); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestDefaultTokenFile(t *testing.T) {
	t.Setenv("STATE_DIRECTORY", "/var/lib/ohmyclawd:/var/lib/other")
	if got := defaultTokenFile(); got != filepath.Join("/var/lib/ohmyclawd", "token") {
		t.Fatalf("got %q", got)
	}
	t.Setenv("STATE_DIRECTORY", "")
	if got := defaultTokenFile(); got != "/var/lib/ohmyclawd/token" {
		t.Fatalf("got %q", got)
	}
}
