# ohmyclawd daemon

Reads Claude Code session (5h) and weekly (7d) utilization, counts open Claude sessions, serves compact JSON. ESP32 polls over HTTP.

## How it works

**Usage sources**, tried in order (`OHMYCLAWD_SOURCES`, default `cli,probe`). First success wins:

| Source | How | Cadence |
|--------|-----|---------|
| `cli` | Runs `claude -p /usage` (hooks, MCP and session persistence off; no tokens used) and parses the "Current session" / "Current week" lines | `OHMYCLAWD_CLI_INTERVAL` (3m) |
| `probe` | 1-Haiku-token call to `/v1/messages`, parses `anthropic-ratelimit-unified-*` headers | `OHMYCLAWD_PROBE_INTERVAL` (60s) |

If the CLI is missing, logged out, or its output can't be parsed, the probe answers instead. It never reports zeros as data. If `~/.claude/.credentials.json` is missing, the probe is dropped with a warning (fatal only when `probe` is the only source).

**Session counting** (`OHMYCLAWD_SESSIONS`, default `auto`):

| Mode | Behavior |
|------|----------|
| `auto` | Use [herdr](https://herdr.dev) while its socket answers, else tmux |
| `herdr` | herdr `pane.list` over `~/.config/herdr/herdr.sock`. `cs` = panes whose agent matches `OHMYCLAWD_AGENT_MATCH`, `cw` = those with `agent_status` in `OHMYCLAWD_HERDR_WAITING` |
| `tmux` | Panes running claude/kiro; waiting = screen unchanged ≥30s |
| `off` | Always 0 |

**Idle gating**: when no client has requested `/usage` for `OHMYCLAWD_IDLE_AFTER` (5m), fetching stops; the next `/usage` request triggers an immediate fetch. `/metrics` scrapes don't count.

## Install

```bash
# remote install (downloads latest release binary)
curl -fsSL https://raw.githubusercontent.com/opariffazman/ohmyclawd/master/install.sh | sudo bash

# LAN access (a bearer token is generated and printed on a fresh install)
curl -fsSL https://raw.githubusercontent.com/opariffazman/ohmyclawd/master/install.sh \
  | OHMYCLAWD_LISTEN=:8787 sudo -E bash

# from a fork's releases
curl -fsSL https://raw.githubusercontent.com/<you>/ohmyclawd/master/install.sh \
  | OHMYCLAWD_REPO=<you>/ohmyclawd sudo -E bash

# local build install
sudo OHMYCLAWD_USER=youruser ./daemon/install.sh
```

Builds static binary → `/usr/local/bin/ohmyclawd-daemon`. Installs a hardened systemd unit (`ProtectSystem=strict`, `ProtectHome=read-only`, only `~/.claude` writable) running as the target user. Re-running the installer upgrades in place and keeps every `OHMYCLAWD_*` setting in the unit.

## Config

Env vars on systemd unit. Edit `/etc/systemd/system/ohmyclawd-daemon.service` (or add a drop-in with `systemctl edit ohmyclawd-daemon`), then `systemctl daemon-reload && systemctl restart ohmyclawd-daemon`.

| Var | Default | What |
|-----|---------|------|
| `OHMYCLAWD_LISTEN` | `127.0.0.1:8787` | Bind address. `:8787` for LAN |
| `OHMYCLAWD_SOURCES` | `cli,probe` | Ordered usage sources. `probe` = original behavior |
| `OHMYCLAWD_CLI_INTERVAL` | `3m` | Fetch interval while the CLI source succeeds |
| `OHMYCLAWD_PROBE_INTERVAL` | `60s` | Fetch interval while the probe succeeds |
| `OHMYCLAWD_CLAUDE_BIN` | auto | `claude` path. Auto: `$PATH`, `~/.local/bin`, `~/.claude/local`, `/usr/local/bin`, `/opt/homebrew/bin` |
| `OHMYCLAWD_CLAUDE_CONFIG_DIR` | unset | Passed to the CLI as `CLAUDE_CONFIG_DIR`. Unset = default account |
| `OHMYCLAWD_SESSIONS` | `auto` | `auto` \| `herdr` \| `tmux` \| `off` |
| `OHMYCLAWD_HERDR_SOCK` | auto | herdr socket. Falls back to `HERDR_SOCK`, `HERDR_SOCKET_PATH`, `~/.config/herdr/herdr.sock` |
| `OHMYCLAWD_AGENT_MATCH` | `claude,kiro` | herdr agent names counted as sessions (substring, case-insensitive) |
| `OHMYCLAWD_HERDR_WAITING` | `blocked` | herdr `agent_status` values counted as waiting (`cw`). `blocked` = permission prompt or question |
| `OHMYCLAWD_IDLE_AFTER` | `5m` | Stop fetching after no `/usage` requests for this long. `0` = never |
| `OHMYCLAWD_TOKEN_FILE` | `$STATE_DIRECTORY/token`, else `/var/lib/ohmyclawd/token` | Bearer token file |
| `OHMYCLAWD_TOKEN` | *(empty)* | **Deprecated.** Bearer token; overrides the file, logs a warning |
| `OHMYCLAWD_CREDS_PATH` | `~/.claude/.credentials.json` | OAuth creds path (probe) |
| `OHMYCLAWD_ANTHROPIC_URL` | `https://api.anthropic.com/v1/messages` | API endpoint (probe) |

### Back to the old behavior

```ini
Environment=OHMYCLAWD_SOURCES=probe
Environment=OHMYCLAWD_SESSIONS=tmux
Environment=OHMYCLAWD_IDLE_AFTER=0
```

### Token

The bearer token lives in a mode-0600 file, not the unit (unit files are world-readable). If the file exists, `/usage` and `/metrics` require `Authorization: Bearer <token>`.

```bash
sudo -u youruser ohmyclawd-daemon token           # print (creates one if missing)
sudo -u youruser ohmyclawd-daemon token --rotate  # replace
sudo rm /var/lib/ohmyclawd/token                  # back to open access
```

Tokens are 32 hex chars (the board's field holds 40). Upgrading migrates an `Environment=OHMYCLAWD_TOKEN=` line into the file. A fresh install with a non-loopback `LISTEN` generates one unless `OHMYCLAWD_NO_TOKEN=1`. A LAN listener without a token logs a warning but still starts.

### Access modes

| Scenario | LISTEN | Token file |
|----------|--------|------------|
| Local only (default) | `127.0.0.1:8787` | absent |
| LAN no auth | `:8787` | absent |
| LAN + auth | `:8787` | present |
| CF Tunnel (remote) | `127.0.0.1:8787` | present |

### WSL

- systemd must be enabled: `[boot]` / `systemd=true` in `/etc/wsl.conf`, then `wsl --shutdown`.
- LAN access to the board needs either mirrored networking (`networkingMode=mirrored` in `%UserProfile%\.wslconfig`) or a Windows `netsh interface portproxy` rule to the WSL IP, plus an inbound firewall rule for port 8787.

## Endpoints

| Path | Auth | Response |
|------|------|----------|
| `GET /usage` | token (if set) | JSON, see below |
| `GET /healthz` | none | `ok` plain text |
| `GET /metrics` | token (if set) | Prometheus exposition |

### `/usage`

```json
{"s":27,"sr":174,"w":9,"wr":7514,"st":"allowed","ok":true,"ts":1793385938,"cs":5,"cw":1,
 "src":"cli","age":5,"sw":"herdr"}
```

| Key | What |
|-----|------|
| `s` / `w` | Session (5h) / weekly (7d) utilization, 0–100 |
| `sr` / `wr` | Minutes until each window resets |
| `st` | `allowed` \| `limited` |
| `ok` | Latest fetch succeeded (on failure the last good values are kept) |
| `ts` | Unix time of the latest fetch attempt |
| `cs` / `cw` | Claude sessions / sessions waiting on you |
| `src` | Source of the data: `cli`, `probe`, `fake` |
| `age` | Seconds since the last *successful* fetch (`-1` = none yet) |
| `err` | Latest error, only when the latest attempt failed |
| `sw` | Session watcher answering: `herdr`, `tmux`, `none` |
| `lim` | Per-model weekly rows from the CLI, e.g. `[{"l":"Sonnet only","p":12,"r":4870}]` (label, %, reset minutes); omitted when none |

The first nine keys are unchanged; new keys are additive and ignored by older firmware.

### `/metrics`

Existing: `ohmyclawd_probe_total`, `ohmyclawd_probe_fail_total`, `ohmyclawd_probe_last_age_seconds` (fetch attempts across all sources).

New: `ohmyclawd_source_fetch_total{src}`, `ohmyclawd_source_fail_total{src}`, `ohmyclawd_source_active{src}`, `ohmyclawd_data_age_seconds`, `ohmyclawd_idle`, `ohmyclawd_sessions`, `ohmyclawd_sessions_waiting`, `ohmyclawd_session_watcher{kind}`.

The journal logs a line when the winning source or the session watcher changes (`usage source changed: cli -> probe`), and when fetching goes idle or resumes.

## Fake mode

```bash
ohmyclawd-daemon --fake
```

Scripted utilization curve. No Anthropic calls. Test firmware in isolation.

## Tests

```bash
go test -race -count=1 ./...
```
