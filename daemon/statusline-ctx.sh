#!/bin/sh
# Claude Code status line for ohmyclawd: prints "ctx N%" and copies N onto
# this herdr pane as the "ctx" token, which ohmyclawd-daemon reads from
# pane.list and publishes per agent (the "c" key on /agents and MQTT).
#
# ~/.claude/settings.json:
#   "statusLine": {"type": "command", "command": "ohmyclawd-statusline"}
#
# Needs jq. Outside herdr it only prints.
input=$(cat)
pct=$(printf '%s' "$input" | jq -r '.context_window.used_percentage // 0 | round' 2>/dev/null) || pct=0
case "$pct" in '' | *[!0-9]*) pct=0 ;; esac

if [ "${HERDR_ENV:-}" = 1 ] && [ -n "${HERDR_PANE_ID:-}" ]; then
  # Only call herdr when the value changes; the status line redraws often.
  cache="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}/ohmyclawd-ctx-$(printf '%s' "$HERDR_PANE_ID" | tr -c 'A-Za-z0-9_-' _)"
  if [ "$(cat "$cache" 2>/dev/null)" != "$pct" ]; then
    printf '%s' "$pct" >"$cache"
    herdr=${HERDR_BIN_PATH:-herdr}
    if [ "$pct" -gt 0 ]; then
      "$herdr" pane report-metadata "$HERDR_PANE_ID" --source ohmyclawd --token "ctx=$pct" >/dev/null 2>&1 &
    else
      "$herdr" pane report-metadata "$HERDR_PANE_ID" --source ohmyclawd --clear-token ctx >/dev/null 2>&1 &
    fi
  fi
fi

[ "$pct" -gt 0 ] && printf 'ctx %s%%' "$pct"
exit 0
