#!/usr/bin/env bash
set -euo pipefail

# OhMyClawd daemon installer
# Usage: curl -fsSL https://raw.githubusercontent.com/opariffazman/ohmyclawd/master/install.sh | sudo bash
#
# Environment variables (optional):
#   OHMYCLAWD_USER    — user who runs claude code (defaults to SUDO_USER)
#   OHMYCLAWD_REPO    — GitHub repo to download the release from (default: opariffazman/ohmyclawd)
#   OHMYCLAWD_LISTEN  — bind address (default: 127.0.0.1:8787, use :8787 for LAN)
#   OHMYCLAWD_TOKEN   — bearer token for /usage and /metrics (optional; stored in
#                       /var/lib/ohmyclawd/token, generated on a fresh LAN install)
#   OHMYCLAWD_NO_TOKEN=1 — don't generate a token on a fresh LAN install
#   OHMYCLAWD_SOURCES, OHMYCLAWD_SESSIONS, OHMYCLAWD_IDLE_AFTER, ... — any daemon
#                       setting; existing values in the unit are kept on upgrade

REPO="${OHMYCLAWD_REPO:-opariffazman/ohmyclawd}"
BINARY="ohmyclawd-daemon-linux-amd64"
INSTALL_DIR="/usr/local/bin"
SERVICE_NAME="ohmyclawd-daemon"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
STATE_DIR="/var/lib/ohmyclawd"

# Settings carried across upgrades (OHMYCLAWD_<key> in the environment wins).
PRESERVE_KEYS=(LISTEN PROBE_INTERVAL SOURCES CLI_INTERVAL CLAUDE_BIN CLAUDE_CONFIG_DIR
  SESSIONS HERDR_SOCK AGENT_MATCH HERDR_WAITING HERDR_RECONCILE IDLE_AFTER TOKEN_FILE
  MQTT_URL MQTT_USER MQTT_PASSWORD_FILE MQTT_PREFIX MQTT_NODE HA_DISCOVERY NOTIFY_AFTER MAX_AGENTS)

is_loopback() {
  local host="${1%:*}"
  case "${host}" in
    127.*|localhost|"[::1]") return 0 ;;
  esac
  return 1
}

# set_env KEY VALUE — set (or uncomment) Environment=OHMYCLAWD_KEY in the unit.
set_env() {
  local val
  val=$(printf '%s' "$2" | sed 's/[&|\\]/\\&/g')
  if grep -qE "^(# )?Environment=OHMYCLAWD_$1=" "${SERVICE_FILE}"; then
    sed -i -E "s|^(# )?Environment=OHMYCLAWD_$1=.*|Environment=OHMYCLAWD_$1=${val}|" "${SERVICE_FILE}"
  else
    sed -i "/^StateDirectory=/i Environment=OHMYCLAWD_$1=${val}" "${SERVICE_FILE}"
  fi
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "error: must run as root (sudo)" >&2
  exit 1
fi

TARGET_USER="${OHMYCLAWD_USER:-${SUDO_USER:-}}"
if [[ -z "${TARGET_USER}" ]]; then
  echo "error: set OHMYCLAWD_USER=<user who runs claude code> or invoke via sudo" >&2
  exit 1
fi

if ! id -u "${TARGET_USER}" >/dev/null 2>&1; then
  echo "error: user '${TARGET_USER}' does not exist" >&2
  exit 1
fi
# %h in a system unit is root's home, so substitute the real one.
TARGET_HOME=$(getent passwd "${TARGET_USER}" | cut -d: -f6)

# --- Preserve existing config on upgrade ---
FRESH_INSTALL=1
[[ -f "${SERVICE_FILE}" ]] && FRESH_INSTALL=0
declare -A KEEP=()
for k in "${PRESERVE_KEYS[@]}"; do
  v=$(grep -oP "(?<=^Environment=OHMYCLAWD_${k}=).+" "${SERVICE_FILE}" 2>/dev/null || true)
  envname="OHMYCLAWD_${k}"
  [[ -n "${!envname:-}" ]] && v="${!envname}"
  [[ -n "${v}" ]] && KEEP[${k}]="${v}"
done
EXISTING_TOKEN=$(grep -oP '(?<=Environment=OHMYCLAWD_TOKEN=).+' "${SERVICE_FILE}" 2>/dev/null || true)
TOKEN="${OHMYCLAWD_TOKEN:-${EXISTING_TOKEN:-}}"
LISTEN="${KEEP[LISTEN]:-127.0.0.1:8787}"
TOKEN_FILE="${KEEP[TOKEN_FILE]:-${STATE_DIR}/token}"

echo "==> fetching latest release from ${REPO}..."
DOWNLOAD_URL=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
  | grep "browser_download_url.*${BINARY}" \
  | cut -d '"' -f 4)

if [[ -z "${DOWNLOAD_URL}" ]]; then
  echo "error: could not find ${BINARY} in latest release" >&2
  exit 1
fi

# --- Stop service before replacing binary (avoids "Text file busy") ---
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
  echo "==> stopping ${SERVICE_NAME}..."
  systemctl stop "${SERVICE_NAME}"
fi

echo "==> downloading ${DOWNLOAD_URL}..."
curl -fsSL -o "/tmp/${BINARY}" "${DOWNLOAD_URL}"
install -m 0755 "/tmp/${BINARY}" "${INSTALL_DIR}/ohmyclawd-daemon"
rm -f "/tmp/${BINARY}"

# --- Token lives in a 0600 file under the state dir, never in the unit ---
install -d -m 0700 -o "${TARGET_USER}" -g "${TARGET_USER}" "${STATE_DIR}"
GENERATED_TOKEN=""
if [[ -n "${TOKEN}" ]]; then
  (umask 077; printf '%s\n' "${TOKEN}" > "${TOKEN_FILE}")
  chown "${TARGET_USER}:${TARGET_USER}" "${TOKEN_FILE}"
elif [[ "${FRESH_INSTALL}" == 1 && "${OHMYCLAWD_NO_TOKEN:-}" != 1 ]] && ! is_loopback "${LISTEN}"; then
  GENERATED_TOKEN=$(sudo -u "${TARGET_USER}" env OHMYCLAWD_TOKEN_FILE="${TOKEN_FILE}" \
    "${INSTALL_DIR}/ohmyclawd-daemon" token)
fi
if [[ -f "${TOKEN_FILE}" ]]; then
  TOKEN=$(cat "${TOKEN_FILE}")
fi

echo "==> installing systemd service for user '${TARGET_USER}'..."
cat > "${SERVICE_FILE}" << EOF
[Unit]
Description=ohmyclawd daemon — reports Claude Code utilization and sessions
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/ohmyclawd-daemon
Restart=always
RestartSec=5
User=${TARGET_USER}
Group=${TARGET_USER}
# Bind address: 127.0.0.1:8787 (localhost) or :8787 (LAN)
Environment=OHMYCLAWD_LISTEN=127.0.0.1:8787
# Usage sources, tried in order: cli (claude -p /usage), probe (1-token API call).
# Set to "probe" for the original behavior.
Environment=OHMYCLAWD_SOURCES=cli,probe
Environment=OHMYCLAWD_PROBE_INTERVAL=60s
# Session counting: auto (herdr, else tmux) | herdr | tmux | off
# Environment=OHMYCLAWD_SESSIONS=auto
# MQTT + Home Assistant (off unless set). Password: ohmyclawd-daemon mqtt-password
# Environment=OHMYCLAWD_MQTT_URL=mqtt://homeassistant.local:1883
# Environment=OHMYCLAWD_MQTT_USER=ohmyclawd
# systemd's default PATH omits ~/.local/bin, where claude usually lives.
Environment=PATH=${TARGET_HOME}/.local/bin:/usr/local/bin:/usr/bin:/bin
# Bearer token: ${TOKEN_FILE} (show/create: sudo -u ${TARGET_USER} ohmyclawd-daemon token)
StateDirectory=ohmyclawd
StateDirectoryMode=0700
NoNewPrivileges=true
PrivateTmp=false
ProtectSystem=strict
ProtectHome=read-only
# The CLI refreshes OAuth credentials and caches state under ~/.claude.
ReadWritePaths=${TARGET_HOME}/.claude -${TARGET_HOME}/.claude.json

[Install]
WantedBy=multi-user.target
EOF

# Re-apply preserved values
for k in "${!KEEP[@]}"; do
  set_env "${k}" "${KEEP[${k}]}"
done

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"

echo "==> done! ohmyclawd-daemon is running"
systemctl --no-pager status "${SERVICE_NAME}.service" | head -5
echo ""
echo "config:"
echo "  listen: ${LISTEN}"
if [[ -n "${GENERATED_TOKEN}" ]]; then
  echo "  token:  ${GENERATED_TOKEN}  (generated; enter it as Daemon Token on the board)"
  echo "  usage:  curl -H 'Authorization: Bearer <token>' http://$(hostname).local:${LISTEN##*:}/usage"
elif [[ -n "${TOKEN}" ]]; then
  echo "  token:  set (${#TOKEN} chars) in ${TOKEN_FILE}"
  echo "  usage:  curl -H 'Authorization: Bearer <token>' http://$(hostname).local:${LISTEN##*:}/usage"
else
  echo "  token:  none (open access)"
  echo "  usage:  curl http://$(hostname).local:${LISTEN##*:}/usage"
fi
