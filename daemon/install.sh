#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "install.sh must run as root (sudo)" >&2
  exit 1
fi

TARGET_USER="${OHMYCLAWD_USER:-${SUDO_USER:-}}"
if [[ -z "${TARGET_USER}" ]]; then
  echo "Set OHMYCLAWD_USER=<user who runs claude code> or invoke via sudo." >&2
  exit 1
fi

if ! id -u "${TARGET_USER}" >/dev/null 2>&1; then
  echo "user '${TARGET_USER}' does not exist" >&2
  exit 1
fi
# %h in a system unit is root's home, so substitute the real one.
TARGET_HOME=$(getent passwd "${TARGET_USER}" | cut -d: -f6)

SERVICE_NAME="ohmyclawd-daemon"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
STATE_DIR="/var/lib/ohmyclawd"

# Settings carried across upgrades (OHMYCLAWD_<key> in the environment wins).
PRESERVE_KEYS=(LISTEN PROBE_INTERVAL SOURCES CLI_INTERVAL CLAUDE_BIN CLAUDE_CONFIG_DIR
  SESSIONS HERDR_SOCK AGENT_MATCH HERDR_WAITING HERDR_RECONCILE IDLE_AFTER TOKEN_FILE
  MQTT_URL MQTT_USER MQTT_PASSWORD_FILE MQTT_PREFIX MQTT_NODE HA_DISCOVERY NOTIFY_AFTER MAX_AGENTS)

FRESH_INSTALL=1
[[ -f "${SERVICE_FILE}" ]] && FRESH_INSTALL=0

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

cd "$(dirname "$0")"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found in root's PATH; try: sudo env PATH=\"\$PATH\" $0" >&2
  exit 1
fi

# Build before stopping so a failed build leaves the running daemon alone.
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT
go build -trimpath -ldflags='-s -w' -o "${BUILD_DIR}/ohmyclawd-daemon" .

# Stop before replacing binary (avoids "Text file busy")
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
  echo "==> stopping ${SERVICE_NAME}..."
  systemctl stop "${SERVICE_NAME}"
fi

install -m 0755 "${BUILD_DIR}/ohmyclawd-daemon" /usr/local/bin/ohmyclawd-daemon
# Claude Code status line that reports context use to herdr (see the script).
install -m 0755 statusline-ctx.sh /usr/local/bin/ohmyclawd-statusline

# Preserve existing env vars on upgrade
declare -A KEEP=()
for k in "${PRESERVE_KEYS[@]}"; do
  v=$(grep -oP "(?<=^Environment=OHMYCLAWD_${k}=).+" "${SERVICE_FILE}" 2>/dev/null || true)
  envname="OHMYCLAWD_${k}"
  [[ -n "${!envname:-}" ]] && v="${!envname}"
  [[ -n "${v}" ]] && KEEP[${k}]="${v}"
done
EXISTING_TOKEN=$(grep -oP '(?<=Environment=OHMYCLAWD_TOKEN=).+' "${SERVICE_FILE}" 2>/dev/null || true)
TOKEN="${OHMYCLAWD_TOKEN:-${EXISTING_TOKEN}}"

sed -e "s|__USER__|${TARGET_USER}|g" -e "s|__HOME__|${TARGET_HOME}|g" \
  systemd/ohmyclawd-daemon.service > "${SERVICE_FILE}"

# Re-apply preserved values
for k in "${!KEEP[@]}"; do
  set_env "${k}" "${KEEP[${k}]}"
done
LISTEN=$(grep -oP '(?<=^Environment=OHMYCLAWD_LISTEN=).+' "${SERVICE_FILE}")
TOKEN_FILE="${KEEP[TOKEN_FILE]:-${STATE_DIR}/token}"

# Token lives in a 0600 file under the state dir, never in the unit.
install -d -m 0700 -o "${TARGET_USER}" -g "${TARGET_USER}" "${STATE_DIR}"
GENERATED_TOKEN=""
if [[ -n "${TOKEN}" ]]; then
  (umask 077; printf '%s\n' "${TOKEN}" > "${TOKEN_FILE}")
  chown "${TARGET_USER}:${TARGET_USER}" "${TOKEN_FILE}"
  echo "==> bearer token stored in ${TOKEN_FILE}"
elif [[ "${FRESH_INSTALL}" == 1 && "${OHMYCLAWD_NO_TOKEN:-}" != 1 ]] && ! is_loopback "${LISTEN}"; then
  GENERATED_TOKEN=$(sudo -u "${TARGET_USER}" env OHMYCLAWD_TOKEN_FILE="${TOKEN_FILE}" \
    /usr/local/bin/ohmyclawd-daemon token)
fi

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"
systemctl --no-pager status "${SERVICE_NAME}.service" | head -10

if [[ -n "${GENERATED_TOKEN}" ]]; then
  echo ""
  echo "Generated a bearer token (enter it as Daemon Token on the board):"
  echo "  ${GENERATED_TOKEN}"
  echo "Show it again with: sudo -u ${TARGET_USER} ohmyclawd-daemon token"
fi
