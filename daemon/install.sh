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

SERVICE_NAME="ohmyclawd-daemon"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
STATE_DIR="/var/lib/ohmyclawd"
TOKEN_FILE="${STATE_DIR}/token"

FRESH_INSTALL=1
[[ -f "${SERVICE_FILE}" ]] && FRESH_INSTALL=0

is_loopback() {
  local host="${1%:*}"
  case "${host}" in
    127.*|localhost|"[::1]") return 0 ;;
  esac
  return 1
}

cd "$(dirname "$0")"

# Stop before replacing binary (avoids "Text file busy")
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
  echo "==> stopping ${SERVICE_NAME}..."
  systemctl stop "${SERVICE_NAME}"
fi

go build -trimpath -ldflags='-s -w' -o ohmyclawd-daemon .
install -m 0755 ohmyclawd-daemon /usr/local/bin/ohmyclawd-daemon

# Preserve existing env vars on upgrade
EXISTING_LISTEN=$(grep -oP '(?<=Environment=OHMYCLAWD_LISTEN=).+' "${SERVICE_FILE}" 2>/dev/null || true)
EXISTING_TOKEN=$(grep -oP '(?<=Environment=OHMYCLAWD_TOKEN=).+' "${SERVICE_FILE}" 2>/dev/null || true)
LISTEN="${OHMYCLAWD_LISTEN:-${EXISTING_LISTEN}}"
TOKEN="${OHMYCLAWD_TOKEN:-${EXISTING_TOKEN}}"

sed "s|__USER__|${TARGET_USER}|g" systemd/ohmyclawd-daemon.service \
  > "${SERVICE_FILE}"

# Re-apply preserved values
if [[ -n "${LISTEN}" ]]; then
  sed -i "s|Environment=OHMYCLAWD_LISTEN=.*|Environment=OHMYCLAWD_LISTEN=${LISTEN}|" "${SERVICE_FILE}"
fi
LISTEN=$(grep -oP '(?<=Environment=OHMYCLAWD_LISTEN=).+' "${SERVICE_FILE}")

# Token lives in a 0600 file under the state dir, never in the unit.
install -d -m 0700 -o "${TARGET_USER}" -g "${TARGET_USER}" "${STATE_DIR}"
GENERATED_TOKEN=""
if [[ -n "${TOKEN}" ]]; then
  (umask 077; printf '%s\n' "${TOKEN}" > "${TOKEN_FILE}")
  chown "${TARGET_USER}:${TARGET_USER}" "${TOKEN_FILE}"
  echo "==> bearer token stored in ${TOKEN_FILE}"
elif [[ "${FRESH_INSTALL}" == 1 && "${OHMYCLAWD_NO_TOKEN:-}" != 1 ]] && ! is_loopback "${LISTEN}"; then
  GENERATED_TOKEN=$(sudo -u "${TARGET_USER}" /usr/local/bin/ohmyclawd-daemon token)
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
