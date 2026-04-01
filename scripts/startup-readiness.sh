#!/usr/bin/env bash
set -euo pipefail

# Poll a health endpoint until it is ready or until timeout.
URL="${1:-${READINESS_URL:-http://127.0.0.1:8000/health}}"
TIMEOUT_SECONDS="${READINESS_TIMEOUT_SECONDS:-60}"
INTERVAL_SECONDS="${READINESS_INTERVAL_SECONDS:-2}"

start_ts="$(date +%s)"

check_ready() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsS "${URL}" >/dev/null
    return $?
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -qO- "${URL}" >/dev/null
    return $?
  fi

  python - <<'PY'
import os
import sys
import urllib.request

url = os.environ["READINESS_CHECK_URL"]
try:
    with urllib.request.urlopen(url, timeout=3) as r:
        if 200 <= r.status < 300:
            sys.exit(0)
except Exception:
    pass
sys.exit(1)
PY
}

echo "[readiness] checking ${URL} (timeout=${TIMEOUT_SECONDS}s interval=${INTERVAL_SECONDS}s)"

while true; do
  if READINESS_CHECK_URL="${URL}" check_ready; then
    echo "[readiness] service is ready"
    exit 0
  fi

  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if [ "${elapsed}" -ge "${TIMEOUT_SECONDS}" ]; then
    echo "[readiness] timed out after ${elapsed}s waiting for ${URL}" >&2
    exit 1
  fi

  sleep "${INTERVAL_SECONDS}"
done
