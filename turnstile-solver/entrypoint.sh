#!/usr/bin/env bash
set -euo pipefail

HOST="${TURNSTILE_HOST:-127.0.0.1}"
PORT="${TURNSTILE_PORT:-5072}"
THREAD="${TURNSTILE_THREAD:-2}"
BROWSER_TYPE="${TURNSTILE_BROWSER_TYPE:-camoufox}"

case "${HOST}" in
  127.0.0.1|localhost|::1) ;;
  *)
    if [[ -z "${API_KEY:-}" ]]; then
      echo "ERROR: API_KEY is required when TURNSTILE_HOST is not loopback" >&2
      exit 1
    fi
    ;;
esac

cd /app
DEBUG_FLAG=()
if [[ "${TURNSTILE_DEBUG:-1}" == "1" || "${TURNSTILE_DEBUG:-true}" == "true" ]]; then
  DEBUG_FLAG=(--debug)
fi

mkdir -p /app/logs /app/keys

echo "[turnstile-solver] browser=${BROWSER_TYPE} thread=${THREAD} ${HOST}:${PORT}"
exec python api_solver.py \
  --browser_type "${BROWSER_TYPE}" \
  --thread "${THREAD}" \
  --host "${HOST}" \
  --port "${PORT}" \
  "${DEBUG_FLAG[@]}"
