#!/usr/bin/env bash
# Start grokcli-2api on Linux / macOS
set -euo pipefail
cd "$(dirname "$0")"

# Optional overrides:
#   export GROK2API_PORT=3000
#   export GROK2API_HOST=0.0.0.0          # listen all interfaces (server)
#   export GROK2API_OPEN_BROWSER=0        # headless: no browser
#   export GROK2API_ADMIN_PASSWORD='...'  # skip first-run UI password
#   export GROK2API_DEFAULT_MODEL=grok-4.5
#   export GROK2API_DATA_DIR=./data

if ! command -v python3 >/dev/null 2>&1 && ! command -v python >/dev/null 2>&1; then
  echo "ERROR: python3 not found. Install Python 3.10+ first." >&2
  exit 1
fi

PY=python3
command -v python3 >/dev/null 2>&1 || PY=python

# Ensure git submodule for registration engine (best-effort)
if [[ -f .gitmodules && ! -f grok-build-auth/xconsole_client/__init__.py ]]; then
  echo "[INFO] Initializing git submodule: grok-build-auth ..."
  if command -v git >/dev/null 2>&1; then
    git submodule update --init --recursive || true
  else
    echo "[WARN] git not found; cannot auto-init submodule"
    echo "       run manually: git submodule update --init --recursive"
  fi
fi

if ! $PY -c "import fastapi, uvicorn, httpx" 2>/dev/null; then
  echo "Installing dependencies..."
  $PY -m pip install -r requirements.txt
fi

if [[ -f grok-build-auth/requirements.txt ]]; then
  if ! $PY -c "import curl_cffi" 2>/dev/null; then
    echo "Installing grok-build-auth dependencies..."
    $PY -m pip install -r grok-build-auth/requirements.txt || true
  fi
fi

# Make xconsole_client importable
export PYTHONPATH="$(pwd)/grok-build-auth${PYTHONPATH:+:$PYTHONPATH}"

# Sensible defaults for servers (multi-account pool)
export GROK2API_OPEN_BROWSER="${GROK2API_OPEN_BROWSER:-0}"
export GROK2API_HOST="${GROK2API_HOST:-127.0.0.1}"
export GROK2API_PORT="${GROK2API_PORT:-3000}"
export GROK2API_ACCOUNT_MODE="${GROK2API_ACCOUNT_MODE:-round_robin}"
export GROK2API_TOKEN_MAINTAIN="${GROK2API_TOKEN_MAINTAIN:-1}"

PORT="$GROK2API_PORT"
echo "Starting grokcli-2api..."
echo "  Admin:  http://127.0.0.1:${PORT}/admin"
echo "  Health: http://127.0.0.1:${PORT}/health"
echo "  OpenAI: http://127.0.0.1:${PORT}/v1"
echo "  Account mode: ${GROK2API_ACCOUNT_MODE}"
echo ""
echo "  Auth (standalone, no local Grok CLI):"
echo "    1) Admin → 设备码登录 (native OIDC) — open URL on phone, enter code"
echo "    2) Import JWT / auth.json (merge on) → multi-account pool"
echo "    3) Mode round_robin rotates accounts; 401/429 auto-failover"
echo ""

exec $PY app.py
