#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Command handling ──────────────────────────────────────────────────────────

COMMAND="${1:-}"

usage() {
  echo "Usage: ./docker.sh [start|stop]"
  echo ""
  echo "  start  — build and start production"
  echo "  stop   — stop all containers"
  echo ""
}

case "$COMMAND" in
  start)  ;;
  stop)
      echo ""s
      echo " Stopping GEE Web App..."
      echo ""
      docker compose --profile prod --profile dev down
      echo " All services stopped."
      echo ""
      exit 0
      ;;
esac

# ── Start (prod only) ─────────────────────────────────────────────────────────

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# ── Port selection ────────────────────────────────────────────────────────────

BACKEND_PORTS=(8000 8001 8002 8003)
FRONTEND_PORTS=(3000 3001 3002 3003)

pick_port() {
  local -n arr=$1
  for port in "${arr[@]}"; do
    if ! ss -ltn 2>/dev/null | grep -q ":${port} "; then
      echo "$port"
      return 0
    fi
  done
  echo ""
}

BACKEND_PORT="$(pick_port BACKEND_PORTS)"
if [[ -z "$BACKEND_PORT" ]]; then
  echo "ERROR: No free port for the backend in ${BACKEND_PORTS[*]}."
  exit 1
fi

FRONTEND_PORT="$(pick_port FRONTEND_PORTS)"
if [[ -z "$FRONTEND_PORT" ]]; then
  echo "ERROR: No free port for the frontend."
  exit 1
fi

# ── Persist ports to .env ─────────────────────────────────────────────────────

update_env() {
  local key="$1" val="$2"
  if grep -q "^${key}=" .env 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${val}|" .env
  else
    echo "${key}=${val}" >> .env
  fi
}
touch .env
update_env "HOST_UID"     "$HOST_UID"
update_env "HOST_GID"     "$HOST_GID"
update_env "BACKEND_PORT" "$BACKEND_PORT"
update_env "APP_PORT"     "$FRONTEND_PORT"

# ── Docker daemon check ───────────────────────────────────────────────────────

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker and try again."
  exit 1
fi

# ── Pixi conflict check ───────────────────────────────────────────────────────

if [ -f ".pixi.pid" ]; then
  PIXI_PID=$(cat .pixi.pid)
  if kill -0 "$PIXI_PID" 2>/dev/null; then
    PIXI_PORT=$(cat .pixi.port 2>/dev/null || echo "unknown")
    echo "ERROR: A Pixi-managed backend is already running (PID $PIXI_PID, port $PIXI_PORT)."
    echo "Stop it first with: ./Stop-pixi.sh"
    exit 1
  else
    rm -f .pixi.pid .pixi.port
  fi
fi

# ── Build & launch ────────────────────────────────────────────────────────────

echo "Building backend image..."
docker compose build backend

echo "Building React production image..."
docker compose build frontend

echo "Starting backend + nginx frontend..."
docker compose --profile prod up -d --force-recreate backend frontend

# ── Wait for backend ──────────────────────────────────────────────────────────

echo -n "Waiting for backend (http://localhost:${BACKEND_PORT}/api/gee-key)..."
backend_ready=0
for i in $(seq 1 40); do
  if curl -fsS "http://localhost:${BACKEND_PORT}/api/gee-key" >/dev/null 2>&1; then
    backend_ready=1
    break
  fi
  sleep 1
  echo -n "."
done
echo

if [[ $backend_ready -eq 0 ]]; then
  echo "WARNING: Backend did not respond after 40 s. Check logs:"
  echo "  docker compose logs -f backend"
fi

# ── Wait for frontend ─────────────────────────────────────────────────────────

FRONTEND_URL="http://localhost:${FRONTEND_PORT}"
echo -n "Waiting for frontend (${FRONTEND_URL})..."
frontend_ready=0
for i in $(seq 1 60); do
  if curl -fsS "${FRONTEND_URL}" >/dev/null 2>&1; then
    frontend_ready=1
    break
  fi
  sleep 1
  echo -n "."
done
echo

if [[ $frontend_ready -eq 0 ]]; then
  echo "ERROR: Frontend did not respond after 60 s."
  echo "  docker compose logs -f gee_frontend"
  exit 1
fi

# ── Ready ─────────────────────────────────────────────────────────────────────

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GEE Web App is ready"
echo "  Frontend : ${FRONTEND_URL}"
echo "  Backend  : http://localhost:${BACKEND_PORT}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "${FRONTEND_URL}" &
elif command -v open >/dev/null 2>&1; then
  open "${FRONTEND_URL}"
fi

echo "Run './docker.sh stop' when you are done."