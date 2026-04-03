#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Functions ─────────────────────────────────────────────────────────────────

usage() {
  echo ""
  echo " Usage: ./pixi.sh [start|stop]"
  echo ""
  echo "   start  -- build frontend and start backend via Pixi"
  echo "   stop   -- stop all Pixi-managed processes"
  echo ""
}

stop_app() {
  echo ""
  echo " Stopping GEE Web App (Pixi)..."
  echo ""

  PORT=8000
  if [[ -f ".pixi.port" ]]; then
    PORT=$(cat .pixi.port)
  fi

  # Kill by PID if we have it
  if [[ -f ".pixi.pid" ]]; then
    PIXI_PID=$(cat .pixi.pid)
    if kill -0 "$PIXI_PID" 2>/dev/null; then
      echo " Stopping PID $PIXI_PID..."
      kill "$PIXI_PID" 2>/dev/null || true
      sleep 1
      kill -9 "$PIXI_PID" 2>/dev/null || true
    fi
  fi

  # Safety net — kill anything still on the port
  PIDS=$(ss -tlnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' || true)
  for pid in $PIDS; do
    echo " Killing PID $pid on port $PORT..."
    kill -9 "$pid" 2>/dev/null || true
  done

  # Clean up state files
  rm -f .pixi.pid .pixi.port

  echo " Done."
  echo ""
}

start_app() {
  echo ""
  echo " GEE Web App - Pixi (no Docker)"
  echo " ================================"
  echo ""

  # --- Check pixi ---
  if ! command -v pixi &>/dev/null; then
    echo " Pixi not found."
    echo ""
    read -rp " Install Pixi now? [Y/N] " answer
    if [[ "$answer" =~ ^[Yy] ]]; then
      echo " Installing Pixi..."
      if ! curl -fsSL https://pixi.sh/install.sh | sh; then
        echo " Pixi installation failed. Please install manually and try again."
        exit 1
      fi
      export PATH="$HOME/.pixi/bin:$PATH"
      if ! command -v pixi &>/dev/null; then
        echo " Pixi installed but not found in PATH."
        echo " Please open a new terminal and run this script again."
        exit 1
      fi
      echo " Pixi ready."
      echo ""
    else
      echo " Pixi is required. Exiting."
      exit 1
    fi
  fi

  # --- Check for conflicting Docker containers ---
  if docker info &>/dev/null 2>&1; then
    RUNNING=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^gee_' || true)
    if [[ -n "$RUNNING" ]]; then
      echo " ERROR: Docker containers are already running:"
      echo "   $(echo "$RUNNING" | tr '\n' ' ')"
      echo " Stop them first with: ./docker.sh stop"
      echo ""
      exit 1
    fi
  fi

  # --- Find a free port ---
  PORT=""
  for p in 8000 8001 8002 8003; do
    if ! ss -ltn 2>/dev/null | grep -q ":${p} "; then
      PORT="$p"; break
    fi
  done
  if [[ -z "$PORT" ]]; then
    echo " No free port (tried 8000-8003). Free a port and try again."
    exit 1
  fi

  # --- Warn if GEE key missing ---
  if [[ ! -f "config/gee-key.json" ]]; then
    echo " WARNING: config/gee-key.json not found."
    echo " The app will start but GEE operations will fail until a key is uploaded."
    echo ""
  fi

  echo " App port : $PORT"
  echo ""

  # --- Build frontend ---
  echo " Building frontend..."
  pixi run build-frontend

  # --- Start backend ---
  echo " Starting backend..."
  GOOGLE_APPLICATION_CREDENTIALS=config/gee-key.json \
    pixi run uvicorn backend.app:app --host 0.0.0.0 --port "$PORT" > pixi.log 2>&1 &
  echo $! > .pixi.pid
  echo "$PORT" > .pixi.port

  # --- Wait for ready ---
  echo -n " Waiting for app"
  ready=0
  for i in $(seq 1 60); do
    if curl -fsS "http://localhost:${PORT}/api/gee-key" >/dev/null 2>&1; then
      ready=1; break
    fi
    sleep 1; echo -n "."
  done
  echo ""

  if [[ $ready -eq 0 ]]; then
    echo ""
    echo " ERROR: App did not respond after 60 s. Check pixi.log for details."
    exit 1
  fi

  echo ""
  echo " =========================================="
  echo "  GEE Web App is ready"
  echo "  http://localhost:${PORT}"
  echo " =========================================="
  echo ""

  xdg-open "http://localhost:${PORT}" 2>/dev/null || \
    open "http://localhost:${PORT}" 2>/dev/null || true

  echo " Run: ./pixi.sh stop -- when you are done."
  echo ""
}

# ── Entry point ───────────────────────────────────────────────────────────────

COMMAND="${1:-}"

case "$COMMAND" in
  start)  start_app ;;
  stop)   stop_app ;;
  *)      usage; exit 1 ;;
esac