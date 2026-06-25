#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Beacon — start.sh
#
#  Starts the full pipeline locally. Only Redpanda uses Docker.
#
#  Usage:
#    ./start.sh                    # 2 gateways (default)
#    GATEWAY_COUNT=4 ./start.sh   # 4 gateways behind the load balancer
# ─────────────────────────────────────────────────────────────────────────────

# Re-exec with bash if called via `sh start.sh`
[ -z "${BASH_VERSION:-}" ] && exec bash "$0" "$@"

set -euo pipefail

GATEWAY_COUNT="${GATEWAY_COUNT:-2}"
GATEWAY_BASE_PORT="${GATEWAY_BASE_PORT:-8010}"
REDPANDA_IMAGE="redpandadata/redpanda:v24.2.1"
CONTAINER_NAME="beacon-redpanda"

export PYTHONPATH="$(pwd)"

# ── Terminal colours ──────────────────────────────────────────────
GRN='\033[0;32m'
DIM='\033[2m'
YLW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { printf "${GRN}▸${NC}  %s\n" "$*"; }
dim()  { printf "${DIM}   %s${NC}\n"  "$*"; }
warn() { printf "${YLW}▸${NC}  %s\n" "$*"; }
die()  { printf "${RED}✗${NC}  %s\n" "$*" >&2; exit 1; }

# ── PID tracking ─────────────────────────────────────────────────
declare -a PIDS=()

spawn() {
    # spawn <label> <cmd> [args...]
    local label="$1"; shift
    "$@" &
    local pid=$!
    PIDS+=("$pid")
    dim "${label}  (pid ${pid})"
}

# ── Cleanup on Ctrl-C / exit ──────────────────────────────────────
cleanup() {
    printf "\n"
    warn "Stopping services..."
    if [[ ${#PIDS[@]} -gt 0 ]]; then
        for pid in "${PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
        wait 2>/dev/null || true
    fi
    log "Python services stopped."

    warn "Stopping Redpanda..."
    docker stop "$CONTAINER_NAME" 2>/dev/null && \
        docker rm   "$CONTAINER_NAME" 2>/dev/null || true
    log "Done."
}
trap cleanup EXIT INT TERM

# ── Docker connectivity check ─────────────────────────────────────
# Tries the default Docker context first, then probes known socket
# paths one-by-one with an actual `docker info` to confirm the daemon
# is alive (not just that a stale socket file exists).
_ensure_docker() {
    # 1. Already works (e.g. DOCKER_HOST already set, or desktop context active)
    docker info &>/dev/null && return 0

    # 2. Probe candidate sockets
    local candidates=(
        "$HOME/.docker/run/docker.sock"
        "/var/run/docker.sock"
        "$HOME/Library/Containers/com.docker.docker/Data/vms/0/data/docker.sock"
    )
    for s in "${candidates[@]}"; do
        if [[ -S "$s" ]]; then
            DOCKER_HOST="unix://$s" docker info &>/dev/null && {
                export DOCKER_HOST="unix://$s"
                dim "Docker socket → $DOCKER_HOST"
                return 0
            }
        fi
    done

    return 1
}

if ! _ensure_docker; then
    die "Docker daemon is not responding. Open Docker Desktop and wait for it to finish starting, then re-run ./start.sh"
fi

# ─────────────────────────────────────────────────────────────────
# 0. Python dependencies
# ─────────────────────────────────────────────────────────────────
log "Checking Python dependencies..."
pip install -r requirements.txt -q --disable-pip-version-check
log "Dependencies OK."

# ─────────────────────────────────────────────────────────────────
# 1. Redpanda (Docker only)
# ─────────────────────────────────────────────────────────────────
log "Starting Redpanda..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d \
    --name "$CONTAINER_NAME" \
    -p 9092:9092 \
    -p 9644:9644 \
    "$REDPANDA_IMAGE" \
    redpanda start \
        --smp=1 \
        --memory=512M \
        --overprovisioned \
        --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
        --advertise-kafka-addr=PLAINTEXT://localhost:9092 \
    > /dev/null

log "Waiting for Redpanda..."
for i in $(seq 1 30); do
    if docker exec "$CONTAINER_NAME" rpk cluster info &>/dev/null 2>&1; then
        log "Redpanda ready."
        break
    fi
    [[ "$i" -eq 30 ]] && die "Redpanda did not start within 30 s."
    sleep 1
done

log "Creating Kafka topic..."
docker exec "$CONTAINER_NAME" \
    rpk topic create agent-state-transitions \
        --partitions 12 --replicas 1 2>/dev/null \
    && log "Topic created." \
    || log "Topic already exists — skipping."

# ─────────────────────────────────────────────────────────────────
# 2. Gateway instances
# ─────────────────────────────────────────────────────────────────
printf "\n"
log "Starting ${GATEWAY_COUNT} gateway instance(s)..."
GATEWAY_URLS=""
for i in $(seq 0 $((GATEWAY_COUNT - 1))); do
    PORT=$((GATEWAY_BASE_PORT + i))
    spawn "gateway-${i}    :${PORT}" \
        uvicorn services.gateway.main:app \
            --port "$PORT" \
            --log-level warning
    GATEWAY_URLS="${GATEWAY_URLS}http://localhost:${PORT},"
done
export GATEWAY_URLS="${GATEWAY_URLS%,}"

# ─────────────────────────────────────────────────────────────────
# 3. Load balancer
# ─────────────────────────────────────────────────────────────────
log "Starting Load Balancer on :8000  (backends: ${GATEWAY_URLS})..."
spawn "load-balancer :8000" \
    uvicorn infra.load_balancer.main:app \
        --port 8000 \
        --log-level warning

# ─────────────────────────────────────────────────────────────────
# 4. Archiver (Kafka → SQLite)
# ─────────────────────────────────────────────────────────────────
log "Starting Archiver  (metrics :8002)..."
spawn "archiver         :8002" \
    python services/archiver/main.py

# ─────────────────────────────────────────────────────────────────
# 5. Broadcaster (Kafka → WebSocket)
# ─────────────────────────────────────────────────────────────────
log "Starting Broadcaster on :8001..."
spawn "broadcaster     :8001" \
    uvicorn services.broadcaster.main:app \
        --port 8001 \
        --log-level warning

# ─────────────────────────────────────────────────────────────────
# 6. Watchtower
# ─────────────────────────────────────────────────────────────────
log "Starting Watchtower on :9000..."
spawn "watchtower      :9000" \
    uvicorn infra.watchtower.main:app \
        --port 9000 \
        --log-level warning

# ─────────────────────────────────────────────────────────────────
# 7. Prometheus (optional — native binary)
# ─────────────────────────────────────────────────────────────────
PROMETHEUS_URL=""
if command -v prometheus &>/dev/null; then
    log "Starting Prometheus on :9090..."
    spawn "prometheus      :9090" \
        prometheus \
            --config.file=infra/watchtower/prometheus.yml \
            --storage.tsdb.path=/tmp/beacon-prometheus \
            --web.listen-address=:9090 \
            --log.level=warn
    PROMETHEUS_URL="http://localhost:9090"
else
    warn "prometheus not found — skipping. Install: brew install prometheus"
fi

# ─────────────────────────────────────────────────────────────────
# Ready
# ─────────────────────────────────────────────────────────────────
sleep 2
printf "\n"
printf "${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
printf "  ${GRN}Beacon is live.${NC}\n"
printf "${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n\n"
printf "  ${DIM}POST  /ingest/state${NC}   →  http://localhost:8000\n"
printf "  ${DIM}WS    /ws${NC}             →  ws://localhost:8001/ws\n"
printf "  ${DIM}Status page${NC}           →  http://localhost:9000/status\n"
printf "  ${DIM}Metrics (LB)${NC}          →  http://localhost:8000/metrics\n"
[[ -n "$PROMETHEUS_URL" ]] && \
printf "  ${DIM}Prometheus${NC}            →  %s\n" "$PROMETHEUS_URL"
printf "\n  ${DIM}Gateways (%sx)${NC}\n" "$GATEWAY_COUNT"
for i in $(seq 0 $((GATEWAY_COUNT - 1))); do
    PORT=$((GATEWAY_BASE_PORT + i))
    printf "    ${DIM}gateway-%s${NC}  →  http://localhost:%s\n" "$i" "$PORT"
done
printf "\n  ${DIM}Add a backend at runtime:${NC}\n"
printf "  ${DIM}  curl -X POST localhost:8000/admin/backends \\ ${NC}\n"
printf "  ${DIM}       -H 'Content-Type: application/json'  \\ ${NC}\n"
printf "  ${DIM}       -d '{\"url\":\"http://localhost:8012\"}'${NC}\n"
printf "\n  Press ${YLW}Ctrl+C${NC} to stop.\n\n"

wait
