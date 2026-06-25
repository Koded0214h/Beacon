# Beacon

**The nervous system for agent swarms.**

> the goal is not to use AI to build the whole system, I designed the architecture myself 😌

---

## The Problem

Here's what nobody tells you when you start running AI agents at scale.

They don't behave. 50,000 autonomous agents executing distributed workflows will fire state changes at you simultaneously, out of order, with absolutely no regard for what your database or your dashboard can actually handle. You've got `agent-3847` completing a task at the exact same millisecond `agent-0012` is throwing an error and `agent-9999` is spinning up cold for the first time. Every single one of them wants your attention. Right now.

So the question becomes:

> *How do you ingest, strictly order, and visualize concurrent state transitions from 50,000 autonomous AI agents executing distributed workflows in real-time — ensuring zero dropped events and sub-second dashboard updates — all without overwhelming your persistent audit database?*

Most people answer this by making the API wait for the database. The database gets hammered, starts falling behind, then falls over entirely. Now your agents are timing out, your dashboard is frozen, and you're getting paged at 3am for something that was a preventable architecture decision.

We had a different answer.

---

## The Answer

Beacon is a **strictly-ordered, high-throughput event pipeline** that sits between your agent swarm and everything downstream. It absorbs the chaos, forces order, and delivers a clean stream of events to wherever they need to go — in real time.

The core insight is this: **your database doesn't need to see every event the moment it happens. Your dashboard does.**

So we built two completely independent consumer groups. One runs fast and streams everything live to the frontend over WebSockets. One runs smart, batches events in memory, and flushes them to the database efficiently every 500 events or 2 seconds — whichever comes first. They never block each other. The dashboard doesn't care how slow the database is. The database doesn't care how fast the dashboard is.

That's the whole trick.

---

## Architecture

```
                    ┌──────────────────────────────────┐
                    │        50,000 AI Agents          │
                    └─────────────┬────────────────────┘
                                  │  POST /ingest/state
                                  ▼
                    ┌──────────────────────────────────┐
                    │   Load Balancer   :8000          │
                    │   Round-Robin across N gateways  │
                    └──────┬───────────┬───────────────┘
                           │           │
                    ┌──────▼──┐  ┌─────▼───┐
                    │Gateway 0│  │Gateway 1│  ...N
                    │  :8010  │  │  :8011  │
                    └──────┬──┘  └─────┬───┘
                           │           │
                           └─────┬─────┘
                                 │  key = agent_id
                                 ▼
                    ┌──────────────────────────────────┐
                    │     Redpanda / Kafka             │
                    │  topic: agent-state-transitions  │
                    │  12 partitions · ordered by key  │
                    └───────────────┬──────────────────┘
                                    │
                       ┌────────────┴────────────┐
                       │                         │
               ┌───────▼────────┐       ┌────────▼───────┐
               │   Archiver     │       │  Broadcaster   │
               │  Consumer Grp A│       │ Consumer Grp B │
               │                │       │                │
               │  batch=500     │       │  offset=latest │
               │  flush every 2s│       │  streams live  │
               └───────┬────────┘       └────────┬───────┘
                       │                         │
               ┌───────▼────────┐       ┌────────▼───────┐
               │   SQLite DB    │       │   Dashboard    │
               │   beacon.db    │       │  React  :5173  │
               └────────────────┘       └────────────────┘

                    ┌──────────────────────────────────┐
                    │  Watchtower  :9000               │
                    │  polls /health on all services   │
                    │  exposes /metrics for Prometheus │
                    └──────────────────────────────────┘
```

Events are keyed by `agent_id` before they hit Kafka. That single decision is what guarantees ordering — every state transition for a given agent lands in the same partition, in the exact sequence it was produced. No sorting required downstream.

---

## Services

| Service | Port | What it does |
|---|---|---|
| Load Balancer | `:8000` | Round-robin HTTP proxy across gateway instances. Add/remove backends at runtime via `/admin/backends`. |
| Gateway (×N) | `:8010+` | Accepts `POST /ingest/state`, produces to Kafka immediately, returns `202`. Never waits on a DB. |
| Broadcaster | `:8001` | WebSocket server. Kafka → browser in under a second. |
| Archiver | — | Background worker. Kafka → SQLite in efficient batches. Commits offsets only after a successful write. |
| Watchtower | `:9000` | Status page at `/status`. Prometheus metrics at `/metrics`. Polls every 10s. |
| Dashboard | `:5173` | Live feed + service status page. Black and white. No noise. |

---

## Running It

You need Docker (for Redpanda only) and Python 3.11+.

```bash
# Clone and enter
git clone <repo>
cd Beacon

# Activate your venv
python -m venv .venv && source .venv/bin/activate

# Start the whole stack (installs dependencies automatically)
./start.sh

# Open the dashboard
open http://localhost:5173
```

Want more gateway instances? Just set the count:

```bash
GATEWAY_COUNT=8 ./start.sh
```

---

## Seeing It Work

Once the stack is running, open a second terminal and start the simulator. It generates fake agent events so you can watch the dashboard come alive:

```bash
# 10 agents firing at 20 events/sec
python scripts/simulate.py --agents 10 --rate 20

# Crank it up
python scripts/simulate.py --agents 100 --rate 500
```

Then hit the gateway directly to see a single event flow end-to-end:

```bash
curl -X POST http://localhost:8000/ingest/state \
  -H "Content-Type: application/json" \
  -d '{
    "agent_id": "agent-001",
    "status": "running",
    "timestamp": "2026-06-26T12:00:00Z",
    "metadata": {"task": "web_search"}
  }'
```

**Verify each layer:**

```bash
# 1. Kafka — watch raw events as they arrive
docker exec beacon-redpanda \
  rpk topic consume agent-state-transitions --brokers localhost:9092

# 2. Database — check the archiver is writing
sqlite3 beacon.db "SELECT agent_id, status, timestamp FROM agent_events ORDER BY id DESC LIMIT 10;"

# 3. Dashboard — open http://localhost:5173 and switch to the feed tab

# 4. Service health — check the status tab in the dashboard, or curl directly
curl http://localhost:9000/status | python -m json.tool
```

---

## Add a Gateway at Runtime

The load balancer supports hot-adding backends without a restart:

```bash
# Start a new gateway instance
PYTHONPATH=. uvicorn services.gateway.main:app --port 8012 &

# Register it with the load balancer
curl -X POST http://localhost:8000/admin/backends \
  -H "Content-Type: application/json" \
  -d '{"url": "http://localhost:8012"}'

# Verify it's in rotation
curl http://localhost:8000/admin/backends
```

---

## Prometheus

If you have Prometheus installed (`brew install prometheus`), `start.sh` will launch it automatically on `:9090` and scrape metrics from every service.

If you don't, every service still exposes a `/metrics` endpoint in Prometheus format — the watchtower at `:9000/metrics` being the most useful starting point.

---

## Stack

- **Python** — FastAPI, confluent-kafka, SQLAlchemy, prometheus-client
- **Redpanda** — Kafka-compatible, lighter, runs in a single Docker container
- **React + Vite** — dashboard, no framework overhead
- **SQLite** — local persistence (swap for PostgreSQL in production without changing the archiver logic)

---

## What's Next

- Replace SQLite with PostgreSQL for production persistence
- Add a Grafana dashboard wired to the Prometheus metrics
- Swap the simulator for real agents
- Watch it handle 50,000 of them

---

*Built to solve a real problem. Not a toy.*
