#!/usr/bin/env python3
"""
Beacon Agent Simulator

Fires fake agent state transitions at the gateway so you can watch
the dashboard come alive without needing real agents.

Usage:
    python scripts/simulate.py                        # 5 agents, 10 events/sec
    python scripts/simulate.py --agents 20 --rate 50
    python scripts/simulate.py --url http://localhost:8010  # hit a gateway directly
"""
import argparse
import json
import random
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

STATUSES = [
    "running", "running", "running",   # weighted toward running
    "idle", "idle",
    "completed",
    "waiting",
    "error",
    "paused",
]

TASKS = [
    "data_fetch", "llm_call", "tool_use", "planning",
    "reflection", "memory_write", "memory_read", "web_search",
]


def _post(url: str, payload: dict) -> bool:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.status == 202
    except urllib.error.URLError as e:
        print(f"  [warn] {e.reason}")
        return False


def main() -> None:
    p = argparse.ArgumentParser(description="Beacon event simulator")
    p.add_argument("--agents", type=int, default=5,   help="Number of simulated agents (default: 5)")
    p.add_argument("--rate",   type=float, default=10.0, help="Events per second (default: 10)")
    p.add_argument("--url",    default="http://localhost:8000/ingest/state", help="Gateway or load-balancer URL")
    args = p.parse_args()

    agents = [f"agent-{str(i).zfill(3)}" for i in range(args.agents)]
    interval = 1.0 / args.rate
    sent = errors = 0

    print(f"Simulating {args.agents} agents at {args.rate:.0f} ev/s  →  {args.url}")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            agent_id = random.choice(agents)
            status   = random.choice(STATUSES)
            payload  = {
                "agent_id":  agent_id,
                "status":    status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata":  {"task": random.choice(TASKS)},
            }
            if _post(args.url, payload):
                sent += 1
            else:
                errors += 1

            if (sent + errors) % 100 == 0:
                print(f"  {sent} sent  {errors} errors")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\nDone.  sent={sent}  errors={errors}")


if __name__ == "__main__":
    main()
