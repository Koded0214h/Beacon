"""
Watchtower — Unified Status Page + Prometheus Sink

Polls /health on every Beacon service and exposes:
  GET /status   — JSON health summary for all services
  GET /metrics  — Prometheus metrics (gauge per service: 1=up, 0=down)

Run from project root:
    PYTHONPATH=. uvicorn infra.watchtower.main:app --port 9000
"""
import os
import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Gauge, make_asgi_app

# ── Service registry ──────────────────────────────────────────────
# Each value is a base URL.  /health is appended when polling.

def _build_registry() -> dict[str, str]:
    services: dict[str, str] = {
        "load-balancer": os.environ.get("LB_URL",           "http://localhost:8000"),
        "broadcaster":   os.environ.get("BROADCASTER_URL",  "http://localhost:8001"),
        "archiver":      os.environ.get("ARCHIVER_URL",     "http://localhost:8002"),
    }
    gateway_urls = os.environ.get("GATEWAY_URLS", "http://localhost:8010").split(",")
    for i, url in enumerate(u.strip() for u in gateway_urls if u.strip()):
        services[f"gateway-{i}"] = url
    return services


SERVICES = _build_registry()

SERVICE_UP = Gauge(
    "beacon_service_up",
    "1 if the service is reachable and healthy, 0 otherwise",
    ["service"],
)

# Initialise all gauges to 0 (unknown until first poll)
for name in SERVICES:
    SERVICE_UP.labels(service=name).set(0)

# ── Health polling ────────────────────────────────────────────────

async def _poll(client: httpx.AsyncClient, name: str, url: str) -> dict:
    try:
        r = await client.get(f"{url}/health", timeout=3.0)
        up = r.status_code == 200
        SERVICE_UP.labels(service=name).set(1 if up else 0)
        return {"service": name, "url": url, "status": "up" if up else "degraded", "http": r.status_code}
    except Exception as exc:
        SERVICE_UP.labels(service=name).set(0)
        return {"service": name, "url": url, "status": "down", "error": str(exc)}


async def _poll_all() -> list[dict]:
    async with httpx.AsyncClient() as client:
        return list(await asyncio.gather(*[
            _poll(client, name, url) for name, url in SERVICES.items()
        ]))


# Background loop that updates gauges every 10 s
async def _watch_loop() -> None:
    while True:
        await _poll_all()
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_watch_loop())
    yield
    task.cancel()


# ── App ───────────────────────────────────────────────────────────

app = FastAPI(title="Beacon Watchtower", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    results = await _poll_all()
    overall = all(r["status"] == "up" for r in results)
    return {
        "overall": "healthy" if overall else "degraded",
        "services": results,
    }
