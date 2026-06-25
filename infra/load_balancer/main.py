"""
Load Balancer — Round-Robin HTTP Proxy

Reads backend gateway URLs from the GATEWAY_URLS env var (comma-separated).
Forwards every request to the next backend in rotation.
Exposes /admin/backends to add or remove backends at runtime.

Run from project root:
    GATEWAY_URLS=http://localhost:8010,http://localhost:8011 \\
    PYTHONPATH=. uvicorn infra.load_balancer.main:app --port 8000
"""
import os
import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app

# ── Backends ──────────────────────────────────────────────────────

_raw = os.environ.get("GATEWAY_URLS", "http://localhost:8010")
_initial_backends = [u.strip() for u in _raw.split(",") if u.strip()]


class RoundRobin:
    """Thread-safe async round-robin backend selector."""

    def __init__(self, backends: list[str]) -> None:
        self._backends: list[str] = list(backends)
        self._index: int = 0
        self._lock = asyncio.Lock()

    async def next(self) -> str:
        async with self._lock:
            if not self._backends:
                raise RuntimeError("No backends registered")
            backend = self._backends[self._index % len(self._backends)]
            self._index = (self._index + 1) % len(self._backends)
            return backend

    async def add(self, url: str) -> None:
        async with self._lock:
            if url not in self._backends:
                self._backends.append(url)
                self._index = self._index % len(self._backends)

    async def remove(self, url: str) -> None:
        async with self._lock:
            if url in self._backends:
                self._backends.remove(url)
                if self._backends:
                    self._index = self._index % len(self._backends)
                else:
                    self._index = 0

    @property
    def backends(self) -> list[str]:
        return list(self._backends)


rr = RoundRobin(_initial_backends)

# ── Prometheus metrics ────────────────────────────────────────────

LB_REQUESTS = Counter(
    "lb_requests_total", "Requests proxied", ["backend", "status_code"]
)
LB_ERRORS = Counter(
    "lb_upstream_errors_total", "Upstream connection errors", ["backend"]
)
LB_LATENCY = Histogram(
    "lb_request_duration_seconds", "Proxy round-trip latency", ["backend"]
)
LB_BACKENDS = Gauge("lb_backends_total", "Registered backend count")
LB_BACKENDS.set(len(_initial_backends))

# ── App ───────────────────────────────────────────────────────────

_SKIP_HEADERS = {"host", "transfer-encoding", "content-encoding", "content-length"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        app.state.http = client
        yield


app = FastAPI(title="Beacon Load Balancer", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


# ── Admin endpoints ───────────────────────────────────────────────

class BackendPayload(BaseModel):
    url: str


@app.get("/health")
def health():
    return {"status": "ok", "backends": rr.backends}


@app.get("/admin/backends")
def list_backends():
    return {"backends": rr.backends}


@app.post("/admin/backends", status_code=201)
async def add_backend(payload: BackendPayload):
    await rr.add(payload.url)
    LB_BACKENDS.set(len(rr.backends))
    return {"backends": rr.backends}


@app.delete("/admin/backends")
async def remove_backend(payload: BackendPayload):
    await rr.remove(payload.url)
    LB_BACKENDS.set(len(rr.backends))
    return {"backends": rr.backends}


# ── Catch-all proxy ───────────────────────────────────────────────

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    try:
        backend = await rr.next()
    except RuntimeError:
        return JSONResponse({"error": "no backends available"}, status_code=503)

    url = f"{backend}/{path}"
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _SKIP_HEADERS}

    with LB_LATENCY.labels(backend=backend).time():
        try:
            resp = await request.app.state.http.request(
                method=request.method,
                url=url,
                headers=headers,
                content=await request.body(),
                params=dict(request.query_params),
                follow_redirects=False,
            )
            LB_REQUESTS.labels(backend=backend, status_code=str(resp.status_code)).inc()
            out_headers = {
                k: v for k, v in resp.headers.items()
                if k.lower() not in _SKIP_HEADERS
            }
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=out_headers,
            )
        except httpx.RequestError as exc:
            LB_ERRORS.labels(backend=backend).inc()
            return JSONResponse(
                {"error": "upstream unavailable", "backend": backend, "detail": str(exc)},
                status_code=502,
            )
