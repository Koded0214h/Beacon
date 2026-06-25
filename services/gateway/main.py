"""
Ingestion Gateway

Accepts agent state-change events via HTTP and produces them to Kafka.
Returns 202 immediately — never waits on the database.

Run from project root:
    PYTHONPATH=. uvicorn services.gateway.main:app --port 8010
"""
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, make_asgi_app

from shared.schemas.state import AgentStatus

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "agent-state-transitions"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

INGEST_TOTAL = Counter(
    "gateway_ingest_requests_total",
    "Total ingest requests by outcome",
    ["outcome"],
)
INGEST_LATENCY = Histogram(
    "gateway_ingest_duration_seconds",
    "Time to produce a message to Kafka",
)


def _on_delivery(err, msg):
    if err:
        INGEST_TOTAL.labels(outcome="kafka_error").inc()
        print(f"[gateway] Delivery failed for {msg.key()}: {err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    producer.flush()


app = FastAPI(title="Beacon Ingestion Gateway", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest/state", status_code=202)
async def ingest_agent_state(data: AgentStatus):
    """
    Accept a state-change from an AI agent and fire-and-forget into Kafka.
    The agent_id is the partition key — all events for one agent stay in order.
    """
    t0 = time.perf_counter()
    producer.produce(
        topic=TOPIC,
        key=data.agent_id.encode(),
        value=data.model_dump_json().encode(),
        callback=_on_delivery,
    )
    producer.poll(0)
    INGEST_LATENCY.observe(time.perf_counter() - t0)
    INGEST_TOTAL.labels(outcome="accepted").inc()
    return {"status": "accepted"}
