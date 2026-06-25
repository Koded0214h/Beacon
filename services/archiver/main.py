"""
Archiver — Consumer Group B

Reads from Kafka, batches records, bulk-inserts into SQLite.
Commits Kafka offsets only after a successful DB write so no event is lost.

Run from project root:
    PYTHONPATH=. python services/archiver/main.py
"""
import os
import sys
import time
import signal
from datetime import datetime, timezone

import sqlalchemy as sa
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server

from shared.schemas.state import AgentStatus

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "agent-state-transitions"
GROUP_ID        = "beacon-archiver-group"
BATCH_SIZE      = 500
FLUSH_INTERVAL  = 2.0   # seconds
METRICS_PORT    = int(os.environ.get("ARCHIVER_METRICS_PORT", "8002"))

engine = sa.create_engine("sqlite:///beacon.db")
meta   = sa.MetaData()

agent_events = sa.Table(
    "agent_events", meta,
    sa.Column("id",             sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("agent_id",       sa.String,  nullable=False, index=True),
    sa.Column("status",         sa.String,  nullable=False),
    sa.Column("timestamp",      sa.DateTime(timezone=True), nullable=False),
    sa.Column("event_metadata", sa.JSON,    nullable=True),
    sa.Column("ingested_at",    sa.DateTime, nullable=False),
)

EVENTS_FLUSHED = Counter(
    "archiver_events_flushed_total",
    "Total events successfully written to DB",
)
FLUSH_DURATION = Histogram(
    "archiver_flush_duration_seconds",
    "Time to execute a bulk DB insert",
)
BUFFER_SIZE = Gauge(
    "archiver_buffer_size",
    "Current in-memory buffer length",
)


def setup_db() -> None:
    meta.create_all(engine)
    print("[archiver] DB schema ready → beacon.db")


def run() -> None:
    setup_db()
    start_http_server(METRICS_PORT)
    print(f"[archiver] Prometheus metrics on :{METRICS_PORT}")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    buffer: list[dict] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if not buffer:
            return
        now = datetime.now(timezone.utc)
        rows = [{**row, "ingested_at": now} for row in buffer]
        t0 = time.perf_counter()
        with engine.begin() as conn:
            conn.execute(agent_events.insert(), rows)
        FLUSH_DURATION.observe(time.perf_counter() - t0)
        consumer.commit(asynchronous=False)
        EVENTS_FLUSHED.inc(len(buffer))
        print(f"[archiver] Flushed {len(buffer)} rows to DB")
        buffer.clear()
        BUFFER_SIZE.set(0)
        last_flush = time.monotonic()

    def on_shutdown(_sig, _frame) -> None:
        print("\n[archiver] Shutting down, flushing buffer...")
        flush()
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT,  on_shutdown)
    signal.signal(signal.SIGTERM, on_shutdown)

    print(
        f"[archiver] Listening on '{TOPIC}' | "
        f"batch={BATCH_SIZE} | interval={FLUSH_INTERVAL}s"
    )

    while True:
        msg = consumer.poll(1.0)

        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[archiver] Kafka error: {msg.error()}")
            else:
                try:
                    state = AgentStatus.model_validate_json(msg.value().decode())
                    buffer.append({
                        "agent_id":       state.agent_id,
                        "status":         state.status,
                        "timestamp":      state.timestamp,
                        "event_metadata": state.metadata,
                    })
                    BUFFER_SIZE.set(len(buffer))
                except Exception as exc:
                    print(f"[archiver] Parse error: {exc}")

        now = time.monotonic()
        if len(buffer) >= BATCH_SIZE or (buffer and now - last_flush >= FLUSH_INTERVAL):
            flush()


if __name__ == "__main__":
    run()
