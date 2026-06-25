"""
Broadcaster — WebSocket Fan-Out

Consumer Group A: subscribes to Kafka with auto.offset.reset=latest
so it only streams live events, never replays history.

Bridges the sync Kafka consumer thread to async WebSocket clients via
an asyncio.Queue.

Run from project root:
    PYTHONPATH=. uvicorn services.broadcaster.main:app --port 8001
"""
import asyncio
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, make_asgi_app

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "agent-state-transitions"
GROUP_ID = "beacon-broadcaster-group"

clients: set[WebSocket] = set()
_queue: asyncio.Queue[str] = asyncio.Queue()

MESSAGES_BROADCAST = Counter(
    "broadcaster_messages_total",
    "Messages fanned out to WebSocket clients",
)
CONNECTED_CLIENTS = Gauge(
    "broadcaster_connected_clients",
    "Number of currently connected WebSocket clients",
)


def _kafka_thread(loop: asyncio.AbstractEventLoop) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    print(f"[broadcaster] Kafka consumer started on '{TOPIC}'")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[broadcaster] Kafka error: {msg.error()}")
                continue
            asyncio.run_coroutine_threadsafe(
                _queue.put(msg.value().decode("utf-8")), loop
            )
    finally:
        consumer.close()


async def _broadcast_loop() -> None:
    while True:
        message = await _queue.get()
        if not clients:
            continue
        dead: set[WebSocket] = set()
        for ws in list(clients):
            try:
                await ws.send_text(message)
                MESSAGES_BROADCAST.inc()
            except Exception:
                dead.add(ws)
        if dead:
            clients.difference_update(dead)
            CONNECTED_CLIENTS.set(len(clients))


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=_kafka_thread, args=(loop,), daemon=True)
    thread.start()
    task = asyncio.create_task(_broadcast_loop())
    yield
    task.cancel()


app = FastAPI(title="Beacon Broadcaster", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "clients": len(clients)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    CONNECTED_CLIENTS.set(len(clients))
    print(f"[broadcaster] Client connected  (total: {len(clients)})")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
        CONNECTED_CLIENTS.set(len(clients))
        print(f"[broadcaster] Client disconnected (total: {len(clients)})")
