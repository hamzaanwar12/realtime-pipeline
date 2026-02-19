"""Entrypoints: `produce` (fake events -> Kafka) and `consume` (Kafka -> DuckDB, commit offsets after write)."""
import json
import os
import sys
import time
from confluent_kafka import Consumer, Producer
from .events import make_event, parse
from .sink import Sink

BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = os.getenv("TOPIC", "clickstream")


def produce(rate: float = 50.0):
    p = Producer({"bootstrap.servers": BROKER, "enable.idempotence": True})
    while True:
        e = make_event()
        p.produce(TOPIC, key=e["user_id"], value=json.dumps(e))
        p.poll(0)
        time.sleep(1 / rate)


def consume(db: str = os.getenv("DUCKDB_PATH", "warehouse.duckdb"), batch: int = 500):
    sink = Sink(db)
    sink.build_models()
    c = Consumer({"bootstrap.servers": BROKER, "group.id": "duckdb-sink", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    try:
        while True:
            msgs = c.consume(batch, timeout=1.0)
            good, bad = [], []
            for m in msgs:
                if m.error():
                    continue
                (good.append(e) if (e := parse(m.value())) else bad.append(m.value()))
            if msgs:
                sink.write(good, bad)
                c.commit(asynchronous=False)  # offsets only after the DB commit: at-least-once + dedupe = effectively once
                print(f"wrote {len(good)} events, {len(bad)} dead-lettered", flush=True)
    finally:
        c.close()


if __name__ == "__main__":
    {"produce": produce, "consume": consume}[sys.argv[1]]()
