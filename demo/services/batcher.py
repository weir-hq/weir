"""Consumes line items and groups them into batches (fan-in) for the Celery worker.

A batch is flushed at BATCH_SIZE items or BATCH_MAX_WAIT_SECONDS after its first item.
Offsets are committed only after the batch is handed to Celery.
"""

import json
import time
import uuid

import weir
from celery import Celery
from confluent_kafka import Consumer

from common import KAFKA_BOOTSTRAP, LINES_TOPIC, REDIS_URL, env_int, get_logger

log = get_logger("batcher")

BATCH_SIZE = env_int("BATCH_SIZE", 500)
BATCH_MAX_WAIT_SECONDS = env_int("BATCH_MAX_WAIT_SECONDS", 30)


def main() -> None:
    weir.init(pipeline="orders")
    stage = weir.stage("batcher")
    celery = Celery(broker=REDIS_URL)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "batcher",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([LINES_TOPIC])

    log.info("batch_size=%d max_wait=%ds", BATCH_SIZE, BATCH_MAX_WAIT_SECONDS)
    buffer: list[tuple[dict, weir.Item]] = []
    first_at = 0.0
    while True:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                log.warning("consumer error: %s", msg.error())
            else:
                if not buffer:
                    first_at = time.monotonic()
                buffer.append((json.loads(msg.value()), stage.receive(weir.extract(msg.headers()))))

        if buffer and (len(buffer) >= BATCH_SIZE or time.monotonic() - first_at >= BATCH_MAX_WAIT_SECONDS):
            batch_id = uuid.uuid4().hex
            lines = []
            for line, item in buffer:
                line["_weir"] = item.emit().header()
                item.close()
                lines.append(line)
            celery.send_task("tasks.process_batch", args=[batch_id, lines])
            consumer.commit(asynchronous=False)
            log.info("flushed batch %s with %d lines", batch_id, len(buffer))
            buffer = []


if __name__ == "__main__":
    main()
