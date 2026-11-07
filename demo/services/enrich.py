"""Consumes orders, fans each one out into line items.

Faults:
  DROP_RATE           — fraction of orders silently dropped (no error, no log line).
  STALL_SECONDS       — stop consuming for this long, once ...
  STALL_AFTER_SECONDS — ... this many seconds after startup.
"""

import json
import random
import time

import weir
from confluent_kafka import Consumer, Producer

from common import (
    KAFKA_BOOTSTRAP,
    LINES_TOPIC,
    ORDERS_TOPIC,
    env_float,
    env_int,
    get_logger,
    pg_connect,
    record_injected,
)

log = get_logger("enrich")

DROP_RATE = env_float("DROP_RATE", 0)
STALL_SECONDS = env_int("STALL_SECONDS", 0)
STALL_AFTER_SECONDS = env_int("STALL_AFTER_SECONDS", 60)


def main() -> None:
    weir.init(pipeline="orders")
    stage = weir.stage("enrich")
    conn = pg_connect()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "enrich",
            "auto.offset.reset": "earliest",
            # Survive the injected stall without being kicked out of the group.
            "max.poll.interval.ms": max(300_000, (STALL_SECONDS + 60) * 1000),
        }
    )
    consumer.subscribe([ORDERS_TOPIC])
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 20})

    log.info("drop_rate=%s stall=%ss after %ss", DROP_RATE, STALL_SECONDS, STALL_AFTER_SECONDS)
    started = time.monotonic()
    stalled = False
    while True:
        if STALL_SECONDS and not stalled and time.monotonic() - started >= STALL_AFTER_SECONDS:
            stalled = True
            time.sleep(STALL_SECONDS)

        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("consumer error: %s", msg.error())
            continue

        order = json.loads(msg.value())

        with stage.receive(weir.extract(msg.headers())) as item:
            if DROP_RATE and random.random() < DROP_RATE:
                # The bug being simulated: an early exit with no output and no outcome.
                record_injected(conn, [(order["order_id"], "enrich", "drop", None)])
                continue

            for i in range(order["lines"]):
                line = {
                    "line_key": f"{order['order_id']}-{i}",
                    "order_id": order["order_id"],
                    "event_time": order["event_time"],
                    "amount": round(random.uniform(1, 100), 2),
                }
                headers = weir.inject(item.emit())
                producer.produce(LINES_TOPIC, key=order["order_id"], value=json.dumps(line), headers=headers)
        producer.poll(0)


if __name__ == "__main__":
    main()
