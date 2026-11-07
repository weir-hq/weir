"""Generates orders into Kafka and records every one in truth.produced.

Faults: DUPLICATE_RATE — fraction of orders sent twice.
"""

import json
import random
import time

import weir
from confluent_kafka import Producer

from common import KAFKA_BOOTSTRAP, ORDERS_TOPIC, env_float, env_int, get_logger, pg_connect, record_injected

log = get_logger("producer")

ORDERS_PER_SECOND = env_int("ORDERS_PER_SECOND", 20)
MAX_LINES_PER_ORDER = env_int("MAX_LINES_PER_ORDER", 5)
DUPLICATE_RATE = env_float("DUPLICATE_RATE", 0)


def main() -> None:
    weir.init(pipeline="orders")
    stage = weir.stage("producer")
    conn = pg_connect()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 20})

    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(seq), 0) FROM truth.produced")
        seq = cur.fetchone()[0]

    log.info("producing %d orders/s, duplicate_rate=%s", ORDERS_PER_SECOND, DUPLICATE_RATE)
    while True:
        tick = time.monotonic()

        orders = []
        for _ in range(ORDERS_PER_SECOND):
            seq += 1
            orders.append(
                {
                    "seq": seq,
                    "order_id": f"o-{seq:09d}",
                    "lines": random.randint(1, MAX_LINES_PER_ORDER),
                    "event_time": time.time(),
                }
            )

        # Truth first: an order counts as produced once we intend to send it.
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO truth.produced (seq, order_id, n_lines, event_time) VALUES (%s, %s, %s, to_timestamp(%s))",
                [(o["seq"], o["order_id"], o["lines"], o["event_time"]) for o in orders],
            )

        duplicates = []
        for order in orders:
            value = json.dumps(order)
            with stage.receive(None, key=order["order_id"], event_time=order["event_time"]) as item:
                headers = weir.inject(item.emit())
                producer.produce(ORDERS_TOPIC, key=order["order_id"], value=value, headers=headers)
                if DUPLICATE_RATE and random.random() < DUPLICATE_RATE:
                    headers = weir.inject(item.emit())
                    producer.produce(ORDERS_TOPIC, key=order["order_id"], value=value, headers=headers)
                    duplicates.append((order["order_id"], "producer", "duplicate", None))
        producer.flush()

        if duplicates:
            record_injected(conn, duplicates)

        time.sleep(max(0.0, 1.0 - (time.monotonic() - tick)))


if __name__ == "__main__":
    main()
