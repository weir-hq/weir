import logging
import os
import time

import psycopg

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://weir:weir@postgres:5432/weir")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

ORDERS_TOPIC = "orders"
LINES_TOPIC = "lines"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    return logging.getLogger(name)


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name) or default)


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name) or default)


def env_str(name: str, default: str = "") -> str:
    return os.environ.get(name) or default


def pg_connect() -> psycopg.Connection:
    for _ in range(30):
        try:
            return psycopg.connect(PG_DSN, autocommit=True)
        except psycopg.OperationalError:
            time.sleep(2)
    return psycopg.connect(PG_DSN, autocommit=True)


def record_injected(conn: psycopg.Connection, rows: list[tuple[str, str, str, str | None]]) -> None:
    """Ground truth: (order_id, stage, fault, detail) for every fault the demo injects."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO truth.injected (key, stage, fault, detail) VALUES (%s, %s, %s, %s)",
            rows,
        )
