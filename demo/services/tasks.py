"""Celery worker: writes each batch into app.staged_lines.

Faults: POISON_KEY — an order_id that makes its whole batch fail and retry forever.
The retries are scheduled with a countdown, so the worker holds them in memory and
the Redis queue length stays at zero.
"""

import weir
from celery import Celery

from common import REDIS_URL, env_str, get_logger, pg_connect, record_injected

log = get_logger("worker")

POISON_KEY = env_str("POISON_KEY")

app = Celery("tasks", broker=REDIS_URL)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)

weir.init(pipeline="orders")
stage = weir.stage("worker")

_conn = None
# Items of batches being retried, by batch id. Per process: a retry in another process can't close them.
_open_items: dict[str, list[weir.Item]] = {}


def conn():
    global _conn
    if _conn is None:
        _conn = pg_connect()
    return _conn


@app.task(bind=True, max_retries=None)
def process_batch(self, batch_id: str, lines: list[dict]) -> int:
    if self.request.retries == 0:
        _open_items[batch_id] = [stage.receive(weir.Context.from_header(line.get("_weir"))) for line in lines]
    else:
        stage.retried(len(lines))

    if POISON_KEY and any(line["order_id"] == POISON_KEY for line in lines):
        if self.request.retries == 0:
            order_ids = {line["order_id"] for line in lines}
            record_injected(
                conn(),
                [
                    (order_id, "worker", "poison" if order_id == POISON_KEY else "stuck_behind_poison", batch_id)
                    for order_id in order_ids
                ],
            )
        raise self.retry(countdown=10, exc=ValueError(f"poison key {POISON_KEY} in batch {batch_id}"))

    with conn().cursor() as cur:
        cur.executemany(
            "INSERT INTO app.staged_lines (line_key, order_id, event_time, amount, batch_id)"
            " VALUES (%s, %s, to_timestamp(%s), %s, %s)",
            [(line["line_key"], line["order_id"], line["event_time"], line["amount"], batch_id) for line in lines],
        )
    for item in _open_items.pop(batch_id, []):
        item.emit()  # into app.staged_lines, picked up by aggregate
        item.close()
    return len(lines)
