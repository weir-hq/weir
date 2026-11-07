"""Cron-style job: on wall-clock aligned boundaries, moves staged lines into the sink.

Runs at every multiple of CRON_PERIOD_SECONDS, shifted by CRON_OFFSET_SECONDS.
The sink's primary key silently absorbs duplicates.
"""

import math
import time

import weir

from common import env_int, get_logger, pg_connect

log = get_logger("aggregate")

CRON_PERIOD_SECONDS = env_int("CRON_PERIOD_SECONDS", 300)
CRON_OFFSET_SECONDS = env_int("CRON_OFFSET_SECONDS", 0)

MOVE_SQL = """
WITH moved AS (
    DELETE FROM app.staged_lines
    RETURNING line_key, order_id, event_time, amount, batch_id, staged_at
), inserted AS (
    INSERT INTO app.line_items (line_key, order_id, event_time, amount, batch_id, staged_at)
    SELECT DISTINCT ON (line_key) line_key, order_id, event_time, amount, batch_id, staged_at
    FROM moved
    ORDER BY line_key, staged_at
    ON CONFLICT (line_key) DO NOTHING
    RETURNING line_key
)
SELECT m.line_key, m.order_id, extract(epoch FROM m.event_time)::float8,
       m.line_key IN (SELECT line_key FROM inserted)
FROM moved m
"""


def next_run(now: float) -> float:
    return (
        math.floor((now - CRON_OFFSET_SECONDS) / CRON_PERIOD_SECONDS) + 1
    ) * CRON_PERIOD_SECONDS + CRON_OFFSET_SECONDS


def sleep_until(t: float) -> None:
    # Short steps: time.sleep doesn't count time the host spends asleep.
    while (left := t - time.time()) > 0:
        time.sleep(min(left, 1.0))


def main() -> None:
    weir.init(pipeline="orders")
    stage = weir.stage("aggregate")
    conn = pg_connect()
    log.info("period=%ds offset=%ds", CRON_PERIOD_SECONDS, CRON_OFFSET_SECONDS)
    while True:
        sleep_until(next_run(time.time()))
        with conn.cursor() as cur:
            cur.execute(MOVE_SQL)
            rows = cur.fetchall()

        # Staged rows carry no headers: rebuild the context from the row itself.
        inserted = set()
        for line_key, order_id, event_time, was_inserted in rows:
            with stage.receive(
                weir.Context(pipeline="orders", key=order_id, event_time=event_time, last_stage="worker")
            ) as item:
                if was_inserted and line_key not in inserted:
                    inserted.add(line_key)
                    item.done()
                else:
                    item.drop("duplicate")
        log.info("moved %d staged lines, inserted %d into sink", len(rows), len(inserted))


if __name__ == "__main__":
    main()
