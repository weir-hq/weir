"""Where items wait between stages: how many, and the oldest one's birth time."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import psycopg
import weir
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer, TopicPartition


@dataclass
class Waiting:
    items: int | None  # None: not counted
    oldest_event_time: float | None


class KafkaTopic:
    """Messages after the group's committed offsets. Includes read-but-uncommitted ones
    (e.g. a batcher's buffer): a slight overcount while healthy, exact while stopped."""

    def __init__(self, bootstrap: str, topic: str, group: str) -> None:
        self.topic = topic
        self.group = group
        # Never subscribes, so it reads the group's offsets without joining it.
        self._offsets = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
        self._peek = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"weir-scraper-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
            }
        )

    def measure(self) -> Waiting:
        metadata = self._offsets.list_topics(self.topic, timeout=5).topics[self.topic]
        partitions = [TopicPartition(self.topic, p) for p in metadata.partitions]
        committed = self._offsets.committed(partitions, timeout=5)

        items = 0
        first_waiting = {}  # partition -> offset
        for tp in committed:
            low, high = self._offsets.get_watermark_offsets(TopicPartition(self.topic, tp.partition), timeout=5)
            # No commit yet: assumes auto.offset.reset=earliest.
            start = max(tp.offset, low) if tp.offset >= 0 else low
            if high > start:
                items += high - start
                first_waiting[tp.partition] = start

        return Waiting(items, self._oldest_birth(first_waiting))

    def _oldest_birth(self, first_waiting: dict[int, int]) -> float | None:
        if not first_waiting:
            return None
        self._peek.assign([TopicPartition(self.topic, p, offset) for p, offset in first_waiting.items()])
        oldest = None
        pending = set(first_waiting)
        deadline = time.monotonic() + 3
        while pending and time.monotonic() < deadline:
            msg = self._peek.poll(0.5)
            if msg is None or msg.error() or msg.partition() not in pending:
                continue
            pending.discard(msg.partition())
            born = _birth_time(msg)
            if born is not None and (oldest is None or born < oldest):
                oldest = born
        self._peek.unassign()
        return oldest


def _birth_time(msg) -> float | None:
    ctx = weir.extract(msg.headers())
    if ctx is not None and not ctx.invalid:
        return ctx.event_time
    kind, timestamp_ms = msg.timestamp()  # no context: produce time, a lower bound
    return timestamp_ms / 1000 if kind != TIMESTAMP_NOT_AVAILABLE else None


class SqlQuery:
    """Rows waiting in a table. The query returns the oldest birth time (unix seconds) or NULL,
    e.g. `SELECT extract(epoch FROM min(event_time)) FROM staging`. No count(*): it scans the
    whole table. Index the column so min() reads one entry."""

    def __init__(self, dsn: str, query: str, timeout_seconds: float = 1.0) -> None:
        self.dsn = dsn
        self.query = query
        self.timeout_seconds = timeout_seconds
        self._conn: psycopg.Connection | None = None

    def measure(self) -> Waiting:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
            self._conn.execute(f"SET statement_timeout = {int(self.timeout_seconds * 1000)}")
        try:
            row = self._conn.execute(self.query).fetchone()
        except psycopg.Error:
            self._conn.close()
            raise
        oldest = row[0] if row else None
        if oldest is None:
            return Waiting(items=0, oldest_event_time=None)
        return Waiting(items=None, oldest_event_time=float(oldest))
