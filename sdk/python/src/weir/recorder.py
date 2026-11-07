"""In-memory counts per stage: running totals that never reset, and the items inside."""

from __future__ import annotations

import copy
import math
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from weir.stage import Item

# OpenTelemetry base-2 exponential histogram at a fixed scale: bucket i holds values in
# (base**i, base**(i+1)], base = 2**(2**-SCALE). Scale 3 makes buckets 9% apart, so quantiles
# are within ~4%, from microseconds to days.
SCALE = 3
ZERO_THRESHOLD = 1e-6  # seconds; anything smaller counts as zero


@dataclass
class Histogram:
    scale: int = SCALE
    zero_count: int = 0
    offset: int = 0  # index of bucket_counts[0]
    bucket_counts: list[int] = field(default_factory=list)
    sum: float = 0.0
    count: int = 0

    def observe(self, seconds: float) -> None:
        seconds = max(seconds, 0.0)  # clock skew between hosts
        self.sum += seconds
        self.count += 1
        if seconds <= ZERO_THRESHOLD:
            self.zero_count += 1
            return
        index = math.ceil(math.log2(seconds) * (1 << self.scale)) - 1
        if not self.bucket_counts:
            self.offset, self.bucket_counts = index, [0]
        elif index < self.offset:
            self.bucket_counts[:0] = [0] * (self.offset - index)
            self.offset = index
        elif index >= self.offset + len(self.bucket_counts):
            self.bucket_counts += [0] * (index - self.offset - len(self.bucket_counts) + 1)
        self.bucket_counts[index - self.offset] += 1


@dataclass
class StageCounts:
    received: Counter[str] = field(default_factory=Counter)  # by upstream stage
    originated: int = 0
    emitted: int = 0
    completed: Counter[tuple[str, str]] = field(default_factory=Counter)  # by (outcome, drop reason)
    retries: int = 0
    context_errors: int = 0
    entry_age: Histogram = field(default_factory=Histogram)
    exit_age: Histogram = field(default_factory=Histogram)
    process_duration: Histogram = field(default_factory=Histogram)


@dataclass
class StageSnapshot:
    counts: StageCounts
    active: int
    oldest_event_time: float | None


class Recorder:
    def __init__(self) -> None:
        self.started_at = time.time()  # the totals count from here
        self._lock = threading.Lock()
        self._counts: dict[str, StageCounts] = defaultdict(StageCounts)
        self._inside: dict[str, set[Item]] = defaultdict(set)

    def item_entered(self, item: Item) -> None:
        with self._lock:
            self._inside[item.stage_name].add(item)
            counts = self._counts[item.stage_name]
            if item.originated:
                counts.originated += 1
            else:
                counts.received[item.ctx.last_stage or "?"] += 1
            if item.ctx.invalid:
                counts.context_errors += 1
            else:
                counts.entry_age.observe(item.entered_at - item.ctx.event_time)

    def item_emitted(self, item: Item) -> None:
        with self._lock:
            self._counts[item.stage_name].emitted += 1

    def item_left(self, item: Item) -> None:
        assert item.outcome is not None and item.left_at is not None
        with self._lock:
            self._inside[item.stage_name].discard(item)
            counts = self._counts[item.stage_name]
            counts.completed[(item.outcome.value, item.drop_reason)] += 1
            if not item.ctx.invalid:
                counts.exit_age.observe(item.left_at - item.ctx.event_time)
            counts.process_duration.observe(item.left_at - item.entered_at)

    def stage_retried(self, stage_name: str, n: int) -> None:
        with self._lock:
            self._counts[stage_name].retries += n

    def take_snapshot(self) -> dict[str, StageSnapshot]:
        with self._lock:
            stages = set(self._counts) | {name for name, items in self._inside.items() if items}
            return {
                name: StageSnapshot(
                    counts=copy.deepcopy(self._counts.get(name, StageCounts())),
                    active=len(self._inside.get(name, ())),
                    oldest_event_time=min((i.ctx.event_time for i in self._inside.get(name, ())), default=None),
                )
                for name in sorted(stages)
            }


class NullRecorder(Recorder):
    """For WEIR_DISABLED: records nothing."""

    def item_entered(self, item: Item) -> None:
        pass

    def item_emitted(self, item: Item) -> None:
        pass

    def item_left(self, item: Item) -> None:
        pass

    def stage_retried(self, stage_name: str, n: int) -> None:
        pass

    def take_snapshot(self) -> dict[str, StageSnapshot]:
        return {}
