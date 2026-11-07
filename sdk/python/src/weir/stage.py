"""Stage and Item: what pipeline code calls."""

from __future__ import annotations

import dataclasses
import time
from types import TracebackType
from typing import TYPE_CHECKING

from weir.context import Context
from weir.runtime import runtime
from weir.semconv import Outcome

if TYPE_CHECKING:
    from typing_extensions import Self


class Item:
    """An item inside a stage. It leaves with the outcome set, else ERRORED on an exception,
    else PASSED if it emitted and VANISHED if not. An item never closed counts as stuck."""

    def __init__(self, stage_name: str, ctx: Context, originated: bool) -> None:
        self.stage_name = stage_name
        self.ctx = ctx
        self.originated = originated
        self.entered_at = time.time()
        self.left_at: float | None = None
        self.emitted = 0
        self.outcome: Outcome | None = None
        self.drop_reason = ""
        runtime.recorder.item_entered(self)

    def emit(self, key: str | None = None) -> Context:
        """Record one output; returns the context to send with it."""
        self.emitted += 1
        runtime.recorder.item_emitted(self)
        return dataclasses.replace(self.ctx, key=key or self.ctx.key, last_stage=self.stage_name, invalid=False)

    def done(self) -> None:
        self._set_outcome(Outcome.DONE)

    def drop(self, reason: str) -> None:
        self._set_outcome(Outcome.DROPPED, reason)

    def error(self) -> None:
        self._set_outcome(Outcome.ERRORED)

    def close(self) -> None:
        if self.left_at is not None:
            return
        self.left_at = time.time()
        if self.outcome is None:
            self.outcome = Outcome.PASSED if self.emitted else Outcome.VANISHED
        runtime.recorder.item_left(self)

    def _set_outcome(self, outcome: Outcome, drop_reason: str = "") -> None:
        self.outcome = outcome
        self.drop_reason = drop_reason

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None and self.outcome is None:
            self._set_outcome(Outcome.ERRORED)
        self.close()


class Stage:
    def __init__(self, name: str) -> None:
        self.name = name

    def receive(self, ctx: Context | None = None, *, key: str = "", event_time: float | None = None) -> Item:
        """Start tracking an item. Without a context, the item is born in this stage."""
        if ctx is not None and ctx.invalid:
            # Unknown upstream and birth time: counted as received from "?", aged from now.
            ctx = dataclasses.replace(ctx, pipeline=runtime.pipeline, event_time=time.time())
        if ctx is not None:
            return Item(self.name, ctx, originated=False)
        born = Context(pipeline=runtime.pipeline, key=key, event_time=event_time or time.time())
        return Item(self.name, born, originated=True)

    def retried(self, n: int = 1) -> None:
        runtime.recorder.stage_retried(self.name, n)
