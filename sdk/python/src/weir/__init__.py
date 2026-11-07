"""Weir SDK: see how items move through async pipelines.

weir.init(pipeline="orders")
enrich = weir.stage("enrich")

with enrich.receive(weir.extract(msg.headers())) as item:
    for line in explode(msg):
        producer.produce("lines", line, headers=weir.inject(item.emit()))
"""

from __future__ import annotations

from weir.context import HEADER, Context, extract, inject
from weir.runtime import runtime
from weir.semconv import Outcome
from weir.stage import Item, Stage

__all__ = ["HEADER", "Context", "Item", "Outcome", "Stage", "extract", "flush", "init", "inject", "stage"]


def init(pipeline: str, endpoint: str | None = None) -> None:
    """Start reporting. The endpoint defaults to $WEIR_ENDPOINT, then http://localhost:4100."""
    runtime.start(pipeline, endpoint)


def stage(name: str) -> Stage:
    return Stage(name)


def flush() -> None:
    runtime.flush()
