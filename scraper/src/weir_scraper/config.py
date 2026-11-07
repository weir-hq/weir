"""Scraper config (TOML). See demo/scraper.toml for an example.

pipeline = "orders"
endpoint = "..."            # optional: $WEIR_ENDPOINT, then http://localhost:4100
interval_seconds = 5        # optional, also per edge; at most 60
[kafka]    bootstrap = "..."
[postgres] dsn = "..."; timeout_seconds = 1
[[edge]]   from = "producer"; to = "enrich"; kafka = { topic = "orders", group = "enrich" }
[[edge]]   from = "worker"; to = "aggregate"; sql = "SELECT ..."
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from weir_scraper.sources import KafkaTopic, SqlQuery, Waiting

# The server treats reports older than a minute as stale.
MAX_INTERVAL_SECONDS = 60


class Source(Protocol):
    def measure(self) -> Waiting: ...


@dataclass
class Edge:
    upstream: str
    stage: str
    source: Source
    interval_seconds: float = 5.0


@dataclass
class Config:
    pipeline: str
    endpoint: str
    edges: list[Edge]


def load(path: str | Path) -> Config:
    raw = tomllib.loads(Path(path).read_text())
    default_interval = float(raw.get("interval_seconds", 5))
    edges = []
    for e in raw.get("edge", []):
        interval = float(e.get("interval_seconds", default_interval))
        if not 0 < interval <= MAX_INTERVAL_SECONDS:
            raise ValueError(f"edge {e['from']} -> {e['to']}: interval_seconds must be between 0 and 60")
        edges.append(Edge(e["from"], e["to"], _source(e, raw), interval))
    if not edges:
        raise ValueError(f"{path}: no [[edge]] to watch")
    return Config(
        pipeline=raw["pipeline"],
        endpoint=raw.get("endpoint") or os.environ.get("WEIR_ENDPOINT") or "http://localhost:4100",
        edges=edges,
    )


def _source(edge: dict, raw: dict) -> Source:
    name = f"edge {edge['from']} -> {edge['to']}"
    if "kafka" in edge:
        return KafkaTopic(raw["kafka"]["bootstrap"], edge["kafka"]["topic"], edge["kafka"]["group"])
    if "sql" in edge:
        postgres = raw["postgres"]
        return SqlQuery(postgres["dsn"], edge["sql"], float(postgres.get("timeout_seconds", 1)))
    raise ValueError(f'{name}: needs `kafka = {{ topic, group }}` or `sql = "..."`')
