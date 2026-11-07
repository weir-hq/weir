"""weir-scraper path/to/scraper.toml: measure each edge on its interval and send the result."""

from __future__ import annotations

import logging
import sys
import time

from weir import export
from weir import semconv as sc
from weir.runtime import resource

from weir_scraper.config import Edge, load
from weir_scraper.sources import Waiting

log = logging.getLogger("weir_scraper")


def edge_points(pipeline: str, edge: Edge, waiting: Waiting, now: float) -> list[dict]:
    attributes = {
        sc.WEIR_PIPELINE_NAME: pipeline,
        sc.WEIR_STAGE_NAME: edge.stage,
        sc.WEIR_UPSTREAM_STAGE_NAME: edge.upstream,
    }
    points = []
    if waiting.items is not None:
        points.append({"name": sc.WEIR_ITEM_WAITING, "attributes": attributes, "value": waiting.items})
    if waiting.items != 0 and waiting.oldest_event_time is not None:
        age = max(0.0, now - waiting.oldest_event_time)
        points.append({"name": sc.WEIR_ITEM_OLDEST_WAITING_AGE, "attributes": attributes, "value": age})
    return points


def scrape(pipeline: str, edges: list[Edge]) -> dict:
    points = []
    for edge in edges:
        try:
            waiting = edge.source.measure()
        except Exception as e:  # noqa: BLE001 -- skip the edge, rather than report a wrong number
            log.warning("edge %s -> %s: %s", edge.upstream, edge.stage, e)
            continue
        points += edge_points(pipeline, edge, waiting, time.time())
    return {"resource": resource(), "time_unix_nano": time.time_ns(), "metrics": points}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if len(sys.argv) != 2:
        sys.exit("usage: weir-scraper path/to/scraper.toml")
    config = load(sys.argv[1])
    for edge in config.edges:
        log.info("watching %s -> %s every %gs", edge.upstream, edge.stage, edge.interval_seconds)

    next_due = [0.0] * len(config.edges)
    while True:
        now = time.monotonic()
        due = [i for i, at in enumerate(next_due) if at <= now]
        for i in due:
            next_due[i] = now + config.edges[i].interval_seconds
        payload = scrape(config.pipeline, [config.edges[i] for i in due])
        if payload["metrics"]:
            export.send(config.endpoint, payload)
        time.sleep(max(0.0, min(next_due) - time.monotonic()))


if __name__ == "__main__":
    main()
