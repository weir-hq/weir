"""Turns a snapshot into the /ingest JSON and sends it.

Counters and histograms are cumulative since start_time_unix_nano; the server subtracts the
previous report, so a failed export loses nothing. Zero counts are left out.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import urllib.request

from weir import semconv as sc
from weir.recorder import Histogram, StageSnapshot

log = logging.getLogger("weir")


def build_payload(
    pipeline: str, resource: dict[str, str], snapshot: dict[str, StageSnapshot], now: float, started_at: float
) -> dict:
    metrics = []
    for stage_name, stage in snapshot.items():
        metrics += stage_points(pipeline, stage_name, stage, now)
    return {
        "resource": resource,
        "time_unix_nano": int(now * 1e9),
        "start_time_unix_nano": int(started_at * 1e9),
        "temporality": "cumulative",
        "metrics": metrics,
    }


def stage_points(pipeline: str, stage_name: str, stage: StageSnapshot, now: float) -> list[dict]:
    base = {sc.WEIR_PIPELINE_NAME: pipeline, sc.WEIR_STAGE_NAME: stage_name}
    counts = stage.counts
    points = []

    for upstream, n in counts.received.items():
        points.append(_value(sc.WEIR_STAGE_RECEIVED_ITEMS, {**base, sc.WEIR_UPSTREAM_STAGE_NAME: upstream}, n))
    if counts.originated:
        points.append(_value(sc.WEIR_STAGE_ORIGINATED_ITEMS, base, counts.originated))
    if counts.emitted:
        points.append(_value(sc.WEIR_STAGE_EMITTED_ITEMS, base, counts.emitted))
    for (outcome, drop_reason), n in counts.completed.items():
        attributes = {**base, sc.WEIR_ITEM_OUTCOME: outcome}
        if outcome == sc.Outcome.DROPPED:
            attributes[sc.WEIR_DROP_REASON] = drop_reason
        points.append(_value(sc.WEIR_STAGE_COMPLETED_ITEMS, attributes, n))
    if counts.retries:
        points.append(_value(sc.WEIR_STAGE_RETRIES, base, counts.retries))
    if counts.context_errors:
        points.append(_value(sc.WEIR_STAGE_CONTEXT_ERRORS, base, counts.context_errors))

    entry = {**base, sc.WEIR_STAGE_BOUNDARY: sc.WEIR_STAGE_BOUNDARY_ENTRY}
    exit = {**base, sc.WEIR_STAGE_BOUNDARY: sc.WEIR_STAGE_BOUNDARY_EXIT}
    if counts.entry_age.count:
        points.append(_histogram(sc.WEIR_ITEM_AGE, entry, counts.entry_age))
    if counts.exit_age.count:
        points.append(_histogram(sc.WEIR_ITEM_AGE, exit, counts.exit_age))
    if counts.process_duration.count:
        points.append(_histogram(sc.WEIR_STAGE_PROCESS_DURATION, base, counts.process_duration))

    points.append(_value(sc.WEIR_ITEM_ACTIVE, base, stage.active))
    if stage.oldest_event_time is not None:
        points.append(_value(sc.WEIR_ITEM_OLDEST_AGE, base, now - stage.oldest_event_time))
    return points


def send(endpoint: str, payload: dict) -> None:
    """Never raises: telemetry must not break the app."""
    request = urllib.request.Request(
        endpoint + "/ingest", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(request, timeout=5).close()
    except OSError as e:
        log.warning("weir: export failed: %s", e)


def _value(name: str, attributes: dict[str, str], value: float) -> dict:
    return {"name": name, "attributes": attributes, "value": value}


def _histogram(name: str, attributes: dict[str, str], histogram: Histogram) -> dict:
    return {"name": name, "attributes": attributes, "histogram": dataclasses.asdict(histogram)}
