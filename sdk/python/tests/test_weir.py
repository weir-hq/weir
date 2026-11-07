"""Each test reads as one rule of the SDK. Points are checked as the server would receive them."""

import json
import re
import time
from pathlib import Path

import pytest

import weir
from weir import export
from weir import semconv as sc
from weir.recorder import NullRecorder, Recorder
from weir.runtime import Runtime, resource, runtime

CONVENTIONS = Path(__file__).resolve().parents[3] / "conventions"


@pytest.fixture(autouse=True)
def fresh_recorder():
    runtime.recorder = Recorder()
    runtime.pipeline = "orders"


def exported(stage: str | None = None) -> list[dict]:
    """The metric points the next export would send, optionally for one stage."""
    payload = export.build_payload(
        "orders", resource(), runtime.recorder.take_snapshot(), time.time(), runtime.recorder.started_at
    )
    return [p for p in payload["metrics"] if stage is None or p["attributes"][sc.WEIR_STAGE_NAME] == stage]


def total(points: list[dict], name: str, attributes: dict[str, str] | None = None) -> float:
    """Sum of the values of `name` points whose attributes include `attributes`."""
    wanted = (attributes or {}).items()
    return sum(p["value"] for p in points if p["name"] == name and wanted <= p["attributes"].items())


def outcomes(points: list[dict]) -> dict[tuple[str, str | None], int]:
    """(outcome, drop reason) -> count, from the completed-items points."""
    return {
        (p["attributes"][sc.WEIR_ITEM_OUTCOME], p["attributes"].get(sc.WEIR_DROP_REASON)): p["value"]
        for p in points
        if p["name"] == sc.WEIR_STAGE_COMPLETED_ITEMS
    }


# Propagation


def test_context_survives_the_header_round_trip():
    ctx = weir.Context(pipeline="orders", key="o-1", event_time=1700000000.5, sampled=True, last_stage="enrich")
    assert weir.Context.from_header(ctx.header()) == ctx
    assert weir.Context.from_header(ctx.header().encode()) == ctx
    assert weir.extract(weir.inject(ctx)) == ctx


def test_keys_with_separators_survive_the_header():
    ctx = weir.Context(pipeline="orders", key="o;1 ü/%", event_time=1.0, last_stage="enrich")
    assert weir.Context.from_header(ctx.header()) == ctx


def test_header_is_versioned_and_reads_newer_versions():
    assert weir.Context(pipeline="p", key="k", event_time=1.0).header().startswith("00;")
    newer = weir.Context.from_header("01;orders;k;1000;01;enrich;a-field-from-the-future")
    assert newer == weir.Context(pipeline="orders", key="k", event_time=1.0, sampled=True, last_stage="enrich")


def test_missing_header_gives_no_context():
    assert weir.extract({"other": "x"}) is None
    assert weir.extract(None) is None


def test_malformed_header_gives_an_invalid_context():
    for bad in ["not-a-header", "orders;o-1;1000;0;enrich", "ff;a;b;1;00;c", "00;a;b;x;00;c", "00;a;b;1;00;c;extra"]:
        assert weir.Context.from_header(bad) is None, bad
        assert weir.extract({weir.HEADER: bad}).invalid, bad


def test_item_with_unreadable_context_is_counted_not_born():
    with weir.stage("enrich").receive(weir.extract({weir.HEADER: "garbage"})) as item:
        child = item.emit()
    points = exported("enrich")
    assert total(points, sc.WEIR_STAGE_RECEIVED_ITEMS, {sc.WEIR_UPSTREAM_STAGE_NAME: "?"}) == 1
    assert total(points, sc.WEIR_STAGE_ORIGINATED_ITEMS) == 0
    assert total(points, sc.WEIR_STAGE_CONTEXT_ERRORS) == 1
    assert not [p for p in points if p["name"] == sc.WEIR_ITEM_AGE]  # birth time unknown
    assert not child.invalid and weir.Context.from_header(child.header()) is not None


# Outcomes: every item leaves with exactly one


def test_item_that_emits_output_has_passed():
    with weir.stage("enrich").receive(key="a") as item:
        item.emit()
    assert outcomes(exported()) == {("passed", None): 1}


def test_item_that_emits_nothing_has_vanished():
    with weir.stage("enrich").receive(key="a"):
        pass
    assert outcomes(exported()) == {("vanished", None): 1}


def test_dropped_item_keeps_its_reason():
    with weir.stage("enrich").receive(key="a") as item:
        item.drop("duplicate")
    assert outcomes(exported()) == {("dropped", "duplicate"): 1}


def test_exception_in_with_block_marks_item_errored():
    with pytest.raises(ValueError), weir.stage("enrich").receive(key="a"):
        raise ValueError
    assert outcomes(exported()) == {("errored", None): 1}


def test_done_item_finished_the_pipeline():
    with weir.stage("sink").receive(key="a") as item:
        item.done()
    assert outcomes(exported()) == {("done", None): 1}


def test_last_outcome_set_wins():
    with weir.stage("enrich").receive(key="a") as item:
        item.drop("invalid")
        item.done()
    assert outcomes(exported()) == {("done", None): 1}


def test_item_never_closed_stays_active_and_ages():
    weir.stage("worker").receive(key="stuck", event_time=time.time() - 60)
    points = exported("worker")
    assert total(points, sc.WEIR_ITEM_ACTIVE) == 1
    assert outcomes(points) == {}
    oldest = [p["value"] for p in points if p["name"] == sc.WEIR_ITEM_OLDEST_AGE]
    assert oldest and oldest[0] >= 60

    # Still inside after the window resets.
    assert total(exported("worker"), sc.WEIR_ITEM_ACTIVE) == 1


# Conservation: in = out + inside


def test_every_item_in_is_out_or_inside():
    stage = weir.stage("enrich")
    for key in "abcde":
        with stage.receive(key=key) as item:
            item.emit()
    stage.receive(key="f")
    points = exported("enrich")
    items_in = total(points, sc.WEIR_STAGE_ORIGINATED_ITEMS) + total(points, sc.WEIR_STAGE_RECEIVED_ITEMS)
    items_out = sum(outcomes(points).values())
    assert items_in == items_out + total(points, sc.WEIR_ITEM_ACTIVE) == 6


# Cumulative: every export carries the totals so far, so a lost export loses nothing


def test_exports_carry_running_totals_not_per_window_counts():
    stage = weir.stage("enrich")
    for key in "ab":
        with stage.receive(key=key) as item:
            item.emit()
    first = total(exported("enrich"), sc.WEIR_STAGE_EMITTED_ITEMS)

    with stage.receive(key="c") as item:
        item.emit()
    second = total(exported("enrich"), sc.WEIR_STAGE_EMITTED_ITEMS)

    assert (first, second) == (2, 3)  # the second export repeats the first two: nothing to lose


def test_histograms_are_running_totals_too():
    stage = weir.stage("enrich")
    with stage.receive(key="a"):
        pass
    exported()
    with stage.receive(key="b"):
        pass
    ages = [p["histogram"] for p in exported("enrich") if p["name"] == sc.WEIR_STAGE_PROCESS_DURATION]
    assert ages[0]["count"] == 2


def test_histogram_buckets_are_exponential():
    from weir.recorder import Histogram

    h = Histogram()
    for seconds in (0.0, 1.0, 1.05, 2.0):
        h.observe(seconds)
    assert (h.zero_count, h.count) == (1, 4)
    assert h.offset == -1  # 1.0 is the upper edge of bucket -1: (2**(-1/8), 1]
    assert h.bucket_counts[0] == 1 and h.bucket_counts[1] == 1  # 1.05 is in (1, 2**(1/8)]
    assert sum(h.bucket_counts) == 3


# Between stages


def test_emitted_output_keeps_birth_time_and_names_its_stage():
    with weir.stage("producer").receive(key="o-1", event_time=123.0) as item:
        child = item.emit()
    assert (child.key, child.event_time, child.last_stage) == ("o-1", 123.0, "producer")


def test_receiving_stage_counts_its_upstream():
    with weir.stage("producer").receive(key="o-1") as item:
        child = item.emit()
    with weir.stage("enrich").receive(child) as item:
        item.emit()
    points = exported()
    assert (
        total(
            points,
            sc.WEIR_STAGE_RECEIVED_ITEMS,
            {sc.WEIR_STAGE_NAME: "enrich", sc.WEIR_UPSTREAM_STAGE_NAME: "producer"},
        )
        == 1
    )
    assert total(points, sc.WEIR_STAGE_EMITTED_ITEMS, {sc.WEIR_STAGE_NAME: "producer"}) == 1


# Kill switch


def test_weir_disabled_records_nothing_but_still_propagates(monkeypatch):
    monkeypatch.setenv("WEIR_DISABLED", "1")
    disabled = Runtime()
    assert isinstance(disabled.recorder, NullRecorder)
    disabled.start("orders")
    assert not disabled._started  # no exporter thread

    runtime.recorder = disabled.recorder
    with weir.stage("enrich").receive(key="a", event_time=123.0) as item:
        child = item.emit()
    assert child.event_time == 123.0 and child.last_stage == "enrich"
    assert exported() == []


# Wire format and names


def test_payload_matches_server_wire_format():
    with weir.stage("enrich").receive(key="a") as item:
        item.emit()
    payload = export.build_payload(
        "orders", resource(), runtime.recorder.take_snapshot(), time.time(), runtime.recorder.started_at
    )
    body = json.loads(json.dumps(payload))
    assert set(body) == {"resource", "time_unix_nano", "start_time_unix_nano", "temporality", "metrics"}
    assert body["temporality"] == "cumulative"
    assert body["start_time_unix_nano"] <= body["time_unix_nano"]
    assert set(body["resource"]) == {sc.SERVICE_NAME, sc.SERVICE_INSTANCE_ID}
    for point in body["metrics"]:
        assert {sc.WEIR_PIPELINE_NAME, sc.WEIR_STAGE_NAME} <= set(point["attributes"])
        assert set(point) in ({"name", "attributes", "value"}, {"name", "attributes", "histogram"})
        if "histogram" in point:
            assert set(point["histogram"]) == {"scale", "zero_count", "offset", "bucket_counts", "sum", "count"}


def test_semconv_matches_conventions():
    yaml = "".join(f.read_text() for f in CONVENTIONS.glob("*.yaml"))
    defined = set(re.findall(r"metric_name: (\S+)", yaml)) | set(re.findall(r"- id: (weir\.\S+)", yaml))
    in_sdk = {v for k, v in vars(sc).items() if k.isupper() and isinstance(v, str) and v.startswith("weir.")}
    assert in_sdk == defined
