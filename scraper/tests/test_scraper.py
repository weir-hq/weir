import pytest
from weir import semconv as sc
from weir_scraper import config as config_module
from weir_scraper.config import Edge, load
from weir_scraper.main import edge_points, scrape
from weir_scraper.sources import Waiting


class Fixed:
    def __init__(self, waiting: Waiting | Exception) -> None:
        self.waiting = waiting

    def measure(self) -> Waiting:
        if isinstance(self.waiting, Exception):
            raise self.waiting
        return self.waiting


def test_points_name_the_edge_and_age_the_oldest_item():
    edge = Edge("producer", "enrich", Fixed(Waiting(0, None)))
    points = edge_points("orders", edge, Waiting(12, oldest_event_time=1000.0), now=1095.0)
    attributes = {
        sc.WEIR_PIPELINE_NAME: "orders",
        sc.WEIR_STAGE_NAME: "enrich",
        sc.WEIR_UPSTREAM_STAGE_NAME: "producer",
    }
    assert points == [
        {"name": sc.WEIR_ITEM_WAITING, "attributes": attributes, "value": 12},
        {"name": sc.WEIR_ITEM_OLDEST_WAITING_AGE, "attributes": attributes, "value": 95.0},
    ]


def test_nothing_waiting_reports_zero_and_no_age():
    edge = Edge("producer", "enrich", Fixed(Waiting(0, None)))
    points = edge_points("orders", edge, Waiting(0, None), now=1095.0)
    assert [p["name"] for p in points] == [sc.WEIR_ITEM_WAITING]
    assert points[0]["value"] == 0


def test_failing_source_is_left_out_not_reported_as_zero():
    edges = [
        Edge("producer", "enrich", Fixed(ConnectionError("broker down"))),
        Edge("worker", "aggregate", Fixed(Waiting(3, 100.0))),
    ]
    payload = scrape("orders", edges)
    stages = {p["attributes"][sc.WEIR_STAGE_NAME] for p in payload["metrics"]}
    assert stages == {"aggregate"}
    assert set(payload) == {"resource", "time_unix_nano", "metrics"}


def test_uncounted_edge_reports_only_the_oldest_age():
    edge = Edge("worker", "aggregate", Fixed(Waiting(0, None)))
    points = edge_points("orders", edge, Waiting(None, oldest_event_time=1000.0), now=1080.0)
    assert [(p["name"], p["value"]) for p in points] == [(sc.WEIR_ITEM_OLDEST_WAITING_AGE, 80.0)]


def test_config_builds_one_source_per_edge(tmp_path, monkeypatch):
    built = []
    monkeypatch.setattr(config_module, "KafkaTopic", lambda *args: built.append(("kafka", *args)) or Fixed(None))
    monkeypatch.setattr(config_module, "SqlQuery", lambda *args: built.append(("sql", *args)) or Fixed(None))
    path = tmp_path / "scraper.toml"
    path.write_text(
        """
pipeline = "orders"
[kafka]
bootstrap = "kafka:9092"
[postgres]
dsn = "postgresql://x"
timeout_seconds = 2
[[edge]]
from = "producer"
to = "enrich"
kafka = { topic = "orders", group = "enrich" }
[[edge]]
from = "worker"
to = "aggregate"
sql = "SELECT NULL"
interval_seconds = 30
"""
    )
    config = load(path)
    assert [(e.upstream, e.stage, e.interval_seconds) for e in config.edges] == [
        ("producer", "enrich", 5.0),
        ("worker", "aggregate", 30.0),
    ]
    assert built == [("kafka", "kafka:9092", "orders", "enrich"), ("sql", "postgresql://x", "SELECT NULL", 2.0)]


def test_config_rejects_an_interval_the_server_would_treat_as_stale(tmp_path):
    path = tmp_path / "scraper.toml"
    path.write_text(
        'pipeline = "orders"\n[[edge]]\nfrom = "a"\nto = "b"\nsql = "SELECT NULL"\ninterval_seconds = 120\n'
    )
    with pytest.raises(ValueError, match="interval_seconds"):
        load(path)


def test_config_rejects_an_edge_without_a_source(tmp_path):
    path = tmp_path / "scraper.toml"
    path.write_text('pipeline = "orders"\n[[edge]]\nfrom = "a"\nto = "b"\n')
    with pytest.raises(ValueError, match="needs"):
        load(path)
