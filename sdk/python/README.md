# weir-sdk

The Python SDK for [Weir](https://weirhq.com). Weir counts items as they move through an async
pipeline (queues, batches, cron jobs) and shows which stage lost items, which items are stuck,
and where the time goes.

```bash
pip install weir-sdk
```

Python 3.10+, no dependencies. The SDK sends to a Weir server; see the
[repository](https://github.com/weir-hq/weir) to run one.

## Usage

Each step of your pipeline is a stage. Every item a stage receives leaves it with an outcome.

```python
import weir

weir.init(pipeline="orders")
enrich = weir.stage("enrich")

# A Kafka consumer: read the context from the message, attach it to what you produce.
with enrich.receive(weir.extract(msg.headers())) as item:
    for line in split(msg):
        producer.produce("lines", line, headers=weir.inject(item.emit()))
```

Where items enter the pipeline, start them without a context:

```python
ingest = weir.stage("ingest")

with ingest.receive(key=order["id"], event_time=order["created_at"]) as item:  # unix seconds
    producer.produce("orders", order, headers=weir.inject(item.emit()))
```

When an item crosses something without headers, such as a database table, store the context
as a string and read it back:

```python
row["weir"] = item.emit().header()  # writing stage
ctx = weir.Context.from_header(row["weir"])  # reading stage
```

## Outcomes

When the `with` block ends, the item leaves the stage with:

| Outcome | When |
|---|---|
| `done` | You called `item.done()`, e.g. after writing to the sink |
| `dropped` | You called `item.drop("reason")` |
| `errored` | An exception escaped, or you called `item.error()` |
| `passed` | It called `item.emit()` at least once |
| `vanished` | None of the above: it went nowhere and nobody said why |

An item that never leaves (its `with` block never ends) counts as stuck, and its age keeps
growing. Call `stage.retried()` to count retries.

## Configuration

| Setting | Default |
|---|---|
| `weir.init(endpoint=...)` or `WEIR_ENDPOINT` | `http://localhost:4100` |
| `OTEL_SERVICE_NAME` | `unknown_service:<executable>` |
| `WEIR_DISABLED=1` | Off. When set, nothing is recorded or sent, and your code runs unchanged. |

The SDK sends running totals every 5 seconds from a background thread. Nothing is sent per
item. Cost: about 4 µs of CPU per message on a Kafka consumer, under 1 KB/s of traffic.

## Links

- [Weir repository](https://github.com/weir-hq/weir): server, scraper, demo
- [weir-scraper](https://pypi.org/project/weir-scraper/): items waiting between stages
- [Metric names](https://github.com/weir-hq/weir/tree/main/conventions), in OpenTelemetry
  semantic-conventions format
- License: Apache-2.0
