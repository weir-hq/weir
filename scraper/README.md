# Weir scraper

Reports the items **waiting between two stages**: how many, and how old the oldest one is. No code
changes in the stages.

The SDK sees an item only while it is inside a stage. Once a stage sends it on, the item sits
in a Kafka topic or a staging table, invisible until the next stage receives it. So a stopped
consumer, or a cron that stopped running, used to show up only after it came back. The scraper
asks those places directly, every few seconds, and sends the Weir server:

| Metric | Meaning |
|---|---|
| `weir.item.waiting` | Items sent to the stage that it has not received yet |
| `weir.item.oldest_waiting.age` | Age of the oldest of them, since birth. Climbs while nothing is received. |

Both carry `weir.stage.name` (the receiving stage) and `weir.upstream_stage.name` (the sending
stage), so each edge of the graph gets its own numbers.

## Run

```bash
weir-scraper scraper.toml
```

See `src/weir_scraper/config.py` for the config format, and `demo/scraper.toml` for the demo's.

## Sources

| Source | Waiting items | Oldest |
|---|---|---|
| `kafka = { topic, group }` | Messages after the consumer group's committed offsets. Includes messages read but not committed yet (e.g. a batcher's buffer). | Birth time from the `weirparent` header of the first waiting message per partition, else its produce timestamp |
| `sql = "..."` | Not counted, except 0 when the query returns NULL (nothing waits) | Your query's one column: birth time in unix seconds, e.g. `SELECT extract(epoch FROM min(event_time)) FROM staging` |

Not yet: Celery/Redis queues, SQS, Pub/Sub, RabbitMQ.

## Keep the scrape cheap

Kafka costs nothing: the counts come from offsets the broker already keeps, and the scraper
reads one message per partition for the age.

A table needs care, because the scraper queries it every few seconds:

- **No `count(*)`.** It reads the whole table on every scrape. The oldest age already shows
  whether anything is stuck and for how long, and the SDK's sent and received counts show how
  many items are missing. Without a count, `transit` leaves `waiting` and `unaccounted` empty for
  that edge instead of guessing.
- **Index the birth-time column**, e.g. `CREATE INDEX ON staging (event_time)`, or a partial index
  for an outbox: `... WHERE processed = false`. Postgres then answers `min()` by reading one index
  entry, however big the table is. Without the index, `min()` reads the whole table too.
- **Timeout.** Every query runs with `statement_timeout` (`[postgres] timeout_seconds`, default
  1 s). A slower query is cancelled and the edge skipped for that round, so queries never pile up.
- **Interval per edge.** `interval_seconds` on an `[[edge]]`, at most 60. An edge into a 5-minute
  cron does not need checking every 5 s.
- **A read replica** takes the load off the primary entirely: point `dsn` at it.

## Develop

```bash
uv run --package weir-scraper pytest scraper/tests   # from the repo root
```
