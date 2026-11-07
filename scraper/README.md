# weir-scraper

Part of [Weir](https://weirhq.com). The scraper reports the items waiting **between** two
stages of a pipeline, in a queue or a staging table: how many, and how old the oldest one is.
It runs on its own, with no code changes in the stages.

The [Weir SDK](https://pypi.org/project/weir-sdk/) sees an item while a stage is handling it.
In between, the item sits in a topic or a table until the next stage picks it up. The scraper
looks there, so a stopped consumer or a cron job that stopped running shows up while it is
stopped, not after it comes back.

```bash
pip install weir-scraper
weir-scraper scraper.toml
```

Python 3.11+.

## Configuration

```toml
pipeline = "orders"
endpoint = "http://localhost:4100"   # or $WEIR_ENDPOINT
interval_seconds = 5                 # per edge too; at most 60

[kafka]
bootstrap = "kafka:9092"

[postgres]
dsn = "postgresql://user:password@db:5432/app"
timeout_seconds = 1

# producer sends to enrich through the "orders" topic
[[edge]]
from = "producer"
to = "enrich"
kafka = { topic = "orders", group = "enrich" }

# worker sends to aggregate through a staging table
[[edge]]
from = "worker"
to = "aggregate"
sql = "SELECT extract(epoch FROM min(event_time)) FROM staged_lines"
interval_seconds = 15
```

`from` and `to` are the stage names your services pass to `weir.stage()`.

## Sources

| Source | Waiting items | Oldest item |
|---|---|---|
| `kafka = { topic, group }` | Messages after the consumer group's committed offsets. This includes messages read but not committed yet, so it is slightly high while the consumer is healthy. | Birth time from the Weir context of the first waiting message in each partition, else its produce time |
| `sql = "..."` | Not counted (0 when the query returns NULL) | The query's single column: birth time in unix seconds, or NULL when nothing waits |

Not supported yet: Celery/Redis, SQS, Pub/Sub, RabbitMQ.

## Keeping table queries cheap

- Don't use `count(*)`: it reads the whole table on every scrape.
- Index the birth-time column, e.g. `CREATE INDEX ON staged_lines (event_time)`, so `min()`
  reads one index entry. For an outbox, a partial index: `... WHERE processed = false`.
- Every query runs with a statement timeout (`timeout_seconds`, default 1 s); a slow query is
  cancelled and the edge skipped for that round.
- Point `dsn` at a read replica to keep the load off the primary.

## Links

- [Weir repository](https://github.com/weir-hq/weir): server, SDK, demo
- [Example config](https://github.com/weir-hq/weir/blob/main/demo/scraper.toml) from the demo pipeline
- License: Apache-2.0
