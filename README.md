# Weir

> Traces tell you how a request went. Weir tells you how your data is moving.

Weir is an observability signal for **async work**: queues, batches, workflows, ETLs and agent
pipelines. Today these are observed with per-system lag metrics, ad-hoc row-count SQL and guesswork.
Tracing breaks at every batch / fan-in boundary.

Weir answers four questions for any async pipeline, end to end:

| Question | Signal |
|---|---|
| **Is anything stuck or late?** | Age of the oldest unprocessed item, per stage (watermarks) |
| **Is anything lost?** | Conservation per stage and edge: `in = out + dropped + deduped + in_flight` |
| **Where does time go?** | Working vs. queueing vs. waiting for a batch window / schedule |
| **What happened to *this* record?** | Sampled per-key lineage across hops and batches |

See [ROADMAP.md](ROADMAP.md) for what comes next.

## Quick start

Requirements: Docker with Compose (`docker compose` or `docker-compose`) and `make`.

```bash
cd demo
make up FAULT=quick     # instrumented demo pipeline + Weir server; 2% silent drop, 30s cron
make ui                 # opens http://localhost:4100
```

Within a minute the graph shows `enrich` in red with `vanished` items. Then compare:

```bash
make truth SETTLE='2 minutes'   # ground truth from the demo's own tables
make baseline                   # what Kafka lag / Celery queue / logs show (nothing)
make reset                      # stop and wipe everything
```

Other faults: `make reset && make up FAULT=drop|stall|schedule|dup|poison`. See [demo/README.md](demo/README.md).

## Queries to try

| Query | What to look for |
|---|---|
| `graph({pipeline="orders"}[5m])` | The pipeline as a graph. Red = items vanished/errored or unaccounted; amber = old in-flight items or retries. |
| `leak({pipeline="orders"}[15m])` | Conservation per stage. `vanished` is the silent drop. |
| `drops({pipeline="orders"}[15m]) by (stage, reason)` | Why items did not pass. |
| `decompose({pipeline="orders"}[15m])` | Where end-to-end age comes from. With the 5-minute cron, almost all of it is waiting before `aggregate`. |
| `age({pipeline="orders"}, 0.99)` | p99 age when items leave each stage, over time. |
| `oldest({pipeline="orders"})` | Age of the oldest in-flight item: climbs forever under `FAULT=poison`. |
| `transit({pipeline="orders"}[15m])` | Per edge: items waiting between two stages now, the oldest one's age, and items lost in between. From the scraper. |
| `oldest_waiting({pipeline="orders"})` | Age of the oldest item waiting on each edge: climbs while a consumer or cron is stopped. |
| `throughput({pipeline="orders"})`, `in_flight({...})`, `lost({...})` | The usual suspects, per stage. |

Selectors work like PromQL: `{pipeline="orders", stage=~"batcher|worker"}`.

## Layout

```
conventions/         Metric and attribute names (weir.*), in OpenTelemetry semantic-conventions format
scraper/             Zero-code scraper: items waiting between stages (Kafka topics, staging tables)
sdk/python/          Python SDK (src/weir): context propagation (weirparent header) + running totals, exported every 5 s
server/              Go server: ingest, in-memory store, WeirQL parser/executor, embedded UI
demo/                Instrumented demo pipeline with fault injection and ground truth
```

## Developing

```bash
cd server && go test ./... && go run .          # server + UI on :4100
cd demo && docker-compose up -d --build weir-server   # or rebuild just the server container
```

The server keeps everything in memory: restarting it wipes history (conservation stays
correct after a restart; history before it is simply gone).

The SDK sends its 5 s windows to `POST /ingest` as metric data points named after
`conventions/`, the same YAML format as the
[OpenTelemetry semantic conventions](https://github.com/open-telemetry/semantic-conventions).
To change a name, edit `conventions/*.yaml` first, then `sdk/python/src/weir/semconv.py` and
`server/semconv.go`; tests on both sides fail until all three match. `make check-conventions`
validates the YAML with OpenTelemetry Weaver.

## POC status

| Done | Not yet |
|---|---|
| SDK: propagation, outcomes (passed/done/dropped/vanished/errored), in-flight + oldest, age histograms | Per-key hop records and `trace("key")` |
| Server: ingest, series store with label matching, WeirQL v0 (`graph`, `leak`, `drops`, `decompose`, `age`, `oldest`, `in_flight`, `throughput`, `lost`) | Disk snapshots, retention config, `topk` and other composition |
| UI: query bar, graph, time series, tables, auto-refresh | Sankey widths by volume per edge, key trace view |
| In transit: the scraper reports items waiting on Kafka and staging-table edges, live (`transit`, `oldest_waiting`) | |
| Overhead: ~4 µs CPU per message on a real Kafka consumer, +0.5% next to a 1 ms handler (`make bench`) | Overhead with per-key lineage sampling |
| Verified: all five faults scored against `make truth` (scorecard in `demo/README.md`): `quick`, `poison`, `schedule` exact; `stall` and `dup` found with caveats | Celery/SQS/RabbitMQ scraping, stopped-stage detection from schedules, duplicate identity, finer histograms |

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE).
