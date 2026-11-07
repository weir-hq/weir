# Weir demo pipeline

The "victim" pipeline for the Weir POC: a small but realistic async pipeline with faults you can
turn on and an exact **ground truth**, so every answer Weir gives can be scored.

Every service is instrumented with the Weir SDK (`sdk/python`) and reports to the Weir
server, whose UI runs on http://localhost:4100. The `scraper` service watches the edges
between stages from the outside (`scraper.toml`: the two Kafka topics and the staging table),
so items waiting in between are visible too. The `truth.*` tables are never read by Weir.

```
producer ─Kafka(orders)─▶ enrich ─Kafka(lines)─▶ batcher ─Celery/Redis─▶ worker ─▶ staged_lines ─(cron)─▶ aggregate ─▶ line_items
                          fan-out 1→N             fan-in 500 / 30s                (Postgres)        every 5m             (sink, dedupes)
```

## Run

```bash
make up                      # healthy pipeline
make up FAULT=quick          # fastest demo: 2% drop, 30s cron, 10s batches
make up FAULT=drop           # with a fault (see table)
make ui                      # open the Weir UI
make truth                   # ground truth: what was lost, why, and where time went
make baseline                # what standard tools show: Kafka lag, Celery queue, log errors
make reset                   # stop and wipe all data
```

`make truth SETTLE='3 minutes'` changes how old an order must be before a missing line counts as lost
(default 15 minutes, which covers the 5-minute cron).

Postgres is exposed on `localhost:55432` (`weir`/`weir`), or use `make psql`.

## Faults

| `FAULT=` | What happens | Truth shows | Standard tools show |
|---|---|---|---|
| `none` | Healthy | 0% missing | Healthy |
| `quick` | `drop`, plus a 30 s cron and 10 s batches for fast iteration | ~2% missing, all `enrich:drop` | Lag ≈ 0, queue 0, **0 errors** |
| `drop` | 2% of orders silently dropped at `enrich` | ~2% missing, all `enrich:drop` | Lag ≈ 0, queue 0, **0 errors** |
| `stall` | `enrich` stops for 2 min, 1 min after start | End-to-end p99 spikes for that window, `to_staging` dominates | Kafka lag spike (the one fault lag does catch) |
| `schedule` | Cron every 15 min instead of 5 | Age triples, all of it `schedule_wait`; work time unchanged | Everything idle and healthy |
| `dup` | 5% of orders sent twice | 0% missing; duplicates absorbed by the sink | Nothing |
| `poison` | Order `o-000001000` fails its batch forever | ~500 lines missing: 1 `worker:poison` order + collateral `worker:stuck_behind_poison` | **Queue length 0** (retries are held in worker memory); only retry log lines |

Fault presets live in `faults/*.env`; write your own and combine variables freely
(`DROP_RATE`, `STALL_SECONDS`, `STALL_AFTER_SECONDS`, `CRON_PERIOD_SECONDS`, `CRON_OFFSET_SECONDS`,
`DUPLICATE_RATE`, `POISON_KEY`, `ORDERS_PER_SECOND`, `MAX_LINES_PER_ORDER`, `BATCH_SIZE`,
`BATCH_MAX_WAIT_SECONDS`). `poison` needs a fresh database (`make reset` first).

## Ground truth

- `truth.produced`: every order the producer intended to send (expected line keys = `order_id-0..n-1`).
- `truth.injected`: every fault injected, with stage and fault type.
- `app.line_items`: the sink, with `event_time`, `staged_at` and `sink_at`, giving the exact
  latency split into *to staging* vs *schedule wait*.

Weir must never read `truth.*`. It exists only to check Weir's answers.

## Verified run, before Weir (`DROP_RATE=0.02`, 30 s cron, ~3 min)

```
== Conservation ==            expected 6915 | missing 139 | 2.010% | in flight 1297
== Why it is missing ==       enrich:drop   139 lines / 50 orders
== Latency (s) ==             to_staging p99 9.1 | schedule_wait p99 25.4 | end_to_end p99 34.5

== Kafka consumer lag ==      enrich 6-8, batcher 0-9      (healthy)
== Celery queue length ==     0
== Errors in logs ==          0
```

That gap, where 2% of the data is gone and nothing looks wrong, is what Weir has to close.

## Verified run, with Weir (`FAULT=quick`)

```
leak({pipeline="orders"}[5m])
  producer  in=800   vanished=0   unaccounted=0
  enrich    in=800   vanished=17  unaccounted=0   lost=2.1%     <- the silent drop
  batcher   in=2303  vanished=0   unaccounted=0
  worker    in=2500  vanished=0   unaccounted=0
  aggregate in=1500  vanished=0   unaccounted=0

decompose: ~80% of end-to-end age is waiting before `aggregate` (the cron), not work.
make truth: 2.2% of lines missing, all explained by enrich:drop.
```

## Scorecard

`make score` prints `make truth`, the standard tools and Weir's queries one after another.
Keep the laptop awake for the whole run (e.g. `caffeinate -ims`, lid open): a sleeping host
pauses every service and spoils the ground truth.

| Fault | Truth | Standard tools | Weir | Result |
|---|---|---|---|---|
| `quick` | 2.2% missing, all `enrich:drop` | Lag ≈ 0, queue 0, 0 errors | `leak`: enrich vanished 2.1%, 0 unaccounted | ✅ Found |
| `poison` (2026-11-02, `SETTLE='8 minutes'`, `RANGE=10m`) | 500 lines missing: 1 `worker:poison` + 499 `stuck_behind_poison`, 0 unexplained | Celery queue **0**, Kafka lag ≈ 50, 60 retry log lines | `worker`: 500 in flight, `oldest` 622 s and climbing, 30,000 retries (500 × 60), 0 vanished, 0 unaccounted: stuck, not lost | ✅ Found, exact |
| `stall` (2026-11-02, `SETTLE='8 minutes'`, `RANGE=11m`) | 0 missing; lines from the stall minute reach staging at p99 120 s (normally 9 s) | Kafka lag spikes during the stall: the one fault lag catches | 0 lost; `age` at `enrich` entry p99 peaks at 159 s; `decompose` wait before `enrich` 11 s (normally 0.004 s). `oldest` at `enrich` stays 0 during the stall | ⚠️ Found after the fact: items waiting in Kafka are between stages, so Weir sees the delay only when they arrive. p99 is coarse (x1.5 buckets) |
| `schedule` (2026-11-03, `SETTLE='16 minutes'`, `RANGE=20m`) | 0 missing; schedule wait p50 412.0 s, end to end p50 416.1 s / p99 821 s; to staging unchanged (p50 4.1 s) | Lag ≈ 70, queue 0, 0 errors: all healthy | `decompose`: wait before `aggregate` 412.1 s of 416.3 s end to end (99%); `aggregate` age p99 833 s; `batcher` in-stage 4.17 s, unchanged | ✅ Found, exact |
| `dup` (2026-11-03, `SETTLE='8 minutes'`, `RANGE=11m`) | 698 orders sent twice, 0 missing, absorbed by the sink | Lag ≈ 6, queue 0, 0 errors | `producer` emitted 698 more than it originated; `drops`: 1,644 lines `aggregate:duplicate` (5.1%); 0 unaccounted | ✅ Found, but only because the sink reports `drop("duplicate")`: the producer's extra emits look like fan-out |

Open questions:
- **Stopped stage** (`poison`, first run): when the cron stopped (host asleep), Weir showed it only
  as `queue_growth` on `worker`, and a healthy 5-minute cron shows the same between runs. Flagging
  a stage that stopped needs its expected cadence.
- **Items in transit** (`stall`): solved for Kafka and staging tables by the scraper (2026-11-07 run:
  90 s into the stall, `transit` showed `producer → enrich` with 1,599 orders waiting, the oldest
  82 s old, edge amber, while `oldest` at `enrich` still showed 0; 2,800 sent − 1,201 received =
  1,599 waiting, exactly). Kafka edges count messages read but not committed as waiting, so
  `unaccounted` on them is off by about the consumer's uncommitted buffer (−134 on `enrich → batcher`
  with 10 s batches, 20 on `producer → enrich`); the staging-table edge was exact (0). Celery queues
  are not scraped yet.
- **Duplicates** (`dup`): Weir has no item identity across copies; a sink that silently upserts
  would hide them.
- **Histogram precision** (`stall`): x1.5 buckets made p99 coarse (159 s reported vs 120 s true). Fixed: exponential histograms, within ~4%.

## Overhead benchmark

`make bench` measures what Weir costs a real Kafka consumer stage. Stop the demo first
(`make down`), so nothing else competes for CPU. It loads 300k messages into a fresh topic
and drains them with the same consume → work → produce loop in three modes, taking turns:
`off` (no Weir calls), `disabled` (`WEIR_DISABLED=1`) and `on`. It reports throughput, CPU per
message, p50/p99 handling time, memory and export bytes, and the difference between on and off.

```bash
make bench                       # worst case: the handler does nothing but parse JSON
make bench WORK_US=1000          # a handler doing 1 ms of work per message
make bench MESSAGES=100000 RUNS=5
```

Results (2026-11-06, Docker on a 2-CPU VM, 300k messages, median of 3 runs):

| Handler work | Mode | msg/s | CPU µs/msg | p99 µs | Max RSS MB | Export |
|---|---|---|---|---|---|---|
| none (worst case) | off | 120,689 | 3.8 | 6.8 | 128.1 | |
| | disabled | 116,982 | 6.2 | 10.7 | 137.0 | |
| | **on** | 111,873 | 7.6 | 14.0 | 137.7 | 0.6 KB/s |
| 1 ms | off | 992 | 1007.6 | 1018.1 | 79.5 | |
| | disabled | 990 | 1007.8 | 1023.2 | 89.8 | |
| | **on** | 987 | 1012.1 | 1040.9 | 92.5 | 0.3 KB/s |

- **Weir costs about 4 µs of CPU per message** either way: 3.8% of one core for every 10k msg/s.
  Next to a handler doing 1 ms of work that is +0.5% CPU and −0.5% throughput.
- Worst case, throughput drops 7%, not 50%, because Kafka I/O limits the stage more than Python does.
- `disabled` still costs about 60% of `on`: parsing and building contexts, not recording. That is
  where to optimize first.
- About 9 MB more memory, the same with `disabled`: a one-time cost of importing `weir`
  (6 MB of it is `urllib.request`), not per message.
- Export traffic is under 1 KB/s and does not grow with volume.

## Notes

- Kafka sets `-XX:UseSVE=0` because JDK 21 crashes with SIGILL on recent Apple Silicon.
- `enrich` uses auto-commit, so a crash can lose a few in-flight orders. That shows up as
  `UNEXPLAINED` in `make truth`, which is a real finding, not a bug in the report.
