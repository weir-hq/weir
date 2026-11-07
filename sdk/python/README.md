# Weir Python SDK

Measures how items move through an async pipeline: how many enter and leave each stage, how
they leave, how old they are, and which ones are stuck. No dependencies.

```python
import weir

weir.init(pipeline="orders")  # endpoint: $WEIR_ENDPOINT, then http://localhost:4100
enrich = weir.stage("enrich")

with enrich.receive(weir.extract(msg.headers())) as item:
    for line in explode(msg):
        producer.produce("lines", line, headers=weir.inject(item.emit()))
```

Environment variables:

| Variable | Effect |
|---|---|
| `WEIR_ENDPOINT` | Weir server URL, when `weir.init()` gets no `endpoint` |
| `OTEL_SERVICE_NAME` | The reporting service's name |
| `WEIR_DISABLED=1` | Kill switch: records and sends nothing. Your code keeps working unchanged, and contexts still propagate. |

## The rules

1. **Every item that enters a stage leaves it exactly once, with one outcome.** At the end of
   the `with` block the item leaves with:
   - the outcome you set: `item.done()`, `item.drop("reason")` or `item.error()`;
   - otherwise `errored`, if an exception escaped;
   - otherwise `passed` if it called `emit()`, and **`vanished`** if it did not. Vanished is
     silent loss: nobody decided to drop the item, it just went nowhere.
2. **An item never closed stays inside the stage.** It is reported as active, with its age
   growing. That is how stuck items show up.
3. **Ages count from birth.** The first stage stamps `event_time`, and it travels in the
   `weirparent` header. So the age at each stage includes time waiting in queues and for crons.

Together: for every stage, `items in = items out + items still inside`. When that does not
hold on the server, items were lost somewhere Weir could not see.

## How it works

```
your code     stage.receive() ─▶ Item ─▶ item.emit() ─▶ item leaves (with block ends)
                    │                         │                │
recorder.py   item_entered()           item_emitted()     item_left()
              counts per stage for this window + the items inside each stage
                    │
                    │  every 5 s, on a background thread (runtime.py)
                    ▼
export.py     take_snapshot() ─▶ build_payload() ─▶ POST /ingest
```

Nothing per item goes over the network: only counts, histograms and two gauges per stage, so
the cost does not grow with traffic.

The counts are **running totals** since the process started (cumulative, as in Prometheus and
OpenTelemetry), never reset. The server subtracts each report from the previous one. So an
export that fails loses nothing (the next one carries the same counts), an export delivered
twice counts nothing twice, and a restarted process is detected by its new start time.

| File | What it holds |
|---|---|
| `stage.py` | `Stage` and `Item`: everything pipeline code calls |
| `context.py` | `Context`, the `weirparent` header, `extract()` / `inject()` |
| `recorder.py` | Per-stage counts for the current window, and the items inside each stage |
| `export.py` | Turns a snapshot into the `/ingest` JSON, and sends it |
| `runtime.py` | The one global: configuration, the recorder, the exporter thread, fork and exit hooks |
| `semconv.py` | Metric and attribute names, and the `Outcome` values, from `conventions/` |

Dependencies only point down the table, except that `stage.py` reaches the recorder through
`runtime`.

## Changing things

**Add a metric:**
1. Define it in `conventions/metrics.yaml`.
2. Add its name to `semconv.py` and to `server/semconv.go`. A test on each side fails until
   all three match.
3. Count it in `recorder.py`: a field on `StageCounts`, updated in one of the `item_*` methods.
4. Send it in `export.py`: one more `points.append(...)` in `stage_points()`.
5. Read it on the server, in `Counts.add` in `server/store.go`.

**Support a new broker:** nothing in the SDK changes. Call `weir.extract()` on the headers you
receive and attach `weir.inject(item.emit())` to what you send. For a hop with no headers,
such as a database table, store `item.emit().header()` in a column and rebuild the context
with `weir.Context.from_header()`.

## Develop

From the repo root:

```bash
make test-sdk     # tests
make bench-sdk    # cost per message, no broker or network (bench/micro.py)
make bench        # cost on a real Kafka consumer, with and without Weir (demo/bench/)
```

## Overhead

`make bench-sdk` on an M-series Mac, one message through `extract → receive → emit → inject → close`:

| | Per message |
|---|---|
| SDK on | ~3 µs |
| `WEIR_DISABLED=1` | ~1.7 µs (parsing and building contexts still runs) |
| An item inside a stage, not yet closed | ~210 bytes of memory |

That is 0.3% next to a handler doing 1 ms of work, or 3% of one core at 10k msg/s.

On a real Kafka consumer (`make bench`, results in `demo/README.md`): about 4 µs of CPU per
message, +0.5% CPU and −0.5% throughput next to a 1 ms handler, and under 1 KB/s of export
traffic at any volume.
