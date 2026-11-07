# Developing the Weir Python SDK

For users, see [README.md](README.md).

## How it works

```
your code     stage.receive() ─▶ Item ─▶ item.emit() ─▶ item leaves (with block ends)
                    │                         │                │
recorder.py   item_entered()           item_emitted()     item_left()
              running totals per stage + the items inside each stage
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
| `recorder.py` | Per-stage running totals, and the items inside each stage |
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
