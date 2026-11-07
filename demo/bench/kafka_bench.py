"""Weir's overhead on a Kafka consume -> work -> produce loop. Run: make bench (demo stopped).

Modes: off (no Weir calls), disabled (WEIR_DISABLED=1), on. Each run is a fresh process and
consumer group over the same topic; modes alternate; medians are reported. --work-us is the
handler's own work per message (0 = worst case for Weir).
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import statistics
import subprocess
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
MODES = ["off", "disabled", "on"]


# Runs in the parent process


def load_topic(topic: str, messages: int) -> None:
    import weir

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
    born = time.time()
    for i in range(messages):
        ctx = weir.Context(pipeline="bench", key=f"o-{i:09d}", event_time=born, last_stage="producer")
        value = json.dumps({"order_id": ctx.key, "customer": f"c-{i % 1000}", "amount": i % 500, "lines": 3})
        produce(producer, topic, value.encode(), weir.inject(ctx))
    producer.flush()


def run_once(mode: str, topic: str, messages: int, work_us: int) -> dict:
    env = {**os.environ, "OTEL_SERVICE_NAME": f"bench-{mode}"}
    env.pop("WEIR_DISABLED", None)
    if mode == "disabled":
        env["WEIR_DISABLED"] = "1"
    args = [sys.executable, __file__, "--drain", mode, "--topic", topic]
    args += ["--messages", str(messages), "--work-us", str(work_us)]
    done = subprocess.run(args, env=env, capture_output=True, text=True, check=True)
    return json.loads(done.stdout.strip().splitlines()[-1])


def report(results: dict[str, list[dict]], messages: int, work_us: int) -> None:
    def median(mode: str, field: str) -> float:
        return statistics.median(run[field] for run in results[mode])

    print(f"\n{messages:,} messages per run, {work_us} us of work per message, median of {len(results['off'])} runs\n")
    print(
        f"  {'mode':<9} {'msg/s':>9} {'CPU us/msg':>11} {'p50 us':>8} {'p99 us':>8} {'max RSS MB':>11} {'export':>13}"
    )
    for mode in MODES:
        export = median(mode, "export_bytes") / median(mode, "seconds") / 1024
        export_text = f"{export:.1f} KB/s" if mode == "on" else "-"
        print(
            f"  {mode:<9} {median(mode, 'msgs_per_s'):>9,.0f} {median(mode, 'cpu_us_per_msg'):>11.1f}"
            f" {median(mode, 'p50_us'):>8.1f} {median(mode, 'p99_us'):>8.1f}"
            f" {median(mode, 'max_rss_mb'):>11.1f} {export_text:>13}"
        )

    cpu_off, cpu_on = median("off", "cpu_us_per_msg"), median("on", "cpu_us_per_msg")
    rate_off, rate_on = median("off", "msgs_per_s"), median("on", "msgs_per_s")
    print("\nFlow on vs off:")
    print(f"  CPU per message   {cpu_on - cpu_off:+.1f} us ({100 * (cpu_on - cpu_off) / cpu_off:+.1f}%)")
    print(f"  Max throughput    {100 * (rate_on - rate_off) / rate_off:+.1f}%")
    print(f"  p99 latency       {median('on', 'p99_us') - median('off', 'p99_us'):+.1f} us")
    # d us/msg at 10k msg/s = d% of a core
    print(f"  Weir's own CPU    {cpu_on - cpu_off:.1f}% of one core for every 10k msg/s the stage handles")


# Runs in each child process


def drain(mode: str, topic: str, messages: int, work_us: int) -> dict:
    instrumented = mode != "off"
    sent = {"bytes": 0}
    if instrumented:
        import weir
        from weir import export

        send = export.send

        def counting_send(endpoint: str, payload: dict) -> None:
            sent["bytes"] += len(json.dumps(payload))
            send(endpoint, payload)

        export.send = counting_send
        weir.init(pipeline="bench")
        stage = weir.stage("bench")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"bench-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
    out_topic = topic + "-out"

    def handle(msg) -> None:
        if instrumented:
            with stage.receive(weir.extract(msg.headers())) as item:
                value = work(msg.value(), work_us)
                produce(producer, out_topic, value, weir.inject(item.emit()))
        else:
            value = work(msg.value(), work_us)
            produce(producer, out_topic, value, msg.headers())

    first = poll(consumer)  # start the clock once partitions are assigned
    started, cpu_started = time.perf_counter(), time.process_time()
    latencies = []
    msg = first
    for _ in range(messages):
        handled_at = time.perf_counter()
        handle(msg)
        latencies.append(time.perf_counter() - handled_at)
        if len(latencies) < messages:
            msg = poll(consumer)
    producer.flush()
    seconds = time.perf_counter() - started
    cpu = time.process_time() - cpu_started
    if instrumented:
        weir.flush()
    consumer.close()

    latencies.sort()
    return {
        "mode": mode,
        "seconds": seconds,
        "msgs_per_s": messages / seconds,
        "cpu_us_per_msg": cpu / messages * 1e6,
        "p50_us": latencies[len(latencies) // 2] * 1e6,
        "p99_us": latencies[int(len(latencies) * 0.99)] * 1e6,
        "max_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,  # KB on Linux
        "export_bytes": sent["bytes"],
    }


def work(value: bytes, work_us: int) -> bytes:
    order = json.loads(value)
    order["enriched"] = True
    until = time.perf_counter() + work_us / 1e6
    while time.perf_counter() < until:
        pass
    return json.dumps(order).encode()


def poll(consumer: Consumer):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            return msg
    raise TimeoutError("no message from Kafka for 60 s")


def produce(producer: Producer, topic: str, value: bytes, headers) -> None:
    while True:
        try:
            producer.produce(topic, value, headers=headers)
            producer.poll(0)
            return
        except BufferError:  # local queue full: let it drain
            producer.poll(0.1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--messages", type=int, default=300_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--work-us", type=int, default=0)
    parser.add_argument("--drain", choices=MODES, help=argparse.SUPPRESS)
    parser.add_argument("--topic", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.drain:
        print(json.dumps(drain(args.drain, args.topic, args.messages, args.work_us)))
        return

    topic = f"bench-{int(time.time())}"
    print(f"Loading {args.messages:,} messages into {topic} ...", flush=True)
    load_topic(topic, args.messages)

    # The first read of a fresh topic is slow (cold cache): leave it unmeasured.
    warm_up = min(args.messages, 20_000)
    print(f"Warming up on {warm_up:,} messages ...", flush=True)
    run_once("off", topic, warm_up, args.work_us)

    results: dict[str, list[dict]] = {mode: [] for mode in MODES}
    for run in range(args.runs):
        order = MODES[run % len(MODES) :] + MODES[: run % len(MODES)]
        for mode in order:
            result = run_once(mode, topic, args.messages, args.work_us)
            results[mode].append(result)
            print(f"  run {run + 1} {mode:<9} {result['msgs_per_s']:>9,.0f} msg/s", flush=True)
    report(results, args.messages, args.work_us)


if __name__ == "__main__":
    main()
