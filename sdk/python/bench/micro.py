"""SDK cost per message (extract, receive, emit, inject, close), no broker or network.

Run: make bench-sdk. --max-us N fails above N microseconds (used by CI).
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import tracemalloc

import weir
from weir.recorder import NullRecorder, Recorder
from weir.runtime import runtime

HEADERS = weir.inject(weir.Context(pipeline="bench", key="o-1", event_time=time.time(), last_stage="producer"))


def one_message(stage: weir.Stage) -> None:
    with stage.receive(weir.extract(HEADERS)) as item:
        weir.inject(item.emit())


def seconds_per_message(messages: int, threads: int) -> float:
    stage = weir.stage("bench")
    per_thread = messages // threads

    def run() -> None:
        for _ in range(per_thread):
            one_message(stage)

    run_on_threads = [threading.Thread(target=run) for _ in range(threads)]
    started = time.perf_counter()
    for thread in run_on_threads:
        thread.start()
    for thread in run_on_threads:
        thread.join()
    elapsed = time.perf_counter() - started

    return elapsed / (per_thread * threads)


def bytes_per_open_item(items: int) -> float:
    stage = weir.stage("bench")
    ctx = weir.extract(HEADERS)
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    still_open = [stage.receive(ctx) for _ in range(items)]
    after = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    del still_open
    return (after - before) / items


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--messages", type=int, default=200_000)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--max-us", type=float, default=None)
    args = parser.parse_args()

    runtime.recorder = Recorder()
    seconds_per_message(10_000, 1)  # warm up
    on = seconds_per_message(args.messages, 1)
    on_threads = seconds_per_message(args.messages, args.threads)

    runtime.recorder = NullRecorder()
    seconds_per_message(10_000, 1)
    disabled = seconds_per_message(args.messages, 1)

    runtime.recorder = Recorder()
    open_item = bytes_per_open_item(100_000)

    print(f"Per message (extract, receive, emit, inject, close), {args.messages:,} messages:")
    print(f"  SDK on, 1 thread          {on * 1e6:6.2f} us")
    print(f"  SDK on, {args.threads} threads         {on_threads * 1e6:6.2f} us")
    print(f"  WEIR_DISABLED=1, 1 thread {disabled * 1e6:6.2f} us")
    print()
    print("Overhead next to the handler's own work per message:")
    for work_ms in (0.1, 1.0, 10.0):
        print(f"  {work_ms:5.1f} ms of work   {100 * on / (work_ms / 1000):6.2f}%")
    print()
    print(f"At 10k msg/s: {on * 10_000 * 100:.1f}% of one core")
    print(f"Memory per item inside a stage (not yet closed): {open_item:.0f} bytes")

    if args.max_us is not None and on * 1e6 > args.max_us:
        print(f"\nFAIL: {on * 1e6:.2f} us per message is over --max-us {args.max_us}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
