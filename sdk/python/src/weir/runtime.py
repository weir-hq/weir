"""The process-wide runtime: config, recorder, and a thread exporting every FLUSH_SECONDS."""

from __future__ import annotations

import atexit
import os
import socket
import sys
import threading
import time
from pathlib import Path

from weir import export
from weir.recorder import NullRecorder, Recorder
from weir.semconv import SERVICE_INSTANCE_ID, SERVICE_NAME

FLUSH_SECONDS = 5.0
DEFAULT_ENDPOINT = "http://localhost:4100"


class Runtime:
    def __init__(self) -> None:
        self.pipeline = "default"
        self.endpoint = DEFAULT_ENDPOINT
        self.disabled = os.environ.get("WEIR_DISABLED", "").lower() in ("1", "true", "yes")
        self.recorder = NullRecorder() if self.disabled else Recorder()
        self._started = False

    def start(self, pipeline: str, endpoint: str | None = None) -> None:
        self.pipeline = pipeline
        self.endpoint = endpoint or os.environ.get("WEIR_ENDPOINT") or DEFAULT_ENDPOINT
        if not self._started and not self.disabled:
            self._started = True
            self._start_exporter_thread()

    def flush(self) -> None:
        now = time.time()
        snapshot = self.recorder.take_snapshot()
        if snapshot:
            payload = export.build_payload(self.pipeline, resource(), snapshot, now, self.recorder.started_at)
            export.send(self.endpoint, payload)

    def _start_exporter_thread(self) -> None:
        threading.Thread(target=self._export_forever, name="weir-exporter", daemon=True).start()

    def _export_forever(self) -> None:
        while True:
            time.sleep(FLUSH_SECONDS)
            self.flush()

    def _after_fork_in_child(self) -> None:
        # Threads don't survive fork (e.g. Celery prefork): fresh totals and a new exporter.
        self.recorder = NullRecorder() if self.disabled else Recorder()
        if self._started:
            self._start_exporter_thread()


def resource() -> dict[str, str]:
    """OpenTelemetry resource attributes. Recomputed each export: the pid changes after fork."""
    service = os.environ.get("OTEL_SERVICE_NAME") or f"unknown_service:{Path(sys.executable).name}"
    return {SERVICE_NAME: service, SERVICE_INSTANCE_ID: f"{socket.gethostname()}-{os.getpid()}"}


runtime = Runtime()
os.register_at_fork(after_in_child=runtime._after_fork_in_child)
atexit.register(lambda: runtime._started and runtime.flush())  # only if init() was called
