"""Names from conventions/. Keep in sync with conventions/*.yaml and server/semconv.go."""

from __future__ import annotations

from enum import Enum

# Attributes
WEIR_PIPELINE_NAME = "weir.pipeline.name"
WEIR_STAGE_NAME = "weir.stage.name"
WEIR_UPSTREAM_STAGE_NAME = "weir.upstream_stage.name"
WEIR_ITEM_OUTCOME = "weir.item.outcome"
WEIR_DROP_REASON = "weir.drop.reason"
WEIR_STAGE_BOUNDARY = "weir.stage.boundary"
WEIR_STAGE_BOUNDARY_ENTRY = "entry"
WEIR_STAGE_BOUNDARY_EXIT = "exit"


class Outcome(str, Enum):
    """Values of weir.item.outcome."""

    PASSED = "passed"
    DONE = "done"
    DROPPED = "dropped"
    ERRORED = "errored"
    VANISHED = "vanished"  # no output and no outcome: silent loss


# OpenTelemetry resource attributes
SERVICE_NAME = "service.name"
SERVICE_INSTANCE_ID = "service.instance.id"

# Metrics
WEIR_STAGE_RECEIVED_ITEMS = "weir.stage.received.items"
WEIR_STAGE_ORIGINATED_ITEMS = "weir.stage.originated.items"
WEIR_STAGE_EMITTED_ITEMS = "weir.stage.emitted.items"
WEIR_STAGE_COMPLETED_ITEMS = "weir.stage.completed.items"
WEIR_STAGE_RETRIES = "weir.stage.retries"
WEIR_STAGE_CONTEXT_ERRORS = "weir.stage.context.errors"
WEIR_STAGE_PROCESS_DURATION = "weir.stage.process.duration"
WEIR_ITEM_AGE = "weir.item.age"
WEIR_ITEM_ACTIVE = "weir.item.active"
WEIR_ITEM_OLDEST_AGE = "weir.item.oldest.age"
WEIR_ITEM_WAITING = "weir.item.waiting"
WEIR_ITEM_OLDEST_WAITING_AGE = "weir.item.oldest_waiting.age"
