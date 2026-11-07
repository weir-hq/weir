package main

// Metric and attribute names from the Weir semantic conventions (conventions/ at the repo root).
// Keep in sync with conventions/*.yaml and sdk/python/src/weir/semconv.py.

// Attributes
const (
	attrPipelineName      = "weir.pipeline.name"
	attrStageName         = "weir.stage.name"
	attrUpstreamStageName = "weir.upstream_stage.name"
	attrItemOutcome       = "weir.item.outcome"
	attrDropReason        = "weir.drop.reason"
	attrStageBoundary     = "weir.stage.boundary"

	// Resource attributes, from the OpenTelemetry semantic conventions
	attrServiceInstanceID = "service.instance.id"
)

// Metrics
const (
	metricStageReceivedItems   = "weir.stage.received.items"
	metricStageOriginatedItems = "weir.stage.originated.items"
	metricStageEmittedItems    = "weir.stage.emitted.items"
	metricStageCompletedItems  = "weir.stage.completed.items"
	metricStageRetries         = "weir.stage.retries"
	metricStageContextErrors   = "weir.stage.context.errors"
	metricStageProcessDuration = "weir.stage.process.duration"
	metricItemAge              = "weir.item.age"
	metricItemActive           = "weir.item.active"
	metricItemOldestAge        = "weir.item.oldest.age"
	metricItemWaiting          = "weir.item.waiting"
	metricItemOldestWaitingAge = "weir.item.oldest_waiting.age"
)
