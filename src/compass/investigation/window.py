"""Publish completed Investigation Window context to Layer 2."""
from __future__ import annotations

from typing import Any

from compass.bus.publisher import publish
from compass.bus.routing_keys import BusComponent, routing_key_for
from compass.context.context_collector import CollectorResult, ContextCollector
from compass.schemas.envelope import Envelope
from compass.schemas.investigation import InvestigationEnvelope, InvestigationWindowRequest


def _context_payload(result: CollectorResult) -> dict[str, Any]:
    """Convert pull-adapter results into the Layer 2 investigation contract."""
    metrics = result.metrics
    return {
        "metrics": {
            "request_rate": metrics.request_rate,
            "error_rate": metrics.error_rate,
            "p95_latency": metrics.p95_latency,
            "cpu_usage": metrics.cpu_usage,
            "memory_usage": metrics.memory_usage,
            "memory_usage_percent": metrics.memory_usage_percent,
            "disk_usage_percent": metrics.disk_usage_percent,
        },
        "logs": {"signals": result.log_signals, "lines": result.log_lines},
        "kubernetes": {
            "namespace": metrics.namespace,
            "pod": metrics.pod,
            "container": metrics.container,
        },
        "collection": {
            "architecture": metrics.architecture.value,
            "collected_at": metrics.collected_at.isoformat(),
            "had_metric_errors": result.had_metric_errors,
            "had_log_errors": result.had_log_errors,
        },
    }


async def collect_and_process(
    request: InvestigationWindowRequest, context_collector: ContextCollector
) -> CollectorResult:
    """Collect pull-adapter context for an opened window and publish it to Layer 2."""
    result = await context_collector.build(
        request.service, request.environment, request.window_seconds
    )
    investigation = InvestigationEnvelope(
        service=request.service,
        environment=request.environment,
        investigation_id=request.investigation_id,
        correlation_id=request.correlation_id,
        deployment_id=request.deployment_id,
        context=_context_payload(result),
        trigger=request.model_dump(
            mode="json", exclude={"service", "environment", "window_seconds"}
        ),
    )
    await process(investigation)
    return result


async def process(investigation: InvestigationEnvelope) -> None:
    """Wrap a completed investigation and emit it for anomaly detection."""
    envelope_args: dict[str, Any] = dict(
        service=investigation.service,
        environment=investigation.environment,
        emitted_by=BusComponent.INVESTIGATION_WINDOW,
        payload=investigation.model_dump(mode="json"),
    )
    if investigation.correlation_id is not None:
        envelope_args["correlation_id"] = investigation.correlation_id
    envelope = Envelope(**envelope_args)
    await publish(routing_key_for(BusComponent.INVESTIGATION_WINDOW), envelope)
