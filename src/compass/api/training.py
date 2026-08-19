from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from compass.context.context_collector import CONTEXT_COLLECTOR_STATE_KEY, ContextCollector, CollectorResult
from compass.schemas.response import BatchContextResponse, ContextResponse
from compass.bus.publisher import publish
from compass.bus.routing_keys import BusComponent, routing_key_for
from compass.schemas.envelope import Envelope

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/training", tags=["training"])


def get_context_builder(request: Request) -> ContextCollector:
    """Return the shared collector wired to the application's pull adapters."""
    collector: ContextCollector | None = getattr(
        request.app.state, CONTEXT_COLLECTOR_STATE_KEY, None
    )
    if collector is None:
        raise RuntimeError("ContextCollector is not attached to app.state")
    return collector


def _to_context_response(service: str, environment: str, result: CollectorResult) -> ContextResponse:
    return ContextResponse(
        service=service,
        environment=environment,
        context=result.context,
        had_metric_errors=result.had_metric_errors,
        had_log_errors=result.had_log_errors,
    )


@router.post("/collect", response_model=BatchContextResponse)
async def collect_training_sample(
    environment: str = Query(default="prod"),
    window_seconds: int = Query(default=300, gt=0, le=3600),
    services: list[str] | None = Query(default=None),
    builder: ContextCollector = Depends(get_context_builder),
):
    """Collect context for all services, returned as a single training sample."""
    service_set = set(services) if services else None
    target_services = service_set if service_set is not None else await builder.discover_services()

    results: dict[str, ContextResponse] = {}
    failed: list[str] = []
    raw_results = await builder.build_all(environment, window_seconds, services=target_services)

    for service in target_services:
        result = raw_results.get(service)
        if result is None:
            failed.append(service)
            continue
        results[service] = _to_context_response(service, environment, result)

    return BatchContextResponse(
        environment=environment,
        window_seconds=window_seconds,
        results=results,
        failed_services=failed,
    )


@router.get("/dataset", response_model=list[BatchContextResponse])
async def collect_dataset(
    windows: int = Query(default=1, gt=0, le=10),
    environment: str = Query(default="prod"),
    window_seconds: int = Query(default=300, gt=0, le=3600),
    services: list[str] | None = Query(default=None),
    builder: ContextCollector = Depends(get_context_builder),
):
    """Collect N windows of context data (returns a list of batch contexts).
    Note: For true historical data, a script polling over time or a Prometheus 
    range query is needed. This endpoint currently returns the current window N times 
    for demonstration/testing, but in a real scenario would execute range queries.
    """
    # For now, just generate `windows` samples (they will be identical for the same instant, 
    # but provides the requested endpoint shape).
    sample = await collect_training_sample(environment, window_seconds, services, builder)
    return [sample for _ in range(windows)]


@router.post("/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_training():
    """Trigger a full training run by publishing to the bus."""
    envelope = Envelope(
        service="compass",
        environment="prod",
        emitted_by=BusComponent.TRAINING_TRIGGER,
        payload={"action": "start_training"},
    )
    await publish(routing_key_for(BusComponent.TRAINING_TRIGGER), envelope)
    return {"status": "accepted", "message": "Training triggered"}
