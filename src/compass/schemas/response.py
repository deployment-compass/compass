"""Pydantic models for the observability test API responses."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ServiceListResponse(BaseModel):
    """Discovered services from one or more telemetry sources."""
    prometheus_services: list[str]
    loki_services: list[str]
    union: list[str]
    prometheus_error: Optional[str] = None
    loki_error: Optional[str] = None


class ContextResponse(BaseModel):
    """Flattened, model-ready context for a single service."""
    service: str
    environment: str
    context: dict[str, object]
    had_metric_errors: bool
    had_log_errors: bool


class BatchContextResponse(BaseModel):
    """Contexts for every discovered (or requested) service, plus any that failed outright."""
    environment: str
    window_seconds: int
    results: dict[str, ContextResponse]
    failed_services: list[str]

