"""Schemas for opening an investigation window and publishing its context."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class InvestigationEnvelope(BaseModel):
    """Context assembled by the Investigation Window for Layer 2 detection.

    The collector payload is intentionally source-neutral and extensible: it
    can include metrics, logs, Kubernetes state, deployment details, and
    future pull-adapter output without requiring a webhook change.
    """

    service: str
    environment: str = "prod"
    investigation_id: str | None = None
    correlation_id: UUID | None = None
    deployment_id: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class InvestigationWindowRequest(BaseModel):
    """A request to collect context for a newly opened investigation window.

    The caller supplies the target and optional correlation metadata only.
    Context is collected by Compass after the window opens.
    """

    service: str
    environment: str = "prod"
    window_seconds: int = Field(default=300, gt=0, le=3600)
    investigation_id: str | None = None
    correlation_id: UUID | None = None
    deployment_id: str | None = None

    model_config = ConfigDict(extra="allow")
