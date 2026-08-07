"""
Common envelope wrapping every message on the bus.

correlation_id is stamped once at ingestion and carried through every
downstream message.
incident_id stays null until the Incident Manager opens one,
then gets attached to every later message for that incident.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field


from pydantic import BaseModel, ConfigDict

class Envelope(BaseModel):
    correlation_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    incident_id: uuid.UUID | None = None
    deployment_id: uuid.UUID | None = None
    service: str
    environment: str
    emitted_by: str
    emitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any]

    model_config = ConfigDict(
        json_encoders={
            uuid.UUID: str,
            datetime: lambda dt: dt.isoformat(),
        }
    )