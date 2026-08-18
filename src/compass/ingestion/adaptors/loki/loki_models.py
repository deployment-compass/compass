"""Typed log-derived signals and discovered Loki label conventions."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime, timezone

class LogSignalType(str, Enum):
    EXCEPTION_RATE = "log_exception_rate"
    FATAL_RATE = "log_fatal_rate"
    OOM_SIGNAL = "oom_log_signal"
    DEPENDENCY_CONNECTION_ERRORS = "dependency_connection_errors"


@dataclass(frozen=True)
class LogLabelSchema:
    group_label: str
    stream_selector_base: str = ""
    environment_label: str | None = None
    namespace_label: str | None = None
    pod_label: str | None = None
    container_label: str | None = None



@dataclass
class LogSample:
    signal: LogSignalType
    target: str
    value: Optional[float]
    source: LogDataSource
    logql: str
    raw_label: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
 
class LogDataSource(str, Enum):
    RECORDING_RULE = "recording_rule"
    DIRECT_QUERY = "direct_query"
    UNAVAILABLE = "unavailable"

@dataclass
class LogCollectionResult:
    samples: list[LogSample]
    errors: list[str] = field(default_factory=list)
 
    def to_normalized_dict(self) -> dict[str, dict[str, Optional[float]]]:
        out: dict[str, dict[str, Optional[float]]] = {}
        for s in self.samples:
            out.setdefault(s.target, {})[s.signal.value] = s.value
        return out
 