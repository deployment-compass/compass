"""
Strongly typed interfaces shared across the promtheous adapter package.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ArchitectureMode(str, Enum):
    """What kind of deployment topology this Prometheus instance is scraping."""
    MICROSERVICE = "microservice"
    MONOLITH = "monolith"
    UNKNOWN = "unknown"


class MetricType(str, Enum):
    """The 5 core metrics the anomaly-detection platform consumes."""
    REQUEST_RATE = "request_rate"
    ERROR_RATE = "error_rate"
    P95_LATENCY = "p95_latency"
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"


class DataSource(str, Enum):
    """Where a given sample's value actually came from."""
    RECORDING_RULE = "recording_rule"
    DIRECT_QUERY = "direct_query"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class LabelSchema:
    """
    The result of dynamic discovery: which labels/metric-families this
    particular Prometheus instance actually populates. Everything
    downstream (query builder, adapter) is parameterized by this instead
    of hardcoding label names.
    """
    architecture: ArchitectureMode
    http_group_label: str        # e.g. "service", "handler", "route", "job"
    process_group_label: str     # e.g. "service", "job", "instance"
    cpu_metric: str              # e.g. "container_cpu_usage_seconds_total"
    memory_metric: str           # e.g. "container_memory_working_set_bytes"
    environment_label: Optional[str] = None  # e.g. "environment", "env" — None if unused


@dataclass
class MetricSample:
    """A single normalized (metric, target) -> value reading."""
    metric: MetricType
    target: str                       # the grouping label's value (service/job/etc name)
    value: Optional[float]
    source: DataSource
    promql: str
    raw_label: str = ""               # which label key produced `target`
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class CollectionResult:
    """Everything gathered in one collection cycle, plus partial-failure info."""
    architecture: ArchitectureMode
    samples: list[MetricSample]
    errors: list[str] = field(default_factory=list)

    def to_normalized_dict(self) -> dict[str, dict[str, Optional[float]]]:
        """
        Flatten into `{target: {metric_name: value}}`, the shape the
        anomaly-detection layer actually wants — independent of which
        source or label produced each sample.
        """
        out: dict[str, dict[str, Optional[float]]] = {}
        for s in self.samples:
            out.setdefault(s.target, {})[s.metric.value] = s.value
        return out