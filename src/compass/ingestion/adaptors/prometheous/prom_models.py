"""
Strongly typed interfaces shared across the promtheous adapter package.
"""
from __future__ import annotations

from dataclasses import dataclass, field
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

