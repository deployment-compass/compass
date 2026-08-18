"""Typed, environment-neutral Prometheus collection models."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ArchitectureMode(str, Enum):
    MICROSERVICE = "microservice"
    MONOLITH = "monolith"
    UNKNOWN = "unknown"


class MetricType(str, Enum):
    REQUEST_RATE = "request_rate"
    ERROR_RATE = "error_rate"
    P95_LATENCY = "p95_latency"
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    MEMORY_USAGE_PERCENT = "memory_usage_percent"
    DISK_USAGE_PERCENT = "disk_usage_percent"


class DataSource(str, Enum):
    RECORDING_RULE = "recording_rule"
    DIRECT_QUERY = "direct_query"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class LabelSchema:
    """Labels and CPU/memory metric families discovered at runtime."""
    architecture: ArchitectureMode
    http_group_label: str
    process_group_label: str
    cpu_metric: str
    memory_metric: str
    environment_label: Optional[str] = None
    namespace_label: Optional[str] = None
    pod_label: Optional[str] = None
    container_label: Optional[str] = None
    # -- percentage-metric support --
    memory_limit_metric: Optional[str] = None   # container_spec_memory_limit_bytes; denominator when memory_metric is container-scoped
    memory_total_metric: Optional[str] = None   # node_memory_MemTotal_bytes; denominator when memory_metric is node/process-scoped
    disk_metric: Optional[str] = None           # container_fs_usage_bytes OR node_filesystem_avail_bytes
    disk_pair_metric: Optional[str] = None      # container_fs_limit_bytes OR node_filesystem_size_bytes (paired with disk_metric)
    disk_mountpoint_label: Optional[str] = None # "mountpoint" if node-level filesystem metrics were found and the label is populated; else None


@dataclass
class MetricSample:
    metric: MetricType
    target: str
    value: Optional[float]
    source: DataSource
    promql: str
    raw_label: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    namespace: Optional[str] = None
    pod: Optional[str] = None
    container: Optional[str] = None


@dataclass
class CollectionResult:
    architecture: ArchitectureMode
    samples: list[MetricSample]
    errors: list[str] = field(default_factory=list)

    def to_normalized_dict(self) -> dict[str, dict[str, Optional[float]]]:
        out: dict[str, dict[str, Optional[float]]] = {}
        for sample in self.samples:
            out.setdefault(sample.target, {})[sample.metric.value] = sample.value
        return out


@dataclass
class MetricsContext:
    """Common model input; service/environment are primary, K8s fields enrich it."""
    service: str
    environment: str
    request_rate: Optional[float] = None
    error_rate: Optional[float] = None
    p95_latency: Optional[float] = None
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    memory_usage_percent: Optional[float] = None
    disk_usage_percent: Optional[float] = None
    namespace: Optional[str] = None
    pod: Optional[str] = None
    container: Optional[str] = None
    architecture: ArchitectureMode = ArchitectureMode.UNKNOWN
    collected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))