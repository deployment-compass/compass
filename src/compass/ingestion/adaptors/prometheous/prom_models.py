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
    
    K8s-specific labels are optional and discovered separately:
    - namespace: K8s namespace (discovered if populated)
    - pod: K8s pod name (discovered if populated)
    - container: K8s container name (discovered if populated)
    
    Service/environment remain the primary logical identity regardless
    of environment (Docker, VM, K8s). K8s labels are optional enrichment.
    """
    architecture: ArchitectureMode
    http_group_label: str        # e.g. "service", "handler", "route", "job"
    process_group_label: str     # e.g. "service", "job", "instance"
    cpu_metric: str              # e.g. "container_cpu_usage_seconds_total"
    memory_metric: str           # e.g. "container_memory_working_set_bytes"
    environment_label: Optional[str] = None  # e.g. "environment", "env" — None if unused
    
    # K8s-specific (optional, only if discovered)
    namespace_label: Optional[str] = None    # e.g. "namespace" — None if not available
    pod_label: Optional[str] = None          # e.g. "pod" — None if not available
    container_label: Optional[str] = None    # e.g. "container" — None if not available


@dataclass
class MetricSample:
    """A single normalized (metric, target) -> value reading.
    
    Encodes:
    - Core metric: request_rate, error_rate, p95_latency, cpu_usage, memory_usage
    - Target (primary identity): service/job name (from process_group_label or http_group_label)
    - K8s context (optional enrichment): namespace, pod, container if available
    - Source tracking: recording_rule vs direct_query for debugging/optimization
    """
    metric: MetricType
    target: str                       # the grouping label's value (service/job/etc name)
    value: Optional[float]
    source: DataSource
    promql: str
    raw_label: str = ""               # which label key produced `target`
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # K8s context (optional enrichment, only if discovered in schema)
    namespace: Optional[str] = None   # K8s namespace
    pod: Optional[str] = None         # K8s pod name
    container: Optional[str] = None   # K8s container name


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


@dataclass
class MetricsContext:
    """
    Normalized metrics context for a single service/target.
    
    Sent to the anomaly-detection model as the base input.
    Service and environment are the primary logical identity;
    K8s labels are optional enrichment when available.
    
    All metrics are Optional — a metric may not be available in
    the Prometheus instance, and that's OK (the model handles None).
    """
    service: str                          # Primary identity (from http_group_label)
    environment: str                      # Deployment environment (from environment_label, default "unknown")
    
    # Core metrics (from Layer 3 data flow spec)
    request_rate: Optional[float] = None
    error_rate: Optional[float] = None
    p95_latency: Optional[float] = None
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    
    # K8s enrichment (optional, only if available)
    namespace: Optional[str] = None
    pod: Optional[str] = None
    container: Optional[str] = None
    
    # Metadata
    architecture: ArchitectureMode = ArchitectureMode.UNKNOWN
    collected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))