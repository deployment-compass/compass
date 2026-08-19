"""Typed, environment-neutral Prometheus collection models.

This module defines the pure-data structures used throughout the
prometheus-collection stack.  Keeping models in a separate module
lets the discovery, query-building, and adapter layers share a
common vocabulary without creating circular imports.

All classes are designed to be serialisation-friendly (dataclasses
with primitive types) so they can cross process / network boundaries
easily.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ArchitectureMode(str, Enum):
    """Runtime-detected deployment style.

    The discovery layer looks at which metric families are present
    (container_* vs. node_* / process_*) to decide whether we are
    talking to a Kubernetes cluster (microservice) or a bare-metal /
    VM host (monolith).  This enum drives downstream decisions such
    as which labels to group by and whether pod/namespace metadata
    is expected.
    """
    MICROSERVICE = "microservice"
    MONOLITH = "monolith"
    UNKNOWN = "unknown"


class MetricType(str, Enum):
    """Canonical metric kinds the adapter can collect.

    Each variant maps to exactly one builder method in PromQLBuilder
    and one keyword set in RecordingRuleResolver.
    """
    REQUEST_RATE = "request_rate"          # RPS
    ERROR_RATE = "error_rate"              # 5xx / total
    P95_LATENCY = "p95_latency"            # histogram_quantile(0.95, ...)
    CPU_USAGE = "cpu_usage"                # % or seconds
    MEMORY_USAGE = "memory_usage"          # raw bytes
    MEMORY_USAGE_PERCENT = "memory_usage_percent"  # used / limit (or total)
    DISK_USAGE_PERCENT = "disk_usage_percent"      # used / size (or avail / size)


class DataSource(str, Enum):
    """Where a concrete sample came from.

    Helps callers decide whether to trust a value (recording rules
    may lag) and whether to fall back to a direct PromQL query when
    a rule is missing.
    """
    RECORDING_RULE = "recording_rule"  # Pre-aggregated metric name
    DIRECT_QUERY = "direct_query"      # Raw PromQL we built on the fly
    UNAVAILABLE = "unavailable"        # Query returned no data


@dataclass(frozen=True)
class LabelSchema:
    """Labels and CPU/memory metric families discovered at runtime.

    A single `LabelSchema` instance captures everything the
    PromQLBuilder needs to know about *this* Prometheus instance:
    which labels exist, which metric prefixes are in use, and what
    the deployment architecture looks like.

    The fields are intentionally Optional because discovery is best-
    effort: if a cluster has no disk metrics, `disk_metric` will be
    None and the adapter will skip disk collection rather than crash.
    """
    architecture: ArchitectureMode
    http_group_label: str      # e.g. "service", "app", "job"
    process_group_label: str   # e.g. "service", "job", "instance"
    cpu_metric: str            # e.g. "container_cpu_usage_seconds_total"
    memory_metric: str         # e.g. "container_memory_working_set_bytes"
    environment_label: Optional[str] = None   # e.g. "environment", "env"
    namespace_label: Optional[str] = None     # e.g. "namespace"
    pod_label: Optional[str] = None           # e.g. "pod"
    container_label: Optional[str] = None     # e.g. "container"
    # -- percentage-metric support --
    # Denominator for container-scoped memory percentages.
    memory_limit_metric: Optional[str] = None   # "container_spec_memory_limit_bytes"
    # Denominator for node/process-scoped memory percentages.
    memory_total_metric: Optional[str] = None   # "node_memory_MemTotal_bytes"
    # Primary disk metric (usage or avail).
    disk_metric: Optional[str] = None           # "container_fs_usage_bytes" or "node_filesystem_avail_bytes"
    # Paired total/limit metric so we can compute a percentage.
    disk_pair_metric: Optional[str] = None      # "container_fs_limit_bytes" or "node_filesystem_size_bytes"
    # Node-level filesystem metrics need a mountpoint filter so we
    # don't sum across every mounted volume on a multi-disk host.
    disk_mountpoint_label: Optional[str] = None # "mountpoint"


@dataclass
class MetricSample:
    """A single scalar value returned by Prometheus for one target.

    `raw_label` preserves the original label value returned by
    Prometheus (e.g. "checkout-api") so that callers can correlate
    samples even if they rename the target later.
    """
    metric: MetricType
    target: str                # Human-readable target name
    value: Optional[float]     # None = query succeeded but returned NaN / no data
    source: DataSource         # recording rule vs. direct query
    promql: str                # The exact expression that produced this value
    raw_label: str = ""        # Original Prometheus label value
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Kubernetes enrichment (only populated in MICROSERVICE mode).
    namespace: Optional[str] = None
    pod: Optional[str] = None
    container: Optional[str] = None


@dataclass
class CollectionResult:
    """The aggregate output of a single collection pass.

    `to_normalized_dict` flattens the list of samples into a
    per-target dictionary that is easy to feed into dashboards or
    ML pipelines.
    """
    architecture: ArchitectureMode
    samples: list[MetricSample]
    errors: list[str] = field(default_factory=list)

    def to_normalized_dict(self) -> dict[str, dict[str, Optional[float]]]:
        """Flatten samples into {target: {metric_type: value}}.

        Missing metrics are simply omitted from the inner dict,
        allowing the consumer to distinguish between "metric not
        collected" and "metric collected but null".
        """
        out: dict[str, dict[str, Optional[float]]] = {}
        for sample in self.samples:
            out.setdefault(sample.target, {})[sample.metric.value] = sample.value
        return out


@dataclass
class MetricsContext:
    """Common model input; service/environment are primary, K8s fields enrich it.

    This is the "request" object that the adapter layer accepts
    from upstream callers.  It carries both the identity of the
    service to collect and the K8s coordinates needed to scope
    container-level queries.
    """
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