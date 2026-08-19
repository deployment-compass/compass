"""Typed log-derived signals and discovered Loki label conventions.

This module is the shared vocabulary of the Loki adapter. Everything
upstream (discovery, rule resolution) and downstream (anomaly models)
agrees on these types, so changing a field here propagates consistently.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime, timezone


class LogSignalType(str, Enum):
    """The four anomaly signals we know how to extract from logs.

    The string values are the keys used in the flat dict returned by
    LokiAdaptor.query(), so they must stay stable — they are part of the
    public API consumed by the anomaly detection model.
    """
    EXCEPTION_RATE = "log_exception_rate"
    FATAL_RATE = "log_fatal_rate"
    OOM_SIGNAL = "oom_log_signal"
    DEPENDENCY_CONNECTION_ERRORS = "dependency_connection_errors"


@dataclass(frozen=True)
class LogLabelSchema:
    """The label convention discovered from a live Loki instance.

    Loki has no enforced schema; different deployments use different
    labels for the same concepts (e.g. "app" vs "app_kubernetes_io_name").
    This struct captures the *actual* labels present on the target instance
    so that the rest of the adapter can construct valid stream selectors.

    Attributes:
        group_label: The label used to identify a service / workload.
            This is the primary grouping key for aggregation queries.
        stream_selector_base: An optional static selector fragment that
            every query should include (e.g. {job=~".+"}). Empty string
            means "no additional scoping".
        environment_label: Label that distinguishes prod/staging/dev.
        namespace_label: Kubernetes namespace label, if present.
        pod_label: Pod name label, if present.
        container_label: Container name label, if present.
    """
    group_label: str
    stream_selector_base: str = ""
    environment_label: str | None = None
    namespace_label: str | None = None
    pod_label: str | None = None
    container_label: str | None = None


@dataclass
class LogSample:
    """A single scalar observation for one (signal, target) pair.

    This is the row type of the vector returned by Loki's instant query
    endpoint. We keep the original logql and raw_label so that debugging
    tools can show exactly which query produced a value.
    """
    signal: LogSignalType
    target: str               # The service / workload name (value of group_label)
    value: Optional[float]    # None if Loki returned a non-numeric or missing value
    source: LogDataSource     # Whether this came from a recording rule or raw LogQL
    logql: str                # The exact query string that was executed
    raw_label: str = ""       # The label key that 'target' was extracted from
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class LogDataSource(str, Enum):
    """Provenance of a LogSample.

    Used by callers to decide whether to trust a sample (recording rules
    are usually more reliable than ad-hoc count_over_time queries) and to
    build debugging UIs.
    """
    RECORDING_RULE = "recording_rule"
    DIRECT_QUERY = "direct_query"
    UNAVAILABLE = "unavailable"


@dataclass
class LogCollectionResult:
    """The aggregate output of LokiAdaptor.collect().

    Unlike query() — which returns a flat dict for a *single* service —
    collect() gathers samples for *all* services visible to Loki. The
    errors list captures per-signal failures without failing the whole
    batch.
    """
    samples: list[LogSample]
    errors: list[str] = field(default_factory=list)

    def to_normalized_dict(self) -> dict[str, dict[str, Optional[float]]]:
        """Pivot samples into a nested dict: target -> signal -> value.

        This is the shape expected by the anomaly model's feature
        extractor. Targets with no samples for a given signal simply
        won't have that key present.
        """
        out: dict[str, dict[str, Optional[float]]] = {}
        for s in self.samples:
            out.setdefault(s.target, {})[s.signal.value] = s.value
        return out