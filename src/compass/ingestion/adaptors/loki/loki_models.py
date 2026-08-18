"""
Typed interfaces for the log-signal side of the collector, mirroring
compass_metrics/models.py but for LogQL/Loki-derived signals instead of
PromQL/Prometheus metrics. Kept separate from compass_metrics.models
because these are semantically different things (a count of matching log
lines, not a metric sample) even though the shapes rhyme.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class LogSignalType(str, Enum):
    """
    The log-derived signals the anomaly-detection platform's Layer 2
    checks consume, per test_compass-log-recording-rules /
    test_compass-log-alert-rules. Exception/fatal rate have recording
    rules in the uploaded ruleset; OOM/dependency-error signals are
    alert-only LogQL with no recording rule — a real fallback case, not
    a hypothetical one.
    """
    EXCEPTION_RATE = "log_exception_rate"
    FATAL_RATE = "log_fatal_rate"
    OOM_SIGNAL = "oom_log_signal"
    DEPENDENCY_CONNECTION_ERRORS = "dependency_connection_errors"



@dataclass(frozen=True)
class LogLabelSchema:
    """What this Loki instance's log streams are actually labeled with."""
    group_label: str          # e.g. "service", "app", "job"
    stream_selector_base: str  # e.g. 'job=~".+"' — the base stream matcher recording
                                # rules in this ruleset are built on

