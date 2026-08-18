"""Typed log-derived signals and discovered Loki label conventions."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


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
