"""
Pure LogQL construction 
"""
from __future__ import annotations

from typing import Optional

from .loki_models import LogSignalType, LogLabelSchema

Matchers = Optional[dict[str, str]]

_LINE_FILTERS: dict[LogSignalType, str] = {
    LogSignalType.EXCEPTION_RATE: '|= "Traceback"',
    LogSignalType.FATAL_RATE: '|~ "(?i)fatal|panic"',
    LogSignalType.OOM_SIGNAL: '|~ "(?i)out of memory|oom"',
    LogSignalType.DEPENDENCY_CONNECTION_ERRORS: (
        '|~ "(?i)connection refused|connection reset|could not connect"'
    ),
}


class LogQLBuilder:

    @staticmethod
    def build(
        signal: LogSignalType,
        schema: LogLabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        selector = LogQLBuilder._stream_selector(schema, target_matchers)
        line_filter = _LINE_FILTERS[signal]
        label = schema.group_label
        return f'sum by ({label}) (count_over_time({{{selector}}} {line_filter} [{window}]))'

    @staticmethod
    def with_matchers(rule_name: str, target_matchers: Matchers = None) -> str:
        """
        Scope a resolved recording-rule metric name down to one target —
        same purpose as PromQLBuilder.with_matchers, for the per-service
        query() path.
        """
        if not target_matchers:
            return rule_name
        matcher_str = ", ".join(f'{k}="{v}"' for k, v in target_matchers.items())
        return f"{rule_name}{{{matcher_str}}}"

    @staticmethod
    def _stream_selector(schema: LogLabelSchema, target_matchers: Matchers) -> str:
        parts = [schema.stream_selector_base] if schema.stream_selector_base else []
        if target_matchers:
            parts.append(", ".join(f'{k}="{v}"' for k, v in target_matchers.items()))
        return ", ".join(p for p in parts if p)
