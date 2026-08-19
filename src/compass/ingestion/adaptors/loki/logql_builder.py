"""Pure LogQL construction.

This module is stateless and has no I/O — it just turns Python objects
into LogQL strings. Keeping it separate from LokiAdaptor makes it easy
to unit-test query generation without mocking HTTP.
"""

from __future__ import annotations

from typing import Optional

from .loki_models import LogSignalType, LogLabelSchema

# Type alias: a dict of label matchers like {"service": "payments", "env": "prod"}
Matchers = Optional[dict[str, str]]

# ---------------------------------------------------------------------------
# Line-filter templates
# ---------------------------------------------------------------------------
# Each signal maps to a LogQL line-filter expression. These are the
# *content* predicates — they run after the stream selector has narrowed
# us to the right log streams.
#
# Why regex (~) for some and literal (=) for others?
# - |= "Traceback" is exact and cheap; Python tracebacks always contain
#   that literal string.
# - |~ is required when the keyword may appear in varying case or
#   phrasing (e.g. "Out of memory", "OOMKilled", "out of memory").
_LINE_FILTERS: dict[LogSignalType, str] = {
    LogSignalType.EXCEPTION_RATE: '|= "Traceback"',
    LogSignalType.FATAL_RATE: '|~ "(?i)fatal|panic"',
    LogSignalType.OOM_SIGNAL: '|~ "(?i)out of memory|oom"',
    LogSignalType.DEPENDENCY_CONNECTION_ERRORS: (
        '|~ "(?i)connection refused|connection reset|could not connect"'
    ),
}


class LogQLBuilder:
    """Factory for LogQL strings used by the adapter."""

    @staticmethod
    def build(
        signal: LogSignalType,
        schema: LogLabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        """Build a raw count_over_time query for a single signal.

        The generated query looks like:
            sum by (<group_label>) (
                count_over_time(
                    {<stream_selector_base>, <target_matchers>}
                    <line_filter>
                    [<window>]
                )
            )

        Args:
            signal: Which anomaly signal to count.
            schema: Discovered label schema (provides group_label and
                stream_selector_base).
            window: LogQL range vector duration, e.g. "5m" or "1h".
            target_matchers: Optional extra label matchers to scope the
                query to a specific service or environment.

        Returns:
            A complete LogQL expression string.
        """
        # Step 1: Build the stream selector inside the curly braces.
        selector = LogQLBuilder._stream_selector(schema, target_matchers)

        # Step 2: Pick the line filter that corresponds to this signal.
        line_filter = _LINE_FILTERS[signal]

        # Step 3: The label we aggregate by must match the discovered
        # group_label, otherwise Loki will return one series per unique
        # combination of labels instead of one series per service.
        label = schema.group_label

        return f'sum by ({label}) (count_over_time({{{selector}}} {line_filter} [{window}]))'

    @staticmethod
    def with_matchers(rule_name: str, target_matchers: Matchers = None) -> str:
        """Scope a resolved recording-rule metric name down to one target.

        When a recording rule exists, we don't rebuild the raw LogQL;
        we just query the pre-aggregated metric name and optionally add
        label matchers. This mirrors PromQLBuilder.with_matchers in the
        Prometheus adapter so that both metric and log adapters behave
        the same way from the caller's perspective.

        Example:
            with_matchers("log_exception_rate_5m", {"service": "payments"})
            -> 'log_exception_rate_5m{service="payments"}'
        """
        if not target_matchers:
            return rule_name
        matcher_str = ", ".join(f'{k}="{v}"' for k, v in target_matchers.items())
        return f"{rule_name}{{{matcher_str}}}"

    @staticmethod
    def _stream_selector(schema: LogLabelSchema, target_matchers: Matchers) -> str:
        """Assemble the comma-separated label matchers for the {...} clause.

        Order matters for cacheability in Loki, but since we always emit
        the same order for identical inputs, we get stable query strings.
        """
        parts = [schema.stream_selector_base] if schema.stream_selector_base else []
        if target_matchers:
            parts.append(", ".join(f'{k}="{v}"' for k, v in target_matchers.items()))
        return ", ".join(p for p in parts if p)