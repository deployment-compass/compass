"""
Pure PromQL construction.
Given a discovered LabelSchema and a MetricType, produces the raw PromQL string
to run against /api/v1/query (or /api/v1/query_range).

`target_matchers` lets a caller scope the query down to one target
before it ever hits the wire — e.g. {"service": "checkout-api",
"environment": "prod"} 
"""
from __future__ import annotations

from typing import Optional

from .prom_models import MetricType, LabelSchema

Matchers = Optional[dict[str, str]]


class PromQLBuilder:

    @staticmethod
    def build(
        metric: MetricType,
        schema: LabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        if metric is MetricType.P95_LATENCY:
            return PromQLBuilder._p95_latency(schema, window, target_matchers)
        if metric is MetricType.ERROR_RATE:
            return PromQLBuilder._error_rate(schema, window, target_matchers)
        if metric is MetricType.REQUEST_RATE:
            return PromQLBuilder._request_rate(schema, window, target_matchers)
        if metric is MetricType.CPU_USAGE:
            return PromQLBuilder._cpu_usage(schema, window, target_matchers)
        if metric is MetricType.MEMORY_USAGE:
            return PromQLBuilder._memory_usage(schema, window, target_matchers)
        raise ValueError(f"Unsupported metric type: {metric!r}")

    @staticmethod
    def build_range(
        metric: MetricType,
        schema: LabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        """
        Same PromQL as `build`, intended for /api/v1/query_range callers
        (e.g. trend/slope inputs like a memory_trend recording rule). The
        instant-vs-range distinction lives in the query params
        (start/end/step), not the expression, so this just re-exposes
        `build` under an explicit name for range callers.
        """
        return PromQLBuilder.build(metric, schema, window=window, target_matchers=target_matchers)

    @staticmethod
    def with_matchers(metric_name: str, target_matchers: Matchers = None) -> str:
        """
        Scope an arbitrary metric name (including a resolved recording
        rule name) down to a target — used by the adapter's per-service
        `query()` path to filter e.g.
        `test_compass:service:p95_latency_seconds{service="checkout-api"}`
        instead of pulling every service's value.
        """
        return PromQLBuilder._selector(metric_name, "", target_matchers)

    # -- individual builders -------------------------------------------------

    @staticmethod
    def _p95_latency(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        label = schema.http_group_label
        selector = PromQLBuilder._selector(
            "http_request_duration_seconds_bucket", "", matchers
        )
        return (
            f"histogram_quantile(0.95, "
            f"sum(rate({selector}[{window}])) by ({label}, le))"
        )

    @staticmethod
    def _error_rate(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        label = schema.http_group_label
        error_selector = PromQLBuilder._selector("http_requests_total", 'status=~"5.."', matchers)
        total_selector = PromQLBuilder._selector("http_requests_total", "", matchers)
        return (
            f"sum(rate({error_selector}[{window}])) by ({label}) "
            f"/ "
            f"sum(rate({total_selector}[{window}])) by ({label})"
        )

    @staticmethod
    def _request_rate(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        label = schema.http_group_label
        selector = PromQLBuilder._selector("http_requests_total", "", matchers)
        return f"sum(rate({selector}[{window}])) by ({label})"

    @staticmethod
    def _cpu_usage(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        label = schema.process_group_label
        is_container = schema.cpu_metric == "container_cpu_usage_seconds_total"
        base = 'container!=""' if is_container else ""
        selector = PromQLBuilder._selector(schema.cpu_metric, base, matchers)
        return f"sum(rate({selector}[{window}])) by ({label})"

    @staticmethod
    def _memory_usage(schema: LabelSchema, window: str, matchers: Matchers) -> str:  # noqa: ARG004
        label = schema.process_group_label
        is_container = schema.memory_metric == "container_memory_working_set_bytes"
        base = 'container!=""' if is_container else ""
        selector = PromQLBuilder._selector(schema.memory_metric, base, matchers)
        return f"sum({selector}) by ({label})"

    # -- selector assembly ----------------------------------------------

    @staticmethod
    def _selector(metric_name: str, base_matchers: str, extra: Matchers) -> str:
        """Build `metric_name{base_matchers, extra...}`, omitting empty braces."""
        parts = [p for p in (base_matchers,) if p]
        if extra:
            parts.append(", ".join(f'{k}="{v}"' for k, v in extra.items()))
        body = ", ".join(parts)
        return f"{metric_name}{{{body}}}" if body else metric_name