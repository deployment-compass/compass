"""
Pure PromQL construction.
...
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
        if metric is MetricType.MEMORY_USAGE_PERCENT:
            return PromQLBuilder._memory_usage_percent(schema, target_matchers)
        if metric is MetricType.DISK_USAGE_PERCENT:
            return PromQLBuilder._disk_usage_percent(schema, target_matchers)
        raise ValueError(f"Unsupported metric type: {metric!r}")

    @staticmethod
    def build_range(
        metric: MetricType,
        schema: LabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        return PromQLBuilder.build(metric, schema, window=window, target_matchers=target_matchers)

    @staticmethod
    def with_matchers(metric_name: str, target_matchers: Matchers = None) -> str:
        return PromQLBuilder._selector(metric_name, "", target_matchers)

    # -- individual builders -------------------------------------------------

    @staticmethod
    def _p95_latency(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        label = schema.http_group_label
        selector = PromQLBuilder._selector("http_request_duration_seconds_bucket", "", matchers)
        return f"histogram_quantile(0.95, sum(rate({selector}[{window}])) by ({label}, le))"

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
        selector = PromQLBuilder._selector(schema.cpu_metric, "", matchers)
        if schema.cpu_metric == "node_cpu_seconds_total":
            idle_selector = PromQLBuilder._selector(schema.cpu_metric, 'mode="idle"', matchers)
            return f"(1 - avg(rate({idle_selector}[{window}])) by ({label}))"
        return f"sum(rate({selector}[{window}])) by ({label})"

    @staticmethod
    def _memory_usage(schema: LabelSchema, window: str, matchers: Matchers) -> str:  # noqa: ARG004
        label = schema.process_group_label
        selector = PromQLBuilder._selector(schema.memory_metric, "", matchers)
        if schema.memory_metric == "node_memory_MemAvailable_bytes":
            total_selector = PromQLBuilder._selector("node_memory_MemTotal_bytes", "", matchers)
            return f"sum({total_selector} - {selector}) by ({label})"
        return f"sum({selector}) by ({label})"

    @staticmethod
    def _memory_usage_percent(schema: LabelSchema, matchers: Matchers) -> str:
        """0-100 memory utilization. Formula depends on which memory_metric
        discovery found, mirroring _memory_usage's own family switch."""
        label = schema.process_group_label

        if schema.memory_metric == "container_memory_working_set_bytes":
            if not schema.memory_limit_metric:
                raise ValueError("memory_usage_percent unavailable: no container memory limit metric discovered")
            used = PromQLBuilder._selector(schema.memory_metric, "", matchers)
            # Unbounded containers report a huge sentinel limit rather than 0,
            # so this ratio degrades gracefully (reads ~0%) instead of dividing
            # by zero; no extra filtering needed.
            limit = PromQLBuilder._selector(schema.memory_limit_metric, "", matchers)
            return f"clamp(sum({used}) by ({label}) / sum({limit}) by ({label}) * 100, 0, 100)"

        if schema.memory_metric == "node_memory_MemAvailable_bytes":
            avail = PromQLBuilder._selector(schema.memory_metric, "", matchers)
            total = PromQLBuilder._selector("node_memory_MemTotal_bytes", "", matchers)
            return f"clamp((1 - sum({avail}) by ({label}) / sum({total}) by ({label})) * 100, 0, 100)"

        # process_resident_memory_bytes has no natural % denominator at that
        # granularity; fall back to node-level totals if they were discovered.
        if not schema.memory_total_metric:
            raise ValueError("memory_usage_percent unavailable: no node memory total metric discovered")
        used = PromQLBuilder._selector(schema.memory_metric, "", matchers)
        total = PromQLBuilder._selector(schema.memory_total_metric, "", matchers)
        return f"clamp(sum({used}) by ({label}) / sum({total}) by ({label}) * 100, 0, 100)"

    @staticmethod
    def _disk_usage_percent(schema: LabelSchema, matchers: Matchers) -> str:
        """0-100 filesystem utilization. Container mode uses usage/limit
        directly; node mode uses 1 - avail/size, scoped to a mountpoint so a
        multi-volume host doesn't get its filesystems summed together."""
        label = schema.process_group_label

        if not schema.disk_metric or not schema.disk_pair_metric:
            raise ValueError("disk_usage_percent unavailable: no filesystem metric pair discovered")

        if schema.disk_metric == "container_fs_usage_bytes":
            used = PromQLBuilder._selector(schema.disk_metric, "", matchers)
            limit = PromQLBuilder._selector(schema.disk_pair_metric, "", matchers)
            return f"clamp(sum({used}) by ({label}) / sum({limit}) by ({label}) * 100, 0, 100)"

        # node_filesystem_avail_bytes / node_filesystem_size_bytes.
        # Default to root filesystem; caller can override via target_matchers,
        # e.g. {"mountpoint": "/data"}.
        mountpoint_matchers = dict(matchers or {})
        if schema.disk_mountpoint_label and schema.disk_mountpoint_label not in mountpoint_matchers:
            mountpoint_matchers[schema.disk_mountpoint_label] = "/"

        avail = PromQLBuilder._selector(schema.disk_metric, "", mountpoint_matchers)
        size = PromQLBuilder._selector(schema.disk_pair_metric, "", mountpoint_matchers)
        return f"clamp((1 - sum({avail}) by ({label}) / sum({size}) by ({label})) * 100, 0, 100)"

    # -- selector assembly ----------------------------------------------

    @staticmethod
    def _selector(metric_name: str, base_matchers: str, extra: Matchers) -> str:
        parts = [p for p in (base_matchers,) if p]
        if extra:
            parts.append(", ".join(f'{k}="{v}"' for k, v in extra.items()))
        body = ", ".join(parts)
        return f"{metric_name}{{{body}}}" if body else metric_name