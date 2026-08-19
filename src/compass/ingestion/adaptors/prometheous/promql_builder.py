"""Pure PromQL construction.

Given a discovered LabelSchema and a MetricType, produces the raw PromQL
string to run against /api/v1/query (or /api/v1/query_range).

`target_matchers` lets a caller scope the query down to one target
before it ever hits the wire — e.g. {"service": "checkout-api",
"environment": "prod"} — which keeps result payloads small and
avoids aggregating across targets the caller doesn't care about.

This module is intentionally stateless: all methods are @staticmethod.
The only "configuration" comes from the LabelSchema passed in at call
time, making the builder trivial to test and reuse across threads.
"""

from __future__ import annotations

from typing import Optional

from .prom_models import MetricType, LabelSchema

# Convenience alias: None means "no extra filtering".
Matchers = Optional[dict[str, str]]


class PromQLBuilder:
    """
    Stateless factory for PromQL expressions.

    Each `MetricType` maps to a dedicated `_<metric>_` builder method.
    The public API (`build`, `build_range`, `with_matchers`) is a thin
    dispatch layer on top of those builders.
    """

    @staticmethod
    def build(
        metric: MetricType,
        schema: LabelSchema,
        window: str = "5m",
        target_matchers: Matchers = None,
    ) -> str:
        """
        Build an instant-query PromQL expression for `metric`.

        `window` is the look-back duration used in `rate(...)[window]`
        or `sum_over_time(...)[window]` calls.  Default 5m balances
        responsiveness vs. noise for typical scrape intervals (15-60s).
        """
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
            return PromQLBuilder._memory_usage_percent(schema, window, target_matchers)
        if metric is MetricType.DISK_USAGE_PERCENT:
            return PromQLBuilder._disk_usage_percent(schema, window, target_matchers)
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
        (e.g. trend/slope inputs like a memory_trend recording rule).

        The instant-vs-range distinction lives entirely in the HTTP query
        parameters (`start`, `end`, `step`), not the expression itself.
        This method re-exposes `build` under an explicit name so that
        callers reading the adapter code can see at a glance whether
        they're building an instant or range query.
        """
        return PromQLBuilder.build(metric, schema, window=window, target_matchers=target_matchers)

    @staticmethod
    def with_matchers(metric_name: str, target_matchers: Matchers = None) -> str:
        """
        Scope an arbitrary metric name (including a resolved recording
        rule name) down to a specific target.

        Used by the adapter's per-service `query()` path to filter e.g.
        `test_compass:service:p95_latency_seconds{service="checkout-api"}`
        instead of pulling every service's value.

        Unlike `build`, this does NOT wrap the metric in `rate()` or
        `sum()` — it just appends label selectors.
        """
        return PromQLBuilder._selector(metric_name, "", target_matchers)

    # -----------------------------------------------------------------
    # Individual metric builders — one per MetricType
    # -----------------------------------------------------------------

    @staticmethod
    def _p95_latency(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        """
        Build a histogram_quantile(0.95, ...) expression over
        http_request_duration_seconds_bucket.

        Assumes the standard Prometheus histogram convention:
        `_bucket` series with a `le` label.  Aggregates by the
        discovered HTTP grouping label (e.g. "service") AND `le` so
        that histogram_quantile can reconstruct the distribution per
        service.
        """
        label = schema.http_group_label
        # Base selector for the histogram bucket metric.
        selector = PromQLBuilder._selector(
            "http_request_duration_seconds_bucket", "", matchers
        )
        return (
            f"histogram_quantile(0.95, "
            f"sum(rate({selector}[{window}])) by ({label}, le))"
        )

    @staticmethod
    def _error_rate(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        """
        Build a ratio: 5xx errors / total requests.

        Uses two selectors over `http_requests_total`:
          • error_selector filters status=~"5.." (any 5xx code)
          • total_selector has no status filter

        Both are summed and rate'd over `window`, then divided.
        The `by ({label})` ensures the division aligns on the same
        grouping key (e.g. per-service).
        """
        label = schema.http_group_label
        # Match only 5xx status codes for the numerator.
        error_selector = PromQLBuilder._selector("http_requests_total", 'status=~"5.."', matchers)
        # Match all status codes for the denominator.
        total_selector = PromQLBuilder._selector("http_requests_total", "", matchers)
        return (
            f"sum(rate({error_selector}[{window}])) by ({label}) "
            f"/ "
            f"sum(rate({total_selector}[{window}])) by ({label})"
        )

    @staticmethod
    def _request_rate(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        """
        Build a simple requests-per-second expression:
        sum(rate(http_requests_total[window])) by (group_label).
        """
        label = schema.http_group_label
        selector = PromQLBuilder._selector("http_requests_total", "", matchers)
        return f"sum(rate({selector}[{window}])) by ({label})"

    @staticmethod
    def _cpu_usage(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        """
        Build CPU usage in cores consumed.

        The raw CPU counters are sufficient to calculate usage.  Do not
        divide container usage by quota/period metrics: those cAdvisor
        metrics are optional, and vector division returns an empty result
        when either side is unavailable.  Returning cores also keeps this
        metric consistent across container and process deployments.
        """
        label = schema.process_group_label
        cpu_metric = schema.cpu_metric
        selector = PromQLBuilder._selector(cpu_metric, "", matchers)
        return f"sum(rate({selector}[{window}])) by ({label})"

    @staticmethod
    def _memory_usage(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        """
        Build a memory usage expression.

        For container metrics we return working-set bytes directly.
        For node metrics we compute used = total - available so that
        the number is comparable (higher = more pressure).
        """
        mem_metric = schema.memory_metric
        label = schema.process_group_label

        if "container_memory" in mem_metric:
            # Container memory is already a gauge of bytes in use.
            selector = PromQLBuilder._selector(mem_metric, "", matchers)
            return f"sum({selector}) by ({label})"
        elif "node_memory" in mem_metric:
            # Node memory: we want "used", but many node exporters only
            # expose "available".  Derive used = total - available.
            total_selector = PromQLBuilder._selector(
                schema.memory_total_metric or "node_memory_MemTotal_bytes", "", matchers
            )
            avail_selector = PromQLBuilder._selector(mem_metric, "", matchers)
            return f"sum({total_selector}) by ({label}) - sum({avail_selector}) by ({label})"
        else:
            # Process-level RSS — straightforward gauge.
            selector = PromQLBuilder._selector(mem_metric, "", matchers)
            return f"sum({selector}) by ({label})"

    @staticmethod
    def _memory_usage_percent(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        mem_metric = schema.memory_metric
        label = schema.process_group_label

        if "container_memory" in mem_metric:
            usage = PromQLBuilder._selector(mem_metric, "", matchers)
            limit = PromQLBuilder._selector(schema.memory_limit_metric or "container_spec_memory_limit_bytes", "", matchers)
            return f"sum({usage}) by ({label}) / sum({limit} > 0) by ({label})"
        elif "node_memory" in mem_metric:
            total = PromQLBuilder._selector(schema.memory_total_metric or "node_memory_MemTotal_bytes", "", matchers)
            avail = PromQLBuilder._selector(mem_metric, "", matchers)
            return f"(sum({total}) by ({label}) - sum({avail}) by ({label})) / sum({total} > 0) by ({label})"
        else:
            usage = PromQLBuilder._selector(mem_metric, "", matchers)
            total = PromQLBuilder._selector(schema.memory_total_metric or "node_memory_MemTotal_bytes", "", matchers)
            return f"sum({usage}) by ({label}) / sum({total} > 0) by ({label})"

    @staticmethod
    def _disk_usage_percent(schema: LabelSchema, window: str, matchers: Matchers) -> str:
        disk_metric = schema.disk_metric
        disk_pair = schema.disk_pair_metric
        label = schema.process_group_label

        if not disk_metric or not disk_pair:
            return 'vector(0) * 0'  # Gracefully return no data

        if "container_fs" in disk_metric:
            usage = PromQLBuilder._selector(disk_metric, "", matchers)
            limit = PromQLBuilder._selector(disk_pair, "", matchers)
            return f"sum({usage}) by ({label}) / sum({limit} > 0) by ({label})"
        else:
            mp = schema.disk_mountpoint_label
            mp_filter = f'{mp}="/"' if mp else ""
            avail = PromQLBuilder._selector(disk_metric, mp_filter, matchers)
            size = PromQLBuilder._selector(disk_pair, mp_filter, matchers)
            return f"(sum({size}) by ({label}) - sum({avail}) by ({label})) / sum({size} > 0) by ({label})"

    # -----------------------------------------------------------------
    # Shared helper: append label selectors to a metric name
    # -----------------------------------------------------------------

    @staticmethod
    def _selector(metric_name: str, extra: str, matchers: Matchers) -> str:
        """
        Construct `metric_name{extra, matchers...}`.

        `extra` is a raw PromQL label matcher string (e.g. 'status=~"5.."')
        that is always included.  `matchers` are the caller-supplied
        target filters (e.g. {"service": "checkout-api"}).

        Returns the bare metric name if both `extra` and `matchers` are empty.
        """
        parts: list[str] = []
        if extra:
            parts.append(extra)
        if matchers:
            for k, v in matchers.items():
                # Simple equality matcher.  If you need regex matchers,
                # extend this helper or pre-escape values.
                parts.append(f'{k}="{v}"')

        if not parts:
            return metric_name

        return f"{metric_name}{{{','.join(parts)}}}"
