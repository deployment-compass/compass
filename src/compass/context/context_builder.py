"""Build the environment-neutral observability context consumed by anomaly detection."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

from compass.ingestion.adaptors.loki.loki_adaptor import LokiAdapter
from compass.ingestion.adaptors.prometheous.prom_adaptor import PrometheusAdapter
from compass.ingestion.adaptors.prometheous.prom_models import ArchitectureMode, MetricType, MetricsContext


@dataclass
class BuilderResult:
    metrics: MetricsContext
    log_signals: dict[str, Optional[float]] = field(default_factory=dict)
    # Retained for callers using the former LokiAdaptor response. New LokiAdapter
    # produces signals rather than shipping unbounded raw logs into model context.
    log_lines: list[str] = field(default_factory=list)
    had_metric_errors: bool = False
    had_log_errors: bool = False

    @property
    def context(self) -> dict[str, object]:
        """Flat, source-neutral input for the anomaly model."""
        return {
            "service": self.metrics.service,
            "environment": self.metrics.environment,
            "request_rate": self.metrics.request_rate,
            "error_rate": self.metrics.error_rate,
            "p95_latency": self.metrics.p95_latency,
            "cpu_usage": self.metrics.cpu_usage,
            "memory_usage": self.metrics.memory_usage,
            "namespace": self.metrics.namespace,
            "pod": self.metrics.pod,
            "container": self.metrics.container,
            **self.log_signals,
        }


class ContextBuilder:
    def __init__(self, prometheus_adapter: PrometheusAdapter, loki_adapter: Optional[LokiAdapter] = None):
        self._prometheus = prometheus_adapter
        self._loki = loki_adapter

    async def build(self, service: str, environment: str, window_seconds: int = 300) -> BuilderResult:
        metric_task = self._prometheus.query(service, environment, window_seconds)
        log_task = self._loki.query(service, environment, window_seconds) if self._loki else None
        results = await asyncio.gather(metric_task, *( [log_task] if log_task else [] ), return_exceptions=True)

        metric_result = results[0]
        had_metric_errors = isinstance(metric_result, Exception)
        metric_values = {} if had_metric_errors else metric_result
        try:
            schema = await self._prometheus.get_schema()
            architecture = schema.architecture
        except Exception:
            architecture = ArchitectureMode.UNKNOWN
            had_metric_errors = True

        log_result = results[1] if log_task else {}
        had_log_errors = isinstance(log_result, Exception)
        log_result = {} if had_log_errors else log_result
        log_lines = log_result.get("lines", [])
        log_signals = {key: value for key, value in log_result.items() if key != "lines"}

        return BuilderResult(
            metrics=MetricsContext(
                service=service,
                environment=environment,
                request_rate=metric_values.get(MetricType.REQUEST_RATE.value),
                error_rate=metric_values.get(MetricType.ERROR_RATE.value),
                p95_latency=metric_values.get(MetricType.P95_LATENCY.value),
                cpu_usage=metric_values.get(MetricType.CPU_USAGE.value),
                memory_usage=metric_values.get(MetricType.MEMORY_USAGE.value),
                architecture=architecture,
            ),
            log_signals=log_signals,
            log_lines=log_lines,
            had_metric_errors=had_metric_errors,
            had_log_errors=had_log_errors,
        )

    async def build_with_k8s_enrichment(self, service: str, environment: str, window_seconds: int = 300) -> BuilderResult:
        """Compatibility alias; optional K8s fields come from Prometheus samples when available."""
        return await self.build(service, environment, window_seconds)
