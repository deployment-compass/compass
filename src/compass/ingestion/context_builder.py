"""
Context Builder — orchestrates metric and log collection for Layer 3 (AI reasoning).

Responsible for:
  1. Pulling metrics from PrometheusAdapter for a given service/environment
  2. Pulling logs from LokiAdapter for the soak window
  3. Normalizing metrics into MetricsContext (service + environment + metrics + optional K8s context)
  4. Returning a complete, normalized context ready for the anomaly-detection model

Service/environment are the primary logical identity. K8s labels (namespace, pod, container)
are optional enrichment when available.

This module does NOT assume any specific deployment topology — it works equally well with:
  - Local Node Exporter (VM/bare metal)
  - Docker Compose with process metrics
  - Kubernetes with container metrics + K8s labels
"""
from __future__ import annotations

from typing import Optional
from dataclasses import dataclass

from compass.ingestion.adaptors.prometheous.prometheous import PrometheusAdapter
from compass.ingestion.adaptors.loki import LokiAdaptor
from compass.ingestion.adaptors.prometheous.prom_models import MetricType, MetricsContext


@dataclass
class BuilderResult:
    """Complete context for a single service, ready for Layer 3."""
    # Metrics (primary input for anomaly detection model)
    metrics: MetricsContext
    
    # Logs (optional enrichment for root-cause analysis)
    log_lines: list[str]
    
    # Metadata about data availability
    had_metric_errors: bool = False
    had_log_errors: bool = False


class ContextBuilder:
    """
    Pulls and normalizes observability data (metrics + logs) for a service.
    
    Uses dependency injection for adapters so it's easy to test or swap
    implementations (e.g., mock adapters for unit tests).
    """

    def __init__(
        self,
        prometheus_adapter: PrometheusAdapter,
        loki_adapter: Optional[LokiAdaptor] = None,
    ):
        self._prometheus = prometheus_adapter
        self._loki = loki_adapter

    async def build(
        self,
        service: str,
        environment: str,
        window_seconds: int = 300,  # Default 5min soak window
    ) -> BuilderResult:
        """
        Collects and normalizes context for a single service.
        
        Args:
            service: The service name (e.g., "checkout-api")
            environment: The environment (e.g., "prod")
            window_seconds: The lookback window for metrics/logs (default 5 min)
        
        Returns:
            BuilderResult with normalized MetricsContext and log lines.
            Never raises — missing data is encoded as None values or empty lists.
        """
        # Collect metrics (never raises, returns dict with None for missing metrics)
        metrics_dict = await self._prometheus.query(service, environment, window_seconds)
        
        # Collect logs (optional)
        log_lines: list[str] = []
        had_log_errors = False
        if self._loki:
            try:
                loki_result = await self._loki.query(service, environment, window_seconds)
                log_lines = loki_result.get("lines", [])
            except Exception:
                had_log_errors = True
                log_lines = []
        
        # Build normalized MetricsContext
        schema = await self._prometheus.get_schema()
        metrics_context = MetricsContext(
            service=service,
            environment=environment,
            request_rate=metrics_dict.get(MetricType.REQUEST_RATE.value),
            error_rate=metrics_dict.get(MetricType.ERROR_RATE.value),
            p95_latency=metrics_dict.get(MetricType.P95_LATENCY.value),
            cpu_usage=metrics_dict.get(MetricType.CPU_USAGE.value),
            memory_usage=metrics_dict.get(MetricType.MEMORY_USAGE.value),
            architecture=schema.architecture,
        )
        
        return BuilderResult(
            metrics=metrics_context,
            log_lines=log_lines,
            had_metric_errors=False,  # PrometheusAdapter.query() never raises
            had_log_errors=had_log_errors,
        )

    async def build_with_k8s_enrichment(
        self,
        service: str,
        environment: str,
        window_seconds: int = 300,
    ) -> BuilderResult:
        """
        Same as build(), but also extracts K8s labels (namespace, pod, container)
        from the metric samples and includes them in the MetricsContext.
        
        This is useful when you want the full K8s context for debugging or
        root-cause analysis. For most Layer 3 use cases, the basic build()
        is sufficient — the model only needs service/environment + metrics.
        """
        result = await self.build(service, environment, window_seconds)
        
        # Optionally enrich with K8s labels by re-running with full discovery
        # (This is a placeholder for future enhancement where we also call
        # the collect() method and extract per-target K8s metadata.)
        
        return result
