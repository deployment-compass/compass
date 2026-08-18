"""
PrometheusAdapter — the single entry point for the anomaly-detection
platform's metric collection.

Two ways to use it, for two different callers:

  - `query(service, environment, window_seconds)` — matches the
    `PullAdapter` interface your ingestion layer already expects (see
    the original prometheous.py). This is what the Soak-window manager
    / Context Builder calls repeatedly, once per (service, environment),
    to build the context passed to the anomaly-detection model. Returns
    a flat `{metric_name: value}` dict scoped to that one target.

  - `collect(metrics, window)` — fleet-wide: runs the same hybrid
    discovery + recording-rule/fallback logic but returns every target's
    values at once, normalized into MetricSample/CollectionResult. Useful
    for dashboards, debugging, or a batch job rather than the per-service
    soak-window loop.

Both share one internal, lazily-created httpx.AsyncClient. The client is
created on first call and lives for the adapter's lifetime, which
matters because the Context Builder calls `query()` many times across
an open soak window; tearing the connection pool down and rebuilding it
per call would throw away connection reuse for no benefit. Call
`aclose()` once, at process shutdown, to release it — in a FastAPI app
that's the app's lifespan handler, not a per-request context manager
(see fastapi_integration.py).
"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .prom_models import (
    CollectionResult,
    DataSource,
    LabelSchema,
    MetricSample,
    MetricType,
)
from .prom_label_discovery import LabelDiscovery
from .promql_builder import PromQLBuilder
from .prom_recording_rule_resolver import RecordingRuleResolver

ALL_METRICS: tuple[MetricType, ...] = tuple(MetricType)

_TARGET_LABEL_FALLBACKS = ("service", "job", "instance", "handler", "route", "endpoint")


class PrometheusAdapter:
    # Matches the `source` convention your other PullAdapter
    # implementations use (see prometheous.py) for registry/logging.
    source = "prometheus"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 5.0,
        schema_cache_ttl_seconds: int = 300,
        recording_rule_overrides: Optional[dict[MetricType, str]] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._schema_ttl = schema_cache_ttl_seconds
        self._overrides = recording_rule_overrides

        self._client: Optional[httpx.AsyncClient] = None
        self._discovery: Optional[LabelDiscovery] = None
        self._rule_resolver: Optional[RecordingRuleResolver] = None

    # -- lifecycle --------------------------------------------------------

    def _ensure_client(self) -> None:
        """Lazily create the shared client + discovery/resolver on first use."""
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LabelDiscovery(
            self._client, self._base_url, cache_ttl_seconds=self._schema_ttl
        )
        self._rule_resolver = RecordingRuleResolver(
            self._client,
            self._base_url,
            cache_ttl_seconds=self._schema_ttl,
            overrides=self._overrides,
        )

    async def aclose(self) -> None:
        """Release the underlying connection pool. Call once at shutdown."""
        if self._client:
            await self._client.aclose()
            self._client = None
            self._discovery = None
            self._rule_resolver = None

    # -- Context Builder entry point --------------------------------------

    async def query(self, service: str, environment: str, window_seconds: int) -> dict:
        """
        Runs the soak-window metric set (request rate, error rate, p95
        latency, CPU, memory) as instant queries scoped to one service,
        via recording rule when available, else a direct PromQL fallback.
        Returns a flat dict of metric_name -> value (or None if that
        metric had no data) — same shape the ingestion layer's other
        PullAdapters return.
        """
        self._ensure_client()
        schema = await self._discovery.discover()
        window = f"{window_seconds}s"

        tasks = [
            self._query_one(metric, schema, window, service, environment)
            for metric in ALL_METRICS
        ]
        values = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[str, Optional[float]] = {}
        for metric, value in zip(ALL_METRICS, values):
            out[metric.value] = None if isinstance(value, Exception) else value
        return out

    async def _query_one(
        self,
        metric: MetricType,
        schema: LabelSchema,
        window: str,
        service: str,
        environment: str,
    ) -> Optional[float]:
        label_key = self._label_for(metric, schema)
        matchers = self._target_matchers(schema, label_key, service, environment)

        # 1) Primary source: recording rule, scoped to this target.
        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_key)
        if rule_name:
            promql = PromQLBuilder.with_matchers(rule_name, matchers)
            value = await self._run_instant_scalar(promql)
            if value is not None:
                return value
            # Rule exists but no data for this target in-window -> fallback.

        # 2) Fallback: dynamically built raw PromQL, scoped to this target.
        promql = PromQLBuilder.build(metric, schema, window=window, target_matchers=matchers)
        return await self._run_instant_scalar(promql)

    @staticmethod
    def _target_matchers(
        schema: LabelSchema, label_key: str, service: str, environment: str
    ) -> dict[str, str]:
        matchers = {label_key: service}
        if schema.environment_label and environment:
            matchers[schema.environment_label] = environment
        return matchers

    async def _run_instant_scalar(self, promql: str) -> Optional[float]:
        resp = await self._client.get(
            f"{self._base_url}/api/v1/query", params={"query": promql}
        )
        resp.raise_for_status()
        result = resp.json().get("data", {}).get("result", [])
        if not result:
            return None
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            return None

    # -- fleet-wide collection ---------------------------------------------

    async def get_schema(self, force_refresh: bool = False) -> LabelSchema:
        """Expose discovery directly, useful for logging/debugging/tests."""
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)

    async def collect(
        self,
        metrics: tuple[MetricType, ...] = ALL_METRICS,
        window: str = "5m",
    ) -> CollectionResult:
        self._ensure_client()
        schema = await self._discovery.discover()

        raw_results = await asyncio.gather(
            *(self._collect_one(m, schema, window) for m in metrics),
            return_exceptions=True,
        )

        samples: list[MetricSample] = []
        errors: list[str] = []
        for metric, res in zip(metrics, raw_results):
            if isinstance(res, Exception):
                errors.append(f"{metric.value}: {res!r}")
                continue
            samples.extend(res)

        return CollectionResult(architecture=schema.architecture, samples=samples, errors=errors)

    async def _collect_one(
        self, metric: MetricType, schema: LabelSchema, window: str
    ) -> list[MetricSample]:
        label_key = self._label_for(metric, schema)

        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_key)
        if rule_name:
            samples = await self._run_instant_vector(
                rule_name, metric, DataSource.RECORDING_RULE, label_key, schema
            )
            if samples:
                return samples

        promql = PromQLBuilder.build(metric, schema, window=window)
        return await self._run_instant_vector(promql, metric, DataSource.DIRECT_QUERY, label_key, schema)

    @staticmethod
    def _label_for(metric: MetricType, schema: LabelSchema) -> str:
        if metric in (MetricType.CPU_USAGE, MetricType.MEMORY_USAGE):
            return schema.process_group_label
        return schema.http_group_label

    async def _run_instant_vector(
        self, promql: str, metric: MetricType, source: DataSource, label_key: str, schema: LabelSchema
    ) -> list[MetricSample]:
        resp = await self._client.get(
            f"{self._base_url}/api/v1/query", params={"query": promql}
        )
        resp.raise_for_status()
        vector = resp.json().get("data", {}).get("result", [])

        samples: list[MetricSample] = []
        for entry in vector:
            labels = entry.get("metric", {})
            target = self._extract_target(labels, label_key)
            try:
                value = float(entry["value"][1])
            except (KeyError, IndexError, ValueError, TypeError):
                value = None
            
            # Extract optional K8s labels if available in schema
            namespace = labels.get(schema.namespace_label) if schema.namespace_label else None
            pod = labels.get(schema.pod_label) if schema.pod_label else None
            container = labels.get(schema.container_label) if schema.container_label else None
            
            samples.append(
                MetricSample(
                    metric=metric,
                    target=target,
                    value=value,
                    source=source,
                    promql=promql,
                    raw_label=label_key,
                    namespace=namespace,
                    pod=pod,
                    container=container,
                )
            )
        return samples

    @staticmethod
    def _extract_target(labels: dict, label_key: str) -> str:
        if label_key in labels:
            return labels[label_key]
        for fallback in _TARGET_LABEL_FALLBACKS:
            if fallback in labels:
                return labels[fallback]
        for key, value in labels.items():
            if not key.startswith("__"):
                return value
        return "unknown"