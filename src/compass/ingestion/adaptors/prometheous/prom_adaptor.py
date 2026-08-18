"""One Prometheus adapter for application and infrastructure metrics."""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .prom_label_discovery import LabelDiscovery
from .prom_models import CollectionResult, DataSource, LabelSchema, MetricSample, MetricType
from .prom_recording_rule_resolver import RecordingRuleResolver
from .promql_builder import PromQLBuilder

ALL_METRICS: tuple[MetricType, ...] = tuple(MetricType)
_TARGET_LABEL_FALLBACKS = ("service", "app", "job", "instance", "handler", "route", "endpoint")


class PrometheusAdapter:
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

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LabelDiscovery(self._client, self._base_url, self._schema_ttl)
        self._rule_resolver = RecordingRuleResolver(
            self._client, self._base_url, self._schema_ttl, self._overrides
        )

    async def aclose(self) -> None:
        if self._client:
            await self._client.aclose()
        self._client = self._discovery = self._rule_resolver = None

    async def get_schema(self, force_refresh: bool = False) -> LabelSchema:
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)

    async def query(self, service: str, environment: str, window_seconds: int) -> dict[str, Optional[float]]:
        """Return the same logical metric keys in local and Kubernetes deployments."""
        schema = await self.get_schema()
        values = await asyncio.gather(
            *(self._query_one(metric, schema, f"{window_seconds}s", service, environment) for metric in ALL_METRICS),
            return_exceptions=True,
        )
        return {
            metric.value: None if isinstance(value, Exception) else value
            for metric, value in zip(ALL_METRICS, values)
        }

    async def _query_one(
        self, metric: MetricType, schema: LabelSchema, window: str, service: str, environment: str
    ) -> Optional[float]:
        label_key = self._label_for(metric, schema)
        matchers = self._target_matchers(schema, label_key, service, environment)
        value = await self._query_with_fallbacks(metric, schema, window, matchers, label_key)
        if value is not None or metric not in (MetricType.CPU_USAGE, MetricType.MEMORY_USAGE):
            return value
        # Node-level metrics are frequently labeled only by instance. Keep this
        # best-effort fallback limited to generic infrastructure measurements.
        relaxed = {key: value for key, value in matchers.items() if key != label_key}
        return await self._query_with_fallbacks(metric, schema, window, relaxed, label_key)

    async def _query_with_fallbacks(
        self, metric: MetricType, schema: LabelSchema, window: str, matchers: dict[str, str], label_hint: str
    ) -> Optional[float]:
        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_hint)
        if rule_name:
            value = await self._run_instant_scalar(PromQLBuilder.with_matchers(rule_name, matchers))
            if value is not None:
                return value
        return await self._run_instant_scalar(PromQLBuilder.build(metric, schema, window, matchers))

    @staticmethod
    def _target_matchers(schema: LabelSchema, label_key: str, service: str, environment: str) -> dict[str, str]:
        matchers = {label_key: service}
        if schema.environment_label and environment:
            matchers[schema.environment_label] = environment
        return matchers

    @staticmethod
    def _label_for(metric: MetricType, schema: LabelSchema) -> str:
        return (
            schema.process_group_label
            if metric in (
                MetricType.CPU_USAGE,
                MetricType.MEMORY_USAGE,
                MetricType.MEMORY_USAGE_PERCENT,
                MetricType.DISK_USAGE_PERCENT,
            )
            else schema.http_group_label
        )
    async def _run_instant_scalar(self, promql: str) -> Optional[float]:
        response = await self._client.get(f"{self._base_url}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        result = response.json().get("data", {}).get("result", [])
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    async def collect(self, metrics: tuple[MetricType, ...] = ALL_METRICS, window: str = "5m") -> CollectionResult:
        """Fleet-wide normalized samples, retaining optional K8s labels when present."""
        schema = await self.get_schema()
        results = await asyncio.gather(*(self._collect_one(metric, schema, window) for metric in metrics), return_exceptions=True)
        samples, errors = [], []
        for metric, result in zip(metrics, results):
            if isinstance(result, Exception):
                errors.append(f"{metric.value}: {result!r}")
            else:
                samples.extend(result)
        return CollectionResult(schema.architecture, samples, errors)

    async def _collect_one(self, metric: MetricType, schema: LabelSchema, window: str) -> list[MetricSample]:
        label_key = self._label_for(metric, schema)
        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_key)
        if rule_name:
            samples = await self._run_instant_vector(rule_name, metric, DataSource.RECORDING_RULE, label_key, schema)
            if samples:
                return samples
        promql = PromQLBuilder.build(metric, schema, window)
        return await self._run_instant_vector(promql, metric, DataSource.DIRECT_QUERY, label_key, schema)

    async def _run_instant_vector(self, promql: str, metric: MetricType, source: DataSource, label_key: str, schema: LabelSchema) -> list[MetricSample]:
        response = await self._client.get(f"{self._base_url}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        samples = []
        for entry in response.json().get("data", {}).get("result", []):
            labels = entry.get("metric", {})
            try:
                value = float(entry["value"][1])
            except (KeyError, IndexError, TypeError, ValueError):
                value = None
            samples.append(MetricSample(metric, self._extract_target(labels, label_key), value, source, promql, label_key,
                namespace=labels.get(schema.namespace_label) if schema.namespace_label else None,
                pod=labels.get(schema.pod_label) if schema.pod_label else None,
                container=labels.get(schema.container_label) if schema.container_label else None))
        return samples

    @staticmethod
    def _extract_target(labels: dict[str, str], label_key: str) -> str:
        for key in (label_key, *_TARGET_LABEL_FALLBACKS):
            if key in labels:
                return labels[key]
        return next((value for key, value in labels.items() if not key.startswith("__")), "unknown")

    async def ping(self) -> bool:
        self._ensure_client()
        response = await self._client.get(f"{self._base_url}/-/healthy")
        response.raise_for_status()
        return True
