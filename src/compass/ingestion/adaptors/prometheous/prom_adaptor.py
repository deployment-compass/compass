"""One Prometheus adapter for application and infrastructure metrics."""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .prom_label_discovery import LabelDiscovery
from .prom_models import CollectionResult, DataSource, LabelSchema, MetricSample, MetricType
from .prom_recording_rule_resolver import RecordingRuleResolver
from .promql_builder import PromQLBuilder

# Tuple of all supported metric types to query or collect by default
ALL_METRICS: tuple[MetricType, ...] = tuple(MetricType)

# Prioritized list of Prometheus label names used to resolve the target name if the primary label is missing
_TARGET_LABEL_FALLBACKS = ("service", "app", "job", "instance", "handler", "route", "endpoint")

# Infrastructure-level metrics that often lack application/service labels and need query relaxation
_INFRASTRUCTURE_METRICS = frozenset(
    {
        MetricType.CPU_USAGE,
        MetricType.MEMORY_USAGE,
        MetricType.MEMORY_USAGE_PERCENT,
        MetricType.DISK_USAGE_PERCENT,
    }
)


class PrometheusAdapter:
    """Asynchronous adapter for querying and collecting Prometheus application and infrastructure metrics."""
    source = "prometheus"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 5.0,
        schema_cache_ttl_seconds: int = 300,
        recording_rule_overrides: Optional[dict[MetricType, str]] = None,
    ):
        """Store client configuration options without opening connection resources immediately."""
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._schema_ttl = schema_cache_ttl_seconds
        self._overrides = recording_rule_overrides
        
        # Lazy-initialized HTTP client and auxiliary helpers
        self._client: Optional[httpx.AsyncClient] = None
        self._discovery: Optional[LabelDiscovery] = None
        self._rule_resolver: Optional[RecordingRuleResolver] = None

    def _ensure_client(self) -> None:
        """Instantiate the AsyncHTTP client and helper instances lazily on first access."""
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LabelDiscovery(self._client, self._base_url, self._schema_ttl)
        self._rule_resolver = RecordingRuleResolver(
            self._client, self._base_url, self._schema_ttl, self._overrides
        )

    async def aclose(self) -> None:
        """Gracefully close the underlying HTTP client session and reset state."""
        if self._client:
            await self._client.aclose()
        self._client = self._discovery = self._rule_resolver = None

    async def get_schema(self, force_refresh: bool = False) -> LabelSchema:
        """Fetch or refresh the auto-discovered label schema mapping for the Prometheus target."""
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)

    async def query(
        self, service: str, environment: str, window_seconds: int
    ) -> dict[str, object]:
        """Return metric scalars plus a ``_k8s`` key with label metadata.

        The ``_k8s`` dict contains ``namespace``, ``pod``, and ``container``
        extracted from the first matching Prometheus vector result for the CPU
        metric (the most likely to carry container-level labels).  All three
        default to ``None`` when labels are absent (MONOLITH / process-exporter
        setups do not expose these labels).

        Executes concurrent instant queries for every supported metric for a
        specific target service.
        """
        schema = await self.get_schema()
        window = f"{window_seconds}s"

        # Concurrently execute queries for all metrics, safely catching exceptions per query
        values = await asyncio.gather(
            *(self._query_one(metric, schema, window, service, environment) for metric in ALL_METRICS),
            return_exceptions=True,
        )

        # Scalar metric map — metric_name → float | None
        result: dict[str, object] = {
            metric.value: None if isinstance(value, Exception) else value
            for metric, value in zip(ALL_METRICS, values)
        }

        # K8s label enrichment: run a lightweight vector query for the CPU
        # metric (cAdvisor carries namespace/pod/container on every series).
        # Falls back gracefully to all-None when labels are not present.
        result["_k8s"] = await self._extract_k8s_labels(
            schema, window, service, environment
        )
        return result

    async def _query_one(
        self, metric: MetricType, schema: LabelSchema, window: str, service: str, environment: str
    ) -> Optional[float]:
        """Query a single metric for a service with automatic relaxation for infrastructure metrics."""
        label_key = self._label_for(metric, schema)
        matchers = self._target_matchers(schema, label_key, service, environment)
        
        # Primary query attempt using target service matchers
        value = await self._query_with_fallbacks(metric, schema, window, matchers, label_key)
        if value is not None or metric not in _INFRASTRUCTURE_METRICS:
            return value

        # Node-exporter and cAdvisor infrastructure metrics frequently do not
        # carry the application's service label. Retry without that matcher so
        # the response can include the node/container hosting the service.
        relaxed = {key: v for key, v in matchers.items() if key != label_key}
        return await self._query_with_fallbacks(metric, schema, window, relaxed, label_key)

    async def _extract_k8s_labels(
        self, schema: LabelSchema, window: str, service: str, environment: str
    ) -> dict[str, Optional[str]]:
        """Extract K8s label metadata (namespace/pod/container) for the target service.

        Runs a single vector query against the CPU metric (which is the most
        likely series to carry container-level labels in a Kubernetes setup)
        and reads the K8s labels from the first matching result entry.

        Returns a dict with keys ``namespace``, ``pod``, ``container`` — all
        ``None`` when the target does not expose these labels (MONOLITH mode).
        """
        empty: dict[str, Optional[str]] = {"namespace": None, "pod": None, "container": None}

        # Only attempt enrichment when the schema has at least one K8s label.
        if not any([schema.namespace_label, schema.pod_label, schema.container_label]):
            return empty

        try:
            label_key = self._label_for(MetricType.CPU_USAGE, schema)
            matchers = self._target_matchers(schema, label_key, service, environment)
            promql = PromQLBuilder.build(MetricType.CPU_USAGE, schema, window, matchers)
            response = await self._client.get(
                f"{self._base_url}/api/v1/query", params={"query": promql}
            )
            response.raise_for_status()
            results = response.json().get("data", {}).get("result", [])
            if not results:
                return empty
            # Pick the first series that matches the target service label.
            labels = results[0].get("metric", {})
            return {
                "namespace": labels.get(schema.namespace_label) if schema.namespace_label else None,
                "pod": labels.get(schema.pod_label) if schema.pod_label else None,
                "container": labels.get(schema.container_label) if schema.container_label else None,
            }
        except Exception:  # noqa: BLE001
            return empty
    
    async def list_services(self, force_refresh: bool = False) -> set[str]:
        """Discover distinct service/app names via the schema's HTTP group label."""
        self._ensure_client()
        
        schema = await self.get_schema(force_refresh=force_refresh)
        
        response = await self._client.get(
            f"{self._base_url}/api/v1/label/{schema.http_group_label}/values"
        )
        
        response.raise_for_status()
        return set(response.json().get("data", []))
    async def _query_with_fallbacks(
        self, metric: MetricType, schema: LabelSchema, window: str, matchers: dict[str, str], label_hint: str
    ) -> Optional[float]:
        """Attempt to query via pre-computed Recording Rules first; fall back to raw PromQL on missing/empty results."""
        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_hint)
        if rule_name:
            value = await self._run_instant_scalar(PromQLBuilder.with_matchers(rule_name, matchers))
            if value is not None:
                return value
        
        # Fall back to dynamically generating and executing standard PromQL query
        return await self._run_instant_scalar(PromQLBuilder.build(metric, schema, window, matchers))

    @staticmethod
    def _target_matchers(schema: LabelSchema, label_key: str, service: str, environment: str) -> dict[str, str]:
        """Construct PromQL matcher key-value pairs for filtering by service and optional environment."""
        matchers = {label_key: service}
        if schema.environment_label and environment:
            matchers[schema.environment_label] = environment
        return matchers

    @staticmethod
    def _label_for(metric: MetricType, schema: LabelSchema) -> str:
        """Select the appropriate grouping label (process vs HTTP) based on the metric category."""
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
        """Execute an instant PromQL query expecting a single scalar numerical result."""
        response = await self._client.get(f"{self._base_url}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        result = response.json().get("data", {}).get("result", [])
        
        # Safely extract the float value from the first result series
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    async def collect(self, metrics: tuple[MetricType, ...] = ALL_METRICS, window: str = "5m") -> CollectionResult:
        """Fleet-wide normalized samples, retaining optional K8s labels when present."""
        schema = await self.get_schema()
        
        # Execute vector queries across all targets concurrently
        results = await asyncio.gather(*(self._collect_one(metric, schema, window) for metric in metrics), return_exceptions=True)
        
        samples, errors = [], []
        for metric, result in zip(metrics, results):
            if isinstance(result, Exception):
                errors.append(f"{metric.value}: {result!r}")
            else:
                samples.extend(result)
                
        return CollectionResult(schema.architecture, samples, errors)

    async def _collect_one(self, metric: MetricType, schema: LabelSchema, window: str) -> list[MetricSample]:
        """Collect vector samples across the fleet for a single metric using recording rules or standard PromQL."""
        label_key = self._label_for(metric, schema)
        rule_name = await self._rule_resolver.resolve(metric, label_hint=label_key)
        
        # Try fetching from a pre-computed recording rule first
        if rule_name:
            samples = await self._run_instant_vector(rule_name, metric, DataSource.RECORDING_RULE, label_key, schema)
            if samples:
                return samples
                
        # Fall back to raw PromQL vector calculation
        promql = PromQLBuilder.build(metric, schema, window)
        return await self._run_instant_vector(promql, metric, DataSource.DIRECT_QUERY, label_key, schema)

    async def _run_instant_vector(
        self, promql: str, metric: MetricType, source: DataSource, label_key: str, schema: LabelSchema
    ) -> list[MetricSample]:
        """Execute instant query expecting a metric vector and parse into standard MetricSample models."""
        response = await self._client.get(f"{self._base_url}/api/v1/query", params={"query": promql})
        response.raise_for_status()
        
        samples = []
        for entry in response.json().get("data", {}).get("result", []):
            labels = entry.get("metric", {})
            try:
                value = float(entry["value"][1])
            except (KeyError, IndexError, TypeError, ValueError):
                value = None
                
            # Construct sample instance enriched with target and optional Kubernetes metadata
            samples.append(
                MetricSample(
                    metric,
                    self._extract_target(labels, label_key),
                    value,
                    source,
                    promql,
                    label_key,
                    namespace=labels.get(schema.namespace_label) if schema.namespace_label else None,
                    pod=labels.get(schema.pod_label) if schema.pod_label else None,
                    container=labels.get(schema.container_label) if schema.container_label else None,
                )
            )
        return samples

    @staticmethod
    def _extract_target(labels: dict[str, str], label_key: str) -> str:
        """Resolve the target service/instance name using the given label key or fallbacks."""
        for key in (label_key, *_TARGET_LABEL_FALLBACKS):
            if key in labels:
                return labels[key]
        # Return the first non-internal label value if fallbacks fail, or 'unknown'
        return next((value for key, value in labels.items() if not key.startswith("__")), "unknown")

    async def ping(self) -> bool:
        """Check availability by querying the Prometheus server health endpoint."""
        self._ensure_client()
        response = await self._client.get(f"{self._base_url}/-/healthy")
        response.raise_for_status()
        return True