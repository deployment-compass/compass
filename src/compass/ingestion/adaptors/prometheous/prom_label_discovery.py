"""
Dynamic target discovery.

Rather than assuming any single label convention or architecture, this
module *probes* the live Prometheus instance:

  - Which of a prioritized list of HTTP-grouping labels actually has
    values? (service / app / handler / route / endpoint / job)
  - Which of the container- vs process-level CPU/memory metric families
    actually has data? That tells us microservice vs monolith more
    reliably than guessing from label names alone.
  - (K8s-specific) Which Kubernetes labels are available?
    (namespace, pod, container) — optional enrichment, only if present.

Results are cached with a TTL, since this costs several HTTP round trips
and label sets rarely change during a running collection loop.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

from .prom_models import ArchitectureMode, LabelSchema

# Priority-ordered: first candidate with actual values wins.
_HTTP_LABEL_CANDIDATES = [
    "service",
    "app",
    "app_kubernetes_io_name",
    "handler",
    "route",
    "endpoint",
    "job",
]
_PROCESS_LABEL_CANDIDATES = ["service", "job", "instance"]
_ENVIRONMENT_LABEL_CANDIDATES = ["environment", "env", "deployment_environment"]

# K8s-specific labels (optional, only discovered if present)
_K8S_NAMESPACE_LABEL_CANDIDATES = ["namespace", "kube_namespace"]
_K8S_POD_LABEL_CANDIDATES = ["pod", "pod_name", "kube_pod"]
_K8S_CONTAINER_LABEL_CANDIDATES = ["container", "container_name"]

# (metric_name, architecture_it_implies), priority-ordered.
_CPU_METRIC_CANDIDATES = [
    ("container_cpu_usage_seconds_total", ArchitectureMode.MICROSERVICE),
    ("process_cpu_seconds_total", ArchitectureMode.MONOLITH),
    ("node_cpu_seconds_total", ArchitectureMode.MONOLITH),
]
_MEMORY_METRIC_CANDIDATES = [
    ("container_memory_working_set_bytes", ArchitectureMode.MICROSERVICE),
    ("process_resident_memory_bytes", ArchitectureMode.MONOLITH),
    ("node_memory_MemAvailable_bytes", ArchitectureMode.MONOLITH),
]


class LabelDiscovery:
    """Probes a Prometheus instance once (then caches) to build a LabelSchema."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        cache_ttl_seconds: int = 300,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._ttl = cache_ttl_seconds
        self._cached: Optional[LabelSchema] = None
        self._cached_at: float = 0.0

    async def discover(self, force: bool = False) -> LabelSchema:
        if not force and self._cached and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached

        http_label = await self._first_populated_label(_HTTP_LABEL_CANDIDATES)
        cpu_metric, cpu_arch = await self._first_present_metric(_CPU_METRIC_CANDIDATES)
        mem_metric, mem_arch = await self._first_present_metric(_MEMORY_METRIC_CANDIDATES)

        architecture = cpu_arch or mem_arch or ArchitectureMode.UNKNOWN
        process_label = await self._first_populated_label(_PROCESS_LABEL_CANDIDATES)

        environment_label = await self._first_populated_label(_ENVIRONMENT_LABEL_CANDIDATES)
        
        # K8s-specific labels (optional, only if present)
        namespace_label = await self._first_populated_label(_K8S_NAMESPACE_LABEL_CANDIDATES)
        pod_label = await self._first_populated_label(_K8S_POD_LABEL_CANDIDATES)
        container_label = await self._first_populated_label(_K8S_CONTAINER_LABEL_CANDIDATES)

        schema = LabelSchema(
            architecture=architecture,
            http_group_label=http_label or "job",
            process_group_label=process_label or "job",
            cpu_metric=cpu_metric or "process_cpu_seconds_total",
            memory_metric=mem_metric or "process_resident_memory_bytes",
            environment_label=environment_label,
            namespace_label=namespace_label,
            pod_label=pod_label,
            container_label=container_label,
        )
        self._cached = schema
        self._cached_at = time.monotonic()
        return schema

    async def _label_has_values(self, label: str) -> bool:
        try:
            resp = await self._client.get(f"{self._base_url}/api/v1/label/{label}/values")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return len(data) > 0
        except httpx.HTTPError as e:
            print(f"[LabelDiscovery Error] {label}: {e}")
            return False

    async def _first_populated_label(self, candidates: list[str]) -> Optional[str]:
        for label in candidates:
            if await self._label_has_values(label):
                return label
        return None

    async def _metric_exists(self, metric_name: str) -> bool:
        try:
            resp = await self._client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": f"count({metric_name})"},
            )
            resp.raise_for_status()
            result = resp.json().get("data", {}).get("result", [])
            return bool(result) and float(result[0]["value"][1]) > 0
        except (httpx.HTTPError, KeyError, ValueError, IndexError):
            return False

    async def _first_present_metric(
        self, candidates: list[tuple[str, ArchitectureMode]]
    ) -> tuple[Optional[str], Optional[ArchitectureMode]]:
        for metric_name, arch in candidates:
            if await self._metric_exists(metric_name):
                return metric_name, arch
        return None, None
