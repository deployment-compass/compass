"""
Dynamic target discovery.
...
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

from .prom_models import ArchitectureMode, LabelSchema

_HTTP_LABEL_CANDIDATES = [
    "service", "app", "app_kubernetes_io_name", "handler", "route", "endpoint", "job",
]
_PROCESS_LABEL_CANDIDATES = ["service", "job", "instance"]
_ENVIRONMENT_LABEL_CANDIDATES = ["environment", "env", "deployment_environment"]

_K8S_NAMESPACE_LABEL_CANDIDATES = ["namespace", "kube_namespace"]
_K8S_POD_LABEL_CANDIDATES = ["pod", "pod_name", "kube_pod"]
_K8S_CONTAINER_LABEL_CANDIDATES = ["container", "container_name"]

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

# Denominators for percentage metrics. Checked independently of the metrics
# above since a target might expose e.g. process_resident_memory_bytes but
# still have node-level totals available as a fallback denominator.
_MEMORY_LIMIT_METRIC = "container_spec_memory_limit_bytes"
_MEMORY_TOTAL_METRIC = "node_memory_MemTotal_bytes"

# (primary_metric, paired_total/limit_metric, architecture_it_implies, mountpoint_label_or_None)
# Container-level usage/limit pair needs no mountpoint filter (already scoped
# to the container's own filesystem). Node-level avail/size needs one, or a
# multi-disk host's numbers get summed across every mounted volume.
_DISK_PAIR_CANDIDATES = [
    ("container_fs_usage_bytes", "container_fs_limit_bytes", ArchitectureMode.MICROSERVICE, None),
    ("node_filesystem_avail_bytes", "node_filesystem_size_bytes", ArchitectureMode.MONOLITH, "mountpoint"),
]


class LabelDiscovery:
    """Probes a Prometheus instance once (then caches) to build a LabelSchema."""

    def __init__(self, client: httpx.AsyncClient, base_url: str, cache_ttl_seconds: int = 300):
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

        process_label = await self._first_populated_label(_PROCESS_LABEL_CANDIDATES)
        environment_label = await self._first_populated_label(_ENVIRONMENT_LABEL_CANDIDATES)

        namespace_label = await self._first_populated_label(_K8S_NAMESPACE_LABEL_CANDIDATES)
        pod_label = await self._first_populated_label(_K8S_POD_LABEL_CANDIDATES)
        container_label = await self._first_populated_label(_K8S_CONTAINER_LABEL_CANDIDATES)

        memory_limit_metric = _MEMORY_LIMIT_METRIC if await self._metric_exists(_MEMORY_LIMIT_METRIC) else None
        memory_total_metric = _MEMORY_TOTAL_METRIC if await self._metric_exists(_MEMORY_TOTAL_METRIC) else None

        disk_metric, disk_pair_metric, disk_arch, disk_mountpoint_candidate = await self._first_present_disk_pair(
            _DISK_PAIR_CANDIDATES
        )
        disk_mountpoint_label = None
        if disk_mountpoint_candidate and await self._label_has_values(disk_mountpoint_candidate):
            disk_mountpoint_label = disk_mountpoint_candidate

        architecture = cpu_arch or mem_arch or disk_arch or ArchitectureMode.UNKNOWN

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
            memory_limit_metric=memory_limit_metric,
            memory_total_metric=memory_total_metric,
            disk_metric=disk_metric,
            disk_pair_metric=disk_pair_metric,
            disk_mountpoint_label=disk_mountpoint_label,
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

    async def _first_present_disk_pair(
        self, candidates: list[tuple[str, str, ArchitectureMode, Optional[str]]]
    ) -> tuple[Optional[str], Optional[str], Optional[ArchitectureMode], Optional[str]]:
        """Like _first_present_metric, but a candidate only counts if BOTH
        halves of the pair (usage+limit, or avail+size) actually exist —
        one without the other can't produce a percentage."""
        for primary, paired, arch, mountpoint_label in candidates:
            if await self._metric_exists(primary) and await self._metric_exists(paired):
                return primary, paired, arch, mountpoint_label
        return None, None, None, None