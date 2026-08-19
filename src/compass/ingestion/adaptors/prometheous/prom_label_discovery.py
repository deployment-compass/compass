"""Dynamic target discovery.

This module solves the "which labels does *this* Prometheus use?"
problem.  Different teams instrument their apps with different label
names (service vs. app_kubernetes_io_name) and run on different
infrastructure (Kubernetes vs. bare metal).  Rather than hard-coding
assumptions, LabelDiscovery probes the Prometheus API once and
builds a LabelSchema that the rest of the pipeline can rely on.

The discovery is cached (default 5 min) to avoid hammering the
Prometheus API on every request.
"""

from __future__ import annotations

import time
from typing import Optional

import httpx

from .prom_models import ArchitectureMode, LabelSchema

# ------------------------------------------------------------------
# Candidate lists are ordered by preference.  The first label that
# actually has values in Prometheus wins.  This lets us support
# multiple conventions without configuration.
# ------------------------------------------------------------------

# HTTP / application-level grouping labels.
_HTTP_LABEL_CANDIDATES = [
    "service", "app", "app_kubernetes_io_name", "handler", "route", "endpoint", "job",
]

# Process / system-level grouping labels.
_PROCESS_LABEL_CANDIDATES = ["service", "job", "instance"]

# Environment / stage labels (prod, staging, etc.).
_ENVIRONMENT_LABEL_CANDIDATES = ["environment", "env", "deployment_environment"]

# Kubernetes-specific labels.
_K8S_NAMESPACE_LABEL_CANDIDATES = [
    "namespace", "kube_namespace", "kubernetes_namespace", "exported_namespace"
]
_K8S_POD_LABEL_CANDIDATES = [
    "pod", "pod_name", "kube_pod", "kubernetes_pod", "exported_pod"
]
_K8S_CONTAINER_LABEL_CANDIDATES = [
    "container", "container_name", "kube_container", "kubernetes_container", "exported_container"
]

# ------------------------------------------------------------------
# Metric candidates are tuples of (metric_name, implied_architecture).
# We check them in order; the first present metric decides (or at
# least hints at) the architecture mode.
# ------------------------------------------------------------------
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

# Denominators for percentage metrics.  Checked independently of the
# metrics above because a target might expose e.g.
# process_resident_memory_bytes but still have node-level totals
# available as a fallback denominator.
_MEMORY_LIMIT_METRIC = "container_spec_memory_limit_bytes"
_MEMORY_TOTAL_METRIC = "node_memory_MemTotal_bytes"

# Disk metric pairs: (usage_metric, limit_metric, architecture, mountpoint_label_or_None).
# Container-level usage/limit needs no mountpoint filter (already scoped
# to the container's own filesystem).  Node-level avail/size needs one,
# or a multi-disk host's numbers get summed across every mounted volume.
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
        """Return a LabelSchema, probing Prometheus only if the cache is stale.

        Args:
            force: If True, bypass the cache and re-probe immediately.
                   Useful when you know the scrape config has changed.
        """
        if not force and self._cached and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached

        # ------------------------------------------------------------------
        # 1. Discover grouping labels
        # ------------------------------------------------------------------
        http_label = await self._first_populated_label(_HTTP_LABEL_CANDIDATES)
        cpu_metric, cpu_arch = await self._first_present_metric(_CPU_METRIC_CANDIDATES)
        mem_metric, mem_arch = await self._first_present_metric(_MEMORY_METRIC_CANDIDATES)

        process_label = await self._first_populated_label(_PROCESS_LABEL_CANDIDATES)
        environment_label = await self._first_populated_label(_ENVIRONMENT_LABEL_CANDIDATES)

        namespace_label = await self._first_populated_label(_K8S_NAMESPACE_LABEL_CANDIDATES)
        pod_label = await self._first_populated_label(_K8S_POD_LABEL_CANDIDATES)
        container_label = await self._first_populated_label(_K8S_CONTAINER_LABEL_CANDIDATES)

        # ------------------------------------------------------------------
        # 2. Discover percentage-metric denominators
        # ------------------------------------------------------------------
        memory_limit_metric = _MEMORY_LIMIT_METRIC if await self._metric_exists(_MEMORY_LIMIT_METRIC) else None
        memory_total_metric = _MEMORY_TOTAL_METRIC if await self._metric_exists(_MEMORY_TOTAL_METRIC) else None

        # ------------------------------------------------------------------
        # 3. Discover disk metric pairs (both halves must exist)
        # ------------------------------------------------------------------
        disk_metric, disk_pair_metric, disk_arch, disk_mountpoint_candidate = await self._first_present_disk_pair(
            _DISK_PAIR_CANDIDATES
        )
        disk_mountpoint_label = None
        if disk_mountpoint_candidate and await self._label_has_values(disk_mountpoint_candidate):
            disk_mountpoint_label = disk_mountpoint_candidate

        # ------------------------------------------------------------------
        # 4. Resolve architecture mode
        #    CPU is the strongest signal, then memory, then disk.
        # ------------------------------------------------------------------
        architecture = cpu_arch or mem_arch or disk_arch or ArchitectureMode.UNKNOWN

        # ------------------------------------------------------------------
        # 5. Build and cache the schema
        # ------------------------------------------------------------------
        schema = LabelSchema(
            architecture=architecture,
            http_group_label=http_label or "job",          # "job" is the Prometheus default
            process_group_label=process_label or "job",
            cpu_metric=cpu_metric or "process_cpu_seconds_total",  # safe fallback
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

    # ------------------------------------------------------------------
    # Low-level probe helpers
    # ------------------------------------------------------------------

    async def _label_has_values(self, label: str) -> bool:
        """Ask Prometheus for all values of `label` and check if any exist.

        Uses the /api/v1/label/<name>/values endpoint.
        """
        try:
            resp = await self._client.get(f"{self._base_url}/api/v1/label/{label}/values")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return len(data) > 0
        except httpx.HTTPError as e:
            print(f"[LabelDiscovery Error] {label}: {e}")
            return False

    async def _first_populated_label(self, candidates: list[str]) -> Optional[str]:
        """Return the first label in `candidates` that has at least one value."""
        for label in candidates:
            if await self._label_has_values(label):
                return label
        return None

    async def _metric_exists(self, metric_name: str) -> bool:
        """Check whether `metric_name` has any time-series in Prometheus.

        We use `count(metric_name)` rather than a raw series query so
        that Prometheus can answer from its index without fetching
        actual samples.
        """
        try:
            resp = await self._client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": f"count({metric_name})"},
            )
            resp.raise_for_status()
            result = resp.json().get("data", {}).get("result", [])
            # A non-empty result with a count > 0 means the metric exists.
            return bool(result) and float(result[0]["value"][1]) > 0
        except (httpx.HTTPError, KeyError, ValueError, IndexError):
            # Any failure (network, malformed JSON, missing keys) is
            # treated as "metric does not exist" so discovery stays robust.
            return False

    async def _first_present_metric(
        self, candidates: list[tuple[str, ArchitectureMode]]
    ) -> tuple[Optional[str], Optional[ArchitectureMode]]:
        """Return the first (metric_name, architecture) pair where the metric exists."""
        for metric_name, arch in candidates:
            if await self._metric_exists(metric_name):
                return metric_name, arch
        return None, None

    async def _first_present_disk_pair(
        self, candidates: list[tuple[str, str, ArchitectureMode, Optional[str]]]
    ) -> tuple[Optional[str], Optional[str], Optional[ArchitectureMode], Optional[str]]:
        """Like _first_present_metric, but a candidate only counts if BOTH
        halves of the pair (usage+limit, or avail+size) actually exist —
        one without the other can't produce a percentage.
        """
        for primary, paired, arch, mountpoint_label in candidates:
            if await self._metric_exists(primary) and await self._metric_exists(paired):
                return primary, paired, arch, mountpoint_label
        return None, None, None, None