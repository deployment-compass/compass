"""
Dynamic label discovery for Loki, mirroring compass_metrics.label_discovery
but against Loki's label-values endpoint instead of Prometheus's. There's
no architecture (microservice/monolith) verdict to make here — Loki
doesn't expose CPU/memory metric families to probe — just: which label
does this Loki instance's log streams actually carry for grouping.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

from .loki_models import LogLabelSchema

_GROUP_LABEL_CANDIDATES = ["service", "app", "app_kubernetes_io_name", "namespace", "job"]
_ENVIRONMENT_LABEL_CANDIDATES = ["environment", "env", "deployment_environment"]
_NAMESPACE_LABEL_CANDIDATES = ["namespace", "kube_namespace"]
_POD_LABEL_CANDIDATES = ["pod", "pod_name", "kube_pod"]
_CONTAINER_LABEL_CANDIDATES = ["container", "container_name"]

# The base stream selector every rule in the uploaded ruleset is built on
# ({job=~".+"}). Kept as a constant default rather than "discovered"
# because it's a deliberate scoping choice (match all jobs), not a fact
# about label presence the way the group label is.
_DEFAULT_STREAM_SELECTOR_BASE = ""


class LokiLabelDiscovery:

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        cache_ttl_seconds: int = 300,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._ttl = cache_ttl_seconds
        self._cached: Optional[LogLabelSchema] = None
        self._cached_at: float = 0.0

    async def discover(self, force: bool = False) -> LogLabelSchema:
        if not force and self._cached and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached

        group_label = await self._first_populated_label(_GROUP_LABEL_CANDIDATES)
        schema = LogLabelSchema(
            group_label=group_label or "job",
            stream_selector_base=_DEFAULT_STREAM_SELECTOR_BASE,
            environment_label=await self._first_populated_label(_ENVIRONMENT_LABEL_CANDIDATES),
            namespace_label=await self._first_populated_label(_NAMESPACE_LABEL_CANDIDATES),
            pod_label=await self._first_populated_label(_POD_LABEL_CANDIDATES),
            container_label=await self._first_populated_label(_CONTAINER_LABEL_CANDIDATES),
        )
        self._cached = schema
        self._cached_at = time.monotonic()
        return schema

    async def _label_has_values(self, label: str) -> bool:
        try:
            resp = await self._client.get(f"{self._base_url}/loki/api/v1/label/{label}/values")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return len(data) > 0
        except httpx.HTTPError:
            return False

    async def _first_populated_label(self, candidates: list[str]) -> Optional[str]:
        for label in candidates:
            if await self._label_has_values(label):
                return label
        return None
