"""Dynamic label discovery for Loki.

Mirrors compass_metrics.label_discovery but queries Loki's
/loki/api/v1/label/<name>/values endpoint instead of Prometheus.

Why discovery?
--------------
Loki is schemaless. One cluster labels streams with "service", another
with "app_kubernetes_io_name", another with "job". Hard-coding labels
would break on any deployment that doesn't match our conventions.
Instead, we probe the instance at runtime and cache the result.

What we discover vs. what we hard-code
--------------------------------------
- group_label, environment_label, etc.  -> DISCOVERED (vary by cluster)
- stream_selector_base                  -> HARD-CODED (deliberate scoping)
"""

from __future__ import annotations

import time
from typing import Optional

import httpx

from .loki_models import LogLabelSchema

# ---------------------------------------------------------------------------
# Candidate lists — ordered by preference
# ---------------------------------------------------------------------------
# For each conceptual role we want to discover, we try candidates in
# order until one returns non-empty values from Loki. The ordering
# reflects our preference: e.g. "service" is more specific than "job",
# so we try it first.
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
    """Probes a Loki instance to figure out which labels it actually uses."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        cache_ttl_seconds: int = 300,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._ttl = cache_ttl_seconds

        # Cached schema and its timestamp. We use monotonic time so
        # that clock skew doesn't cause premature or delayed expiry.
        self._cached: Optional[LogLabelSchema] = None
        self._cached_at: float = 0.0

    async def discover(self, force: bool = False) -> LogLabelSchema:
        """Return the label schema for the Loki instance.

        Args:
            force: If True, bypass the cache and re-probe Loki. Useful
                when labels are known to have changed (e.g. after a
                deployment that adds new labels).

        Returns:
            A LogLabelSchema with the discovered conventions.
        """
        # Cache hit: return immediately unless caller forced a refresh.
        if not force and self._cached and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached

        # Probe each candidate list in turn. If none of the candidates
        # for a role have values, we fall back to None (or "job" for the
        # group_label, which is guaranteed to exist in any Loki setup).
        group_label = await self._first_populated_label(_GROUP_LABEL_CANDIDATES)

        schema = LogLabelSchema(
            group_label=group_label or "job",
            stream_selector_base=_DEFAULT_STREAM_SELECTOR_BASE,
            environment_label=await self._first_populated_label(_ENVIRONMENT_LABEL_CANDIDATES),
            namespace_label=await self._first_populated_label(_NAMESPACE_LABEL_CANDIDATES),
            pod_label=await self._first_populated_label(_POD_LABEL_CANDIDATES),
            container_label=await self._first_populated_label(_CONTAINER_LABEL_CANDIDATES),
        )

        # Update cache.
        self._cached = schema
        self._cached_at = time.monotonic()
        return schema

    async def _label_has_values(self, label: str) -> bool:
        """Check whether <label> has at least one value in this Loki."""
        try:
            resp = await self._client.get(f"{self._base_url}/loki/api/v1/label/{label}/values")
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return len(data) > 0
        except httpx.HTTPError:
            # If Loki is unreachable or the label doesn't exist, treat
            # it as "not present" rather than raising — discovery should
            # be resilient to transient failures.
            return False

    async def _first_populated_label(self, candidates: list[str]) -> Optional[str]:
        """Return the first candidate that has values, or None."""
        for label in candidates:
            if await self._label_has_values(label):
                return label
        return None