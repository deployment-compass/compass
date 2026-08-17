"""
Resolves an existing Prometheus recording-rule metric name for a given
MetricType — WITHOUT assuming any fixed naming convention
Strategy:
  1. List every recording rule once via /api/v1/rules?type=record (cached).
  2. Fuzzy-match rule names against a per-metric keyword set.
  3. Drop obvious derived/statistical rules (avg_1h, stddev_1h baselines)
     so we don't grab a baseline input instead of the metric itself.
  4. If multiple candidates match, prefer the shortest name — composite
     or derived rules tend to accumulate longer, more qualified names.

An explicit `overrides` dict can bypass fuzzy matching entirely for a
specific metric, for teams that want deterministic behavior in
production instead of relying on the heuristic.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

from .prom_models import MetricType

_METRIC_KEYWORDS: dict[MetricType, list[str]] = {
    MetricType.P95_LATENCY: ["p95", "latency"],
    MetricType.ERROR_RATE: ["error", "rate"],
    MetricType.REQUEST_RATE: ["request", "rate"],
    MetricType.CPU_USAGE: ["cpu"],
    MetricType.MEMORY_USAGE: ["mem"],
}

# Rule names ending in these are statistical inputs derived FROM a base
# metric (rolling mean/stddev for baselining), not the metric itself.
_EXCLUDE_SUFFIXES = ("_avg_1h", "_stddev_1h", ":avg_1h", ":stddev_1h", "_trend_10m")


class RecordingRuleResolver:

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        cache_ttl_seconds: int = 300,
        overrides: Optional[dict[MetricType, str]] = None,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._ttl = cache_ttl_seconds
        self._overrides = overrides or {}
        self._cached_names: Optional[list[str]] = None
        self._cached_at: float = 0.0

    async def resolve(self, metric: MetricType, label_hint: Optional[str] = None) -> Optional[str]:
        """
        `label_hint` is the grouping label discovery found for this
        metric (e.g. "service"). When multiple rule names match the
        metric's keywords, we prefer ones whose name also contains the
        hint — this disambiguates e.g. a node-level `cpu_utilization`
        rule from a service-level `cpu_usage_cores` rule, which pure
        keyword matching on "cpu" alone can't distinguish since neither
        name is inherently more "correct" by length.
        """
        if metric in self._overrides:
            return self._overrides[metric]

        keywords = _METRIC_KEYWORDS[metric]
        candidates = await self._all_recording_rule_names()

        matches = [
            name
            for name in candidates
            if not name.lower().endswith(_EXCLUDE_SUFFIXES)
            and all(kw in name.lower() for kw in keywords)
        ]
        if not matches:
            return None

        if label_hint:
            hinted = [n for n in matches if label_hint.lower() in n.lower()]
            if hinted:
                matches = hinted

        matches.sort(key=len)
        return matches[0]

    async def _all_recording_rule_names(self) -> list[str]:
        if self._cached_names is not None and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached_names

        names: list[str] = []
        try:
            resp = await self._client.get(
                f"{self._base_url}/api/v1/rules", params={"type": "record"}
            )
            resp.raise_for_status()
            groups = resp.json().get("data", {}).get("groups", [])
            for group in groups:
                for rule in group.get("rules", []):
                    if rule.get("type") == "recording" and "name" in rule:
                        names.append(rule["name"])
        except (httpx.HTTPError, KeyError):
            # Non-fatal: caller falls back to direct PromQL when this
            # returns nothing.
            pass

        self._cached_names = names
        self._cached_at = time.monotonic()
        return names