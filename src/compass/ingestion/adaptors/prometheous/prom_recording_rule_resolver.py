"""
Resolves an existing Prometheus recording-rule metric name for a given
MetricType...
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
    # Assumes recording rules use "pct" (a common convention) rather than
    # "percent"/"utilization". If your team names rules differently, pass
    # an explicit entry in `recording_rule_overrides` instead of relying on
    # this heuristic — direct PromQL is used automatically when no rule matches.
    MetricType.MEMORY_USAGE_PERCENT: ["mem", "pct"],
    MetricType.DISK_USAGE_PERCENT: ["disk", "pct"],
}

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
            resp = await self._client.get(f"{self._base_url}/api/v1/rules", params={"type": "record"})
            resp.raise_for_status()
            groups = resp.json().get("data", {}).get("groups", [])
            for group in groups:
                for rule in group.get("rules", []):
                    if rule.get("type") == "recording" and "name" in rule:
                        names.append(rule["name"])
        except (httpx.HTTPError, KeyError):
            pass

        self._cached_names = names
        self._cached_at = time.monotonic()
        return names