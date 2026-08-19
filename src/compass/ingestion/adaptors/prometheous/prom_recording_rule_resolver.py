"""Resolves an existing Prometheus recording-rule metric name for a given MetricType.

Prometheus recording rules pre-aggregate expensive queries (e.g.
histogram_quantile) into cheap metrics.  This module heuristically
maps our canonical MetricType enum to the actual recording-rule
names present in a given Prometheus instance.

If no rule matches, the adapter falls back to direct PromQL built by
PromQLBuilder.  Callers can also supply explicit overrides when the
heuristic guesses wrong.
"""

from __future__ import annotations

import time
from typing import Optional

import httpx

from .prom_models import MetricType

# ------------------------------------------------------------------
# Keyword maps: each MetricType maps to substrings we expect to find
# in a recording-rule name.  For example, a P95 latency rule might be
# named "service:p95_latency_seconds" — it contains both "p95" and
# "latency", so it matches.
# ------------------------------------------------------------------
_METRIC_KEYWORDS: dict[MetricType, list[str]] = {
    MetricType.P95_LATENCY: ["p95", "latency"],
    MetricType.ERROR_RATE: ["error", "rate"],
    MetricType.REQUEST_RATE: ["request", "rate"],
    MetricType.CPU_USAGE: ["cpu"],
    MetricType.MEMORY_USAGE: ["mem"],
    # Assumes recording rules use "pct" (a common convention) rather than
    # "percent"/"utilization".  If your team names rules differently, pass
    # an explicit entry in `recording_rule_overrides` instead of relying on
    # this heuristic — direct PromQL is used automatically when no rule matches.
    MetricType.MEMORY_USAGE_PERCENT: ["mem", "pct"],
    MetricType.DISK_USAGE_PERCENT: ["disk", "pct"],
}

# Suffixes we deliberately ignore.  Rules ending in these are usually
# trend / stddev variants, not the raw value we want.
_EXCLUDE_SUFFIXES = ("_avg_1h", "_stddev_1h", ":avg_1h", ":stddev_1h", "_trend_10m")


class RecordingRuleResolver:
    """Finds the best matching recording-rule name for a MetricType."""

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
        # Cache the full list of recording-rule names so we don't hit
        # the /api/v1/rules endpoint on every resolve() call.
        self._cached_names: Optional[list[str]] = None
        self._cached_at: float = 0.0

    async def resolve(self, metric: MetricType, label_hint: Optional[str] = None) -> Optional[str]:
        """Return the recording-rule metric name for `metric`, or None.

        Resolution order:
          1. Explicit override (highest priority).
          2. Heuristic match based on _METRIC_KEYWORDS.
          3. If `label_hint` is given, prefer rules whose name contains
             the hint (e.g. hint="checkout" prefers
             "checkout_p95_latency" over "frontend_p95_latency").
          4. Shortest match wins (shorter names are usually the "main"
             rule, not a specialised variant).

        Args:
            metric: The canonical metric type we need a rule for.
            label_hint: Optional substring that should appear in the
                        recording-rule name, used to disambiguate when
                        multiple rules match the same keywords.
        """
        # 1. Hard-coded override — always wins.
        if metric in self._overrides:
            return self._overrides[metric]

        keywords = _METRIC_KEYWORDS[metric]
        candidates = await self._all_recording_rule_names()

        # 2. Filter by keywords AND exclude known non-raw suffixes.
        matches = [
            name
            for name in candidates
            if not name.lower().endswith(_EXCLUDE_SUFFIXES)
            and all(kw in name.lower() for kw in keywords)
        ]
        if not matches:
            return None

        # 3. If a label_hint is provided, narrow to names containing it.
        if label_hint:
            hinted = [n for n in matches if label_hint.lower() in n.lower()]
            if hinted:
                matches = hinted

        # 4. Prefer the shortest name — it's usually the base rule, not
        #    a specialised or deeply nested one.
        matches.sort(key=len)
        return matches[0]

    async def _all_recording_rule_names(self) -> list[str]:
        """Fetch and cache every recording-rule name from Prometheus.

        Uses the /api/v1/rules?type=record endpoint.  The cache is
        invalidated after `cache_ttl_seconds`.
        """
        if self._cached_names is not None and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached_names

        names: list[str] = []
        try:
            resp = await self._client.get(f"{self._base_url}/api/v1/rules", params={"type": "record"})
            resp.raise_for_status()
            # The response is a nested structure: data.groups[].rules[].name
            groups = resp.json().get("data", {}).get("groups", [])
            for group in groups:
                for rule in group.get("rules", []):
                    if rule.get("type") == "recording" and "name" in rule:
                        names.append(rule["name"])
        except (httpx.HTTPError, KeyError):
            # If Prometheus is unreachable or the response shape is
            # unexpected, we simply return an empty list.  The caller
            # (adapter) will then fall back to direct PromQL.
            pass

        self._cached_names = names
        self._cached_at = time.monotonic()
        return names