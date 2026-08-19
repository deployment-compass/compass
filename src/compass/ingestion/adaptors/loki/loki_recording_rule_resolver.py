"""Resolves an existing Loki recording-rule metric name for a given
LogSignalType via fuzzy keyword matching.

Design rationale
----------------
Recording rules pre-aggregate expensive count_over_time queries. When
they exist, we prefer them over raw LogQL because:
  1. They are faster (already aggregated).
  2. They are more stable (computed on the ruler, not the querier).
  3. They respect the same retention as metrics.

However, Loki ruler metric names are not standardized. One team may call
it "log_exception_rate_5m", another "service:log_exception_rate". We
therefore use fuzzy keyword matching instead of exact lookup.

Endpoint
--------
Loki's ruler exposes a Prometheus-compatible rules listing at
/prometheus/api/v1/rules when `-ruler.enable-api=true`. The JSON shape
is identical to Prometheus's /api/v1/rules, so we reuse the same parsing
logic as the Prometheus adapter.
"""

from __future__ import annotations

import time
from typing import Optional

import httpx

from .loki_models import LogSignalType

# ---------------------------------------------------------------------------
# Keyword maps
# ---------------------------------------------------------------------------
# For each signal, we list the substrings that must all appear in the
# recording rule name for it to be considered a match. The matching is
# case-insensitive.
_SIGNAL_KEYWORDS: dict[LogSignalType, list[str]] = {
    LogSignalType.EXCEPTION_RATE: ["exception", "rate"],
    LogSignalType.FATAL_RATE: ["fatal", "rate"],
    LogSignalType.OOM_SIGNAL: ["oom"],
    LogSignalType.DEPENDENCY_CONNECTION_ERRORS: ["connection"],
}

# Suffixes we explicitly reject. These are typically statistical
# companion rules (_avg_1h, _stddev_1h) that we don't want to query
# directly — we want the raw rate rule, not its historical average.
_EXCLUDE_SUFFIXES = ("_avg_1h", "_stddev_1h", ":avg_1h", ":stddev_1h")


class LokiRuleResolver:
    """Finds the best recording rule name for a given signal."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        cache_ttl_seconds: int = 300,
        overrides: Optional[dict[LogSignalType, str]] = None,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._ttl = cache_ttl_seconds

        # Optional hard-coded overrides. If a caller knows the exact
        # rule name for a signal, they can supply it here to skip fuzzy
        # matching entirely.
        self._overrides = overrides or {}

        # Cache of all recording rule names. We cache the whole list
        # (not per-signal) because the ruleset is small and this avoids
        # N round-trips when resolving multiple signals.
        self._cached_names: Optional[list[str]] = None
        self._cached_at: float = 0.0

    async def resolve(self, signal: LogSignalType, label_hint: Optional[str] = None) -> Optional[str]:
        """Return the best recording rule name for *signal*, or None.

        The resolution algorithm:
          1. If an override exists for this signal, return it.
          2. Fetch all recording rule names from Loki's ruler API.
          3. Filter: name must contain all keywords for this signal and
             must NOT end with an excluded suffix.
          4. If label_hint is provided (e.g. "service"), prefer names
             that contain that hint — this disambiguates when multiple
             rules match the same keywords.
          5. Return the shortest matching name (heuristic: shorter names
             are usually the base rule, not a derived one).

        Args:
            signal: The anomaly signal we need a rule for.
            label_hint: Optional string used to break ties. Usually the
                discovered group_label.

        Returns:
            A metric name string, or None if no suitable rule exists.
        """
        # Short-circuit: caller knows better than fuzzy matching.
        if signal in self._overrides:
            return self._overrides[signal]

        keywords = _SIGNAL_KEYWORDS[signal]
        candidates = await self._all_recording_rule_names()

        # Primary filter: all keywords present, no excluded suffix.
        matches = [
            name
            for name in candidates
            if not name.lower().endswith(_EXCLUDE_SUFFIXES)
            and all(kw in name.lower() for kw in keywords)
        ]
        if not matches:
            # None of the four signals matched — this is expected for
            # OOM_SIGNAL and DEPENDENCY_CONNECTION_ERRORS when they are
            # alert-only (no recording rule defined in the ruleset).
            return None

        # Tie-breaker: if label_hint is present, prefer names that
        # include it. This helps when a ruleset has both
        # "log_exception_rate_5m" and "service_log_exception_rate_5m".
        if label_hint:
            hinted = [n for n in matches if label_hint.lower() in n.lower()]
            if hinted:
                matches = hinted

        # Final tie-breaker: shortest name. The assumption is that the
        # base rule is the shortest one; longer names tend to be
        # variants or aggregations over the base rule.
        matches.sort(key=len)
        return matches[0]

    async def _all_recording_rule_names(self) -> list[str]:
        """Fetch recording rule names from Loki, with caching."""
        if self._cached_names is not None and (time.monotonic() - self._cached_at) < self._ttl:
            return self._cached_names

        names: list[str] = []
        try:
            resp = await self._client.get(
                f"{self._base_url}/prometheus/api/v1/rules", params={"type": "record"}
            )
            resp.raise_for_status()

            # The response mirrors Prometheus's rules API:
            # data.groups[].rules[].name for rules of type "recording".
            groups = resp.json().get("data", {}).get("groups", [])
            for group in groups:
                for rule in group.get("rules", []):
                    if rule.get("type") == "recording" and "name" in rule:
                        names.append(rule["name"])
        except (httpx.HTTPError, KeyError):
            # If Loki is down or the ruler API is disabled, we fall back
            # to an empty list. The adapter will then use raw LogQL.
            pass

        self._cached_names = names
        self._cached_at = time.monotonic()
        return names