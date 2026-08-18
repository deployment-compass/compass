"""
Resolves an existing Loki recording-rule metric name for a given
LogSignalType via fuzzy keyword matching — same approach as
compass_metrics.recording_rule_resolver, pointed at Loki's ruler API
instead of Prometheus's.

Loki's ruler exposes a Prometheus-compatible rules listing endpoint at
/prometheus/api/v1/rules when `-ruler.enable-api=true` (the same flag
that lets it serve the alert_fired webhook stream this ruleset's header
describes) — same response shape as Prometheus's own /api/v1/rules, so
this resolver's parsing logic is identical to the Prometheus one.

Two of the four log signals in the uploaded ruleset — oom_log_signal and
dependency_connection_errors — are alert-only with no recording rule at
all. resolve() correctly returns None for those, which is what forces
the adapter onto its raw-LogQL fallback every time for those two, not a
bug to be "fixed" by relaxing the matcher.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx

from .loki_models import LogSignalType

_SIGNAL_KEYWORDS: dict[LogSignalType, list[str]] = {
    LogSignalType.EXCEPTION_RATE: ["exception", "rate"],
    LogSignalType.FATAL_RATE: ["fatal", "rate"],
    LogSignalType.OOM_SIGNAL: ["oom"],
    LogSignalType.DEPENDENCY_CONNECTION_ERRORS: ["connection"],
}

_EXCLUDE_SUFFIXES = ("_avg_1h", "_stddev_1h", ":avg_1h", ":stddev_1h")


class LokiRuleResolver:

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
        self._overrides = overrides or {}
        self._cached_names: Optional[list[str]] = None
        self._cached_at: float = 0.0

    async def resolve(self, signal: LogSignalType, label_hint: Optional[str] = None) -> Optional[str]:
        if signal in self._overrides:
            return self._overrides[signal]

        keywords = _SIGNAL_KEYWORDS[signal]
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
                f"{self._base_url}/prometheus/api/v1/rules", params={"type": "record"}
            )
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
