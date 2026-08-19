"""One Loki pull adaptor for log-derived anomaly signals.

This is the main entry point. It orchestrates three subsystems:
  1. LokiLabelDiscovery     – figures out which labels the Loki uses.
  2. LokiRuleResolver       – finds pre-aggregated recording rules.
  3. LogQLBuilder           – builds raw LogQL when rules don't exist.

For every signal, the adaptor tries the fast path (recording rule)
first and falls back to the slow path (raw count_over_time LogQL) only
when necessary. This keeps latency low in well-configured environments
while remaining functional in bare-bones setups.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .loki_label_discovery import LokiLabelDiscovery
from .loki_models import LogLabelSchema, LogSignalType, LogSample, LogCollectionResult, LogDataSource
from .loki_recording_rule_resolver import LokiRuleResolver
from .logql_builder import LogQLBuilder

# All signals we know how to query. Used to expand "query everything"
# calls without enumerating enum members at every call site.
ALL_LOG_SIGNALS: tuple[LogSignalType, ...] = tuple(LogSignalType)

# When the discovered group_label is missing from a result series
# (shouldn't happen, but Loki is schemaless), we try these fallbacks
# in order before giving up and returning "unknown".
_TARGET_LABEL_FALLBACKS = ("service", "app", "job", "namespace")


class LokiAdaptor:
    source = "loki"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 5.0,
        schema_cache_ttl_seconds: int = 300,
        recording_rule_overrides: Optional[dict[LogSignalType, str]] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._schema_ttl = schema_cache_ttl_seconds

        # Optional hard-coded rule names that bypass fuzzy resolution.
        self._overrides = recording_rule_overrides

        # Lazy-initialized subsystems. We don't create the httpx client
        # in __init__ because this object may be instantiated in sync
        # contexts (e.g. module import time) before an event loop exists.
        self._client: Optional[httpx.AsyncClient] = None
        self._discovery: Optional[LokiLabelDiscovery] = None
        self._rule_resolver: Optional[LokiRuleResolver] = None

    def _ensure_client(self) -> None:
        """Idempotent lazy initialization of the HTTP client and subsystems."""
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LokiLabelDiscovery(self._client, self._base_url, self._schema_ttl)
        self._rule_resolver = LokiRuleResolver(
            self._client, self._base_url, self._schema_ttl, self._overrides
        )

    async def aclose(self) -> None:
        """Clean up the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
        self._client = self._discovery = self._rule_resolver = None

    async def get_schema(self, force_refresh: bool = False) -> LogLabelSchema:
        """Return the discovered label schema, probing Loki if necessary."""
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)

    async def query(self, service: str, environment: str, window_seconds: int) -> dict[str, Optional[float]]:
        """Return flat signal keys that can be merged directly into model context.

        This is the "single-service" API. It runs all four signals in
        parallel and returns a dict like:
            {
                "log_exception_rate": 12.0,
                "log_fatal_rate": 0.0,
                "oom_log_signal": None,
                ...
            }

        A value of None means either:
          - Loki returned no data for that signal, or
          - The query failed (we swallow exceptions to avoid one bad
            signal killing the whole context).

        Args:
            service: The value of the group_label to filter on.
            environment: The value of the environment_label to filter on.
            window_seconds: The look-back window for raw LogQL queries.
                Ignored when a recording rule is used (rules have their
                own baked-in range).

        Returns:
            Dict mapping signal name -> float or None.
        """
        schema = await self.get_schema()

        # Fire all four signal queries concurrently. Each is independent.
        values = await asyncio.gather(
            *(
                self._query_one(signal, schema, f"{window_seconds}s", service, environment)
                for signal in ALL_LOG_SIGNALS
            ),
            return_exceptions=True,
        )

        # If any individual query raised, we record None rather than
        # propagating the exception. This matches the contract: partial
        # data is better than no data.
        return {
            signal.value: None if isinstance(value, Exception) else value
            for signal, value in zip(ALL_LOG_SIGNALS, values)
        }

    async def _query_one(
        self,
        signal: LogSignalType,
        schema: LogLabelSchema,
        window: str,
        service: str,
        environment: str,
    ) -> Optional[float]:
        """Query a single signal for a single service, preferring recording rules."""
        # Build the label matchers for the service (+ environment if known).
        matchers = self._target_matchers(schema, service, environment)

        # Fast path: is there a recording rule for this signal?
        rule_name = await self._rule_resolver.resolve(signal, label_hint=schema.group_label)
        if rule_name:
            value = await self._run_instant_scalar(LogQLBuilder.with_matchers(rule_name, matchers))
            if value is not None:
                return value
            # If the recording rule returned nothing (e.g. no data in
            # its range), we fall through to raw LogQL rather than
            # returning None immediately.

        # Slow path: build and execute raw LogQL.
        return await self._run_instant_scalar(
            LogQLBuilder.build(signal, schema, window, matchers)
        )

    @staticmethod
    def _target_matchers(schema: LogLabelSchema, service: str, environment: str) -> dict[str, str]:
        """Construct label matchers for a specific service/environment."""
        matchers = {schema.group_label: service}
        # Only add the environment matcher if the schema discovered an
        # environment label *and* the caller provided a non-empty value.
        if schema.environment_label and environment:
            matchers[schema.environment_label] = environment
        return matchers

    async def _run_instant_scalar(self, logql: str) -> Optional[float]:
        """Execute an instant query and extract the first scalar value.

        This is used for single-service queries where we expect at most
        one series (because the matchers are fully qualified).
        """
        response = await self._client.get(
            f"{self._base_url}/loki/api/v1/query", params={"query": logql}
        )
        response.raise_for_status()
        result = response.json().get("data", {}).get("result", [])
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            # No data, malformed response, or non-numeric value.
            return None

    async def list_services(self, force_refresh: bool = False) -> set[str]:
        """Discover distinct service names via the schema's group label."""
        self._ensure_client()
        
        schema = await self.get_schema(force_refresh=force_refresh)
        response = await self._client.get(
            f"{self._base_url}/loki/api/v1/label/{schema.group_label}/values"
        )
        
        response.raise_for_status()
        return set(response.json().get("data", []))
    
    
    async def ping(self) -> bool:
        """Health check. Returns True if Loki's /ready endpoint is up."""
        self._ensure_client()
        response = await self._client.get(f"{self._base_url}/ready")
        response.raise_for_status()
        return True

    # -----------------------------------------------------------------------
    # Batch collection API (multi-service)
    # -----------------------------------------------------------------------
    # collect() is the "wide" API: it returns samples for *all* services
    # visible to Loki, not just one. This is useful for background
    # anomaly scanning or building service topology maps.

    async def collect(
        self,
        signals: tuple[LogSignalType, ...] = ALL_LOG_SIGNALS,
        window: str = "5m",
    ) -> LogCollectionResult:
        """Collect log-derived samples for all services.

        Unlike query(), this runs vector instant queries without
        target_matchers, so Loki returns one series per distinct value
        of the group_label.
        """
        self._ensure_client()
        schema = await self._discovery.discover()

        raw_results = await asyncio.gather(
            *(self._collect_one(s, schema, window) for s in signals),
            return_exceptions=True,
        )

        samples: list[LogSample] = []
        errors: list[str] = []
        for signal, res in zip(signals, raw_results):
            if isinstance(res, Exception):
                errors.append(f"{signal.value}: {res!r}")
                continue
            samples.extend(res)

        return LogCollectionResult(samples=samples, errors=errors)

    async def _collect_one(
        self, signal: LogSignalType, schema: LogLabelSchema, window: str
    ) -> list[LogSample]:
        """Collect all series for a single signal across every service."""
        label_key = schema.group_label

        # Same fast-path / slow-path logic as _query_one, but we expect
        # a vector (many series) instead of a scalar (one series).
        rule_name = await self._rule_resolver.resolve(signal, label_hint=label_key)
        if rule_name:
            samples = await self._run_instant_vector(
                rule_name, signal, LogDataSource.RECORDING_RULE, label_key
            )
            if samples:
                return samples

        logql = LogQLBuilder.build(signal, schema, window=window)
        return await self._run_instant_vector(
            logql, signal, LogDataSource.DIRECT_QUERY, label_key
        )

    async def _run_instant_vector(
        self, logql: str, signal: LogSignalType, source: LogDataSource, label_key: str
    ) -> list[LogSample]:
        """Execute an instant query and parse the full vector result.

        Each element in the vector becomes one LogSample. We attempt to
        extract the target name using label_key, with sensible fallbacks.
        """
        resp = await self._client.get(
            f"{self._base_url}/loki/api/v1/query", params={"query": logql}
        )
        resp.raise_for_status()
        vector = resp.json().get("data", {}).get("result", [])

        samples: list[LogSample] = []
        for entry in vector:
            labels = entry.get("metric", {})
            target = self._extract_target(labels, label_key)
            try:
                value = float(entry["value"][1])
            except (KeyError, IndexError, ValueError, TypeError):
                value = None
            samples.append(
                LogSample(
                    signal=signal,
                    target=target,
                    value=value,
                    source=source,
                    logql=logql,
                    raw_label=label_key,
                )
            )
        return samples

    @staticmethod
    def _extract_target(labels: dict, label_key: str) -> str:
        """Derive a human-readable target name from Loki series labels.

        Priority:
          1. The discovered group_label (label_key).
          2. Common fallback labels: service, app, job, namespace.
          3. Any non-internal label (not starting with "__").
          4. "unknown" as a last resort.
        """
        if label_key in labels:
            return labels[label_key]
        for fallback in _TARGET_LABEL_FALLBACKS:
            if fallback in labels:
                return labels[fallback]
        for key, value in labels.items():
            if not key.startswith("__"):
                return value
        return "unknown"