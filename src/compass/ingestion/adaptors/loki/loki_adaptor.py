"""
  - `query(service, environment, window_seconds)` — what the Context
    Builder calls, once per (service, environment) pull, to fold
    log-derived signals (exception rate, fatal rate, OOM/dependency
    error bursts) into the same context it builds from Prometheus. This
    is a second, independent PullAdapter — the Context Builder is
    expected to call both and merge their dicts, the same way Layer 2
    treats a log-derived series the same as a metric series per this
    ruleset's own comments.

"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .loki_models import  LogLabelSchema, LogSignalType
from .loki_label_discovery import LokiLabelDiscovery
from .logql_builder import LogQLBuilder
from .loki_recording_rule_resolver import LokiRuleResolver

ALL_LOG_SIGNALS: tuple[LogSignalType, ...] = tuple(LogSignalType)



class LokiAdapter:
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
        self._overrides = recording_rule_overrides

        self._client: Optional[httpx.AsyncClient] = None
        self._discovery: Optional[LokiLabelDiscovery] = None
        self._rule_resolver: Optional[LokiRuleResolver] = None

    # -- lifecycle --------------------------------------------------------

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LokiLabelDiscovery(
            self._client, self._base_url, cache_ttl_seconds=self._schema_ttl
        )
        self._rule_resolver = LokiRuleResolver(
            self._client,
            self._base_url,
            cache_ttl_seconds=self._schema_ttl,
            overrides=self._overrides,
        )

    async def aclose(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            self._discovery = None
            self._rule_resolver = None

    # -- Context Builder entry point --------------------------------------

    async def query(self, service: str, environment: str, window_seconds: int) -> dict:
        """
        Runs the log-signal set (exception rate, fatal rate, OOM signal,
        dependency-connection-error rate) scoped to one service, via
        recording rule when available, else raw LogQL. Returns a flat
        dict of signal_name -> value — same shape PrometheusAdapter.query()
        returns, so the Context Builder can merge both without special-
        casing either source.
        """
        self._ensure_client()
        schema = await self._discovery.discover()
        window = f"{window_seconds}s"

        tasks = [
            self._query_one(signal, schema, window, service, environment)
            for signal in ALL_LOG_SIGNALS
        ]
        values = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[str, Optional[float]] = {}
        for signal, value in zip(ALL_LOG_SIGNALS, values):
            out[signal.value] = None if isinstance(value, Exception) else value
        return out

    async def _query_one(
        self,
        signal: LogSignalType,
        schema: LogLabelSchema,
        window: str,
        service: str,
        environment: str,
    ) -> Optional[float]:
        label_key = schema.group_label
        matchers = self._target_matchers(label_key, service, environment)

        rule_name = await self._rule_resolver.resolve(signal, label_hint=label_key)
        if rule_name:
            logql = LogQLBuilder.with_matchers(rule_name, matchers)
            value = await self._run_instant_scalar(logql)
            if value is not None:
                return value

        logql = LogQLBuilder.build(signal, schema, window=window, target_matchers=matchers)
        return await self._run_instant_scalar(logql)

    @staticmethod
    def _target_matchers(label_key: str, service: str, environment: str) -> dict[str, str]:
        # No separate environment-label discovery here (unlike Prometheus)
        # since this ruleset's log streams don't declare one distinctly
        # from `job` — service is the only dimension we scope on. Extend
        # analogously to LabelSchema.environment_label if your Loki
        # deployment does carry one.
        return {label_key: service}

    async def _run_instant_scalar(self, logql: str) -> Optional[float]:
        resp = await self._client.get(
            f"{self._base_url}/loki/api/v1/query", params={"query": logql}
        )
        resp.raise_for_status()
        result = resp.json().get("data", {}).get("result", [])
        if not result:
            return None
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            return None


    async def get_schema(self, force_refresh: bool = False) -> LogLabelSchema:
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)
