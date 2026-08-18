"""One Loki pull adaptor for log-derived anomaly signals."""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from .loki_label_discovery import LokiLabelDiscovery
from .loki_models import LogLabelSchema, LogSignalType,LogSample,LogCollectionResult,LogDataSource
from .loki_recording_rule_resolver import LokiRuleResolver
from .logql_builder import LogQLBuilder

ALL_LOG_SIGNALS: tuple[LogSignalType, ...] = tuple(LogSignalType)
_TARGET_LABEL_FALLBACKS = ("service", "app", "job", "namespace")


class LokiAdaptor:
    source = "loki"

    def __init__(self, base_url: str, timeout_seconds: float = 5.0, schema_cache_ttl_seconds: int = 300,
                 recording_rule_overrides: Optional[dict[LogSignalType, str]] = None):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._schema_ttl = schema_cache_ttl_seconds
        self._overrides = recording_rule_overrides
        self._client: Optional[httpx.AsyncClient] = None
        self._discovery: Optional[LokiLabelDiscovery] = None
        self._rule_resolver: Optional[LokiRuleResolver] = None

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._discovery = LokiLabelDiscovery(self._client, self._base_url, self._schema_ttl)
        self._rule_resolver = LokiRuleResolver(self._client, self._base_url, self._schema_ttl, self._overrides)

    async def aclose(self) -> None:
        if self._client:
            await self._client.aclose()
        self._client = self._discovery = self._rule_resolver = None

    async def get_schema(self, force_refresh: bool = False) -> LogLabelSchema:
        self._ensure_client()
        return await self._discovery.discover(force=force_refresh)

    async def query(self, service: str, environment: str, window_seconds: int) -> dict[str, Optional[float]]:
        """Return flat signal keys that can be merged directly into model context."""
        schema = await self.get_schema()
        values = await asyncio.gather(
            *(self._query_one(signal, schema, f"{window_seconds}s", service, environment) for signal in ALL_LOG_SIGNALS),
            return_exceptions=True,
        )
        return {signal.value: None if isinstance(value, Exception) else value for signal, value in zip(ALL_LOG_SIGNALS, values)}

    async def _query_one(self, signal: LogSignalType, schema: LogLabelSchema, window: str,
                         service: str, environment: str) -> Optional[float]:
        matchers = self._target_matchers(schema, service, environment)
        rule_name = await self._rule_resolver.resolve(signal, label_hint=schema.group_label)
        if rule_name:
            value = await self._run_instant_scalar(LogQLBuilder.with_matchers(rule_name, matchers))
            if value is not None:
                return value
        return await self._run_instant_scalar(LogQLBuilder.build(signal, schema, window, matchers))

    @staticmethod
    def _target_matchers(schema: LogLabelSchema, service: str, environment: str) -> dict[str, str]:
        matchers = {schema.group_label: service}
        if schema.environment_label and environment:
            matchers[schema.environment_label] = environment
        return matchers

    async def _run_instant_scalar(self, logql: str) -> Optional[float]:
        response = await self._client.get(f"{self._base_url}/loki/api/v1/query", params={"query": logql})
        response.raise_for_status()
        result = response.json().get("data", {}).get("result", [])
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            return None

    async def ping(self) -> bool:
        self._ensure_client()
        response = await self._client.get(f"{self._base_url}/ready")
        response.raise_for_status()
        return True
 
    async def collect(
        self,
        signals: tuple[LogSignalType, ...] = ALL_LOG_SIGNALS,
        window: str = "5m",
    ) -> LogCollectionResult:
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
        label_key = schema.group_label
 
        rule_name = await self._rule_resolver.resolve(signal, label_hint=label_key)
        if rule_name:
            samples = await self._run_instant_vector(
                rule_name, signal, LogDataSource.RECORDING_RULE, label_key
            )
            if samples:
                return samples
 
        logql = LogQLBuilder.build(signal, schema, window=window)
        return await self._run_instant_vector(logql, signal, LogDataSource.DIRECT_QUERY, label_key)
 
    async def _run_instant_vector(
        self, logql: str, signal: LogSignalType, source: LogDataSource, label_key: str
    ) -> list[LogSample]:
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
        if label_key in labels:
            return labels[label_key]
        for fallback in _TARGET_LABEL_FALLBACKS:
            if fallback in labels:
                return labels[fallback]
        for key, value in labels.items():
            if not key.startswith("__"):
                return value
        return "unknown"
 