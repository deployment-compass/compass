"""
Pull adaptor. NOT a trigger source — logs are pulled into context on demand
by the Context Builder, scoped to the soak window / incident timeframe.
"""
from __future__ import annotations
from compass.ingestion.adaptors.base import PullAdapter

import httpx


class LokiAdaptor(PullAdapter):
    source = "loki"

    def __init__(self, base_url: str, timeout_seconds: float = 5.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds

    async def query(self, service: str, environment: str, window_seconds: int) -> dict:
        """
        Runs a LogQL range query scoped to {service, environment} over the
        given window. Returns raw log lines — deduping/counting happens in
        the Context Builder, not here; this adaptor just fetches.
        """
        logql = f'{{app="{service}", namespace="{environment}"}}'
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(
                f"{self._base_url}/loki/api/v1/query_range",
                params={
                    "query": logql,
                    "start": f"-{window_seconds}s",
                    "limit": 1000,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        lines = [
            entry[1]
            for stream in data.get("data", {}).get("result", [])
            for entry in stream.get("values", [])
        ]
        return {"lines": lines}