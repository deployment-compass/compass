"""
Pull adaptor.metrics are pulled by bounded polling during an open soak window, never pushed as discrete events.
Called by the Soak-window manager and the Context Builder, not the collector.
"""
from __future__ import annotations
from compass.ingestion.adaptors.base import PullAdapter

import httpx

_METRIC_QUERIES = {
    "p95_latency": 'histogram_quantile(0.95, rate({service}_request_duration_seconds_bucket[5m]))',
    "error_rate": 'rate({service}_requests_total{{status=~"5.."}}[5m])',
    "memory_bytes": 'container_memory_working_set_bytes{{pod=~"{service}.*"}}',
}


class PrometheusAdaptor(PullAdapter):
    source = "prometheus"

    def __init__(self, base_url: str, timeout_seconds: float = 5.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds

    async def query(self, service: str, environment: str, window_seconds: int) -> dict:
        """
        Runs the soak-window metric set (p95 latency, error rate, memory
        trend) as instant queries against /api/v1/query. Returns a flat
        dict of metric_name -> value, or None if that metric had no data.
        """
        results: dict[str, float | None] = {}
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for metric_name, template in _METRIC_QUERIES.items():
                promql = template.format(service=service)
                resp = await client.get(
                    f"{self._base_url}/api/v1/query",
                    params={"query": promql},
                )
                resp.raise_for_status()
                data = resp.json()
                results[metric_name] = self._extract_scalar(data)
        return results

    @staticmethod
    def _extract_scalar(data: dict) -> float | None:
        result = data.get("data", {}).get("result", [])
        if not result:
            return None
        # instant vector: [timestamp, "value_as_string"]
        return float(result[0]["value"][1])