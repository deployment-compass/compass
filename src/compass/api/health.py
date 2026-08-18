from __future__ import annotations
import asyncio
from fastapi import APIRouter, Depends, Response, status
from compass.main import get_prometheus_adapter, get_loki_adapter
from compass.ingestion.adaptors.prometheous.prom_adaptor import PrometheusAdapter
from compass.ingestion.adaptors.loki.loki_adaptor import LokiAdapter

router = APIRouter(tags=["health"])


@router.get("/health/live")
def liveness():
    """Is the process itself alive? No downstream calls."""
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(
    response: Response,
    prom: PrometheusAdapter = Depends(get_prometheus_adapter),
    loki: LokiAdapter = Depends(get_loki_adapter),
):
    """Can we actually serve requests? Pings each dependency."""
    checks = {"prometheus": prom.ping(), "loki": loki.ping()}
    results = await asyncio.gather(*checks.values(), return_exceptions=True)

    report = {}
    healthy = True
    for name, result in zip(checks.keys(), results):
        ok = not isinstance(result, Exception)
        report[name] = "ok" if ok else str(result)
        healthy = healthy and ok

    response.status_code = status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "degraded", "checks": report}