from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI,Request,Depends,Response, status
from typing import Optional
from compass.api import webhooks 
from compass.src.compass.ingestion.event_collector import collector
from compass.ingestion.adaptors.prometheous.prom_adaptor import PrometheusAdapter
from compass.ingestion.adaptors.loki.loki_adaptor import LokiAdapter
from compass.config import settings
import asyncio

import uvicorn

_PROM_KEY_ = "prometheus_adapter"
_LOKI_KEY_ = "loki_adapter"




@asynccontextmanager
async def lifespan(app: FastAPI):
    # init event collector
    await collector.start()
    
    # Initialize adapters instance on application startup
    prom_adapter = PrometheusAdapter(
        base_url=settings.prometheus_url,
        timeout_seconds=settings.prometheus_timeout_seconds,
        schema_cache_ttl_seconds=settings.prometheus_cache_ttl_seconds,
    )
    setattr(app.state, _PROM_KEY_, prom_adapter)
    
    loki_adaptor = LokiAdapter(
        base_url=settings.loki_url,
        timeout_seconds=settings.loki_timeout_seconds,
        schema_cache_ttl_seconds=settings.loki_cache_ttl_seconds,
    )
    setattr(app.state, _LOKI_KEY_, loki_adaptor)

    yield
    # cleanup event collector
    
    await collector.stop()
    
    # clean up prom adaptor
    prom_adapter: Optional[PrometheusAdapter] = getattr(app.state, _PROM_KEY_, None)
    if prom_adapter is not None:
        await prom_adapter.aclose()
    setattr(app.state, _PROM_KEY_, None) 
    
    # clean up loki adaptor
    loki_adaptor: Optional[LokiAdapter] = getattr(app.state, _LOKI_KEY_, None)
        
    if loki_adaptor is not None:
        await loki_adaptor.aclose()
        setattr(app.state, _LOKI_KEY_, None)

app = FastAPI(
    title="Compass",
    version="1.0.0",
    description="Prototype built with FastAPI For DevopsDays hackthon 2026",
    lifespan=lifespan)

# getter for the adapters
def get_prometheus_adapter(request: Request) -> PrometheusAdapter:
    """
    FastAPI dependency — inject with `Depends(get_prometheus_adapter)`.
    """
    adapter: Optional[PrometheusAdapter] = getattr(request.app.state, _PROM_KEY_, None)
    if adapter is None:
        raise RuntimeError(
            "PrometheusAdapter not attached to app.state — wire up "
        )
    return adapter

  
def get_loki_adapter(request: Request) -> LokiAdapter:
    """FastAPI dependency — inject with Depends(get_loki_adapter)."""
    adapter: Optional[LokiAdapter] = getattr(request.app.state, _LOKI_KEY_, None)
    if adapter is None:
        raise RuntimeError(
            "LokiAdapter not attached to app.state — wire up loki_lifespan "
            "(or call attach_loki_adapter in your own lifespan handler) "
            "before serving requests."
        )
    return adapter


# routers 
  
app.include_router(webhooks.router)

# root endPoints

@app.get("/health/live")
def liveness():
    """Is the process itself alive? No downstream calls."""
    return {"status": "ok"}


@app.get("/health/ready")
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



@app.get("/context/{service}")
async def get_full_context(
    service: str,
    environment: str = "prod",
    window_seconds: int = 300,
    prom: PrometheusAdapter = Depends(get_prometheus_adapter),
    loki: LokiAdapter = Depends(get_loki_adapter),
):
    """
    The actual Context Builder shape: merge metric-derived and
    log-derived signals for one (service, environment) into a single
    context dict for the anomaly-detection model.
    """
    metric_context, log_context = await asyncio.gather(
        prom.query(service, environment, window_seconds),
        loki.query(service, environment, window_seconds),
    )
    return {**metric_context, **log_context}

 
@app.get("/schema")
async def get_schema(adapter: PrometheusAdapter = Depends(get_prometheus_adapter)):
    """Debug endpoint: what did discovery actually find for this Prometheus?"""
    schema = await adapter.get_schema()
    return {
        "architecture": schema.architecture.value,
        "http_group_label": schema.http_group_label,
        "process_group_label": schema.process_group_label,
        "cpu_metric": schema.cpu_metric,
        "memory_metric": schema.memory_metric,
        "environment_label": schema.environment_label,
        "namespace_label": schema.namespace_label,
        "pod_label": schema.pod_label,
        "container_label": schema.container_label,
    }
    
# Start the app
def main():
    uvicorn.run("compass.main:app", host="0.0.0.0", port=8000, reload=True)
