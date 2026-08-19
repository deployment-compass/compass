from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI,Request,Depends,Response, status
from typing import Optional
from compass.api import webhooks 
from compass.ingestion.event_collector import collector
from compass.ingestion.adaptors.prometheous.prom_adaptor import PrometheusAdapter
from compass.ingestion.adaptors.loki.loki_adaptor import LokiAdaptor
from compass.context.context_collector import ContextCollector,CollectorResult
from compass.config import settings
from compass.schemas.response import BatchContextResponse,ContextResponse,ServiceListResponse
import asyncio
from functools import lru_cache

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
    
    loki_adaptor = LokiAdaptor(
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
    loki_adaptor: Optional[LokiAdaptor] = getattr(app.state, _LOKI_KEY_, None)
        
    if loki_adaptor is not None:
        await loki_adaptor.aclose()
        setattr(app.state, _LOKI_KEY_, None)

app = FastAPI(
    title="Compass",
    version="1.0.0",
    description="Prototype built with FastAPI For DevopsDays hackthon 2026",
    lifespan=lifespan)

# getter for the adapters

@lru_cache
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

@lru_cache
def get_loki_adapter(request: Request) -> LokiAdaptor:
    """FastAPI dependency — inject with Depends(get_loki_adapter)."""
    adapter: Optional[LokiAdaptor] = getattr(request.app.state, _LOKI_KEY_, None)
    if adapter is None:
        raise RuntimeError(
            "LokiAdaptor not attached to app.state — wire up loki_lifespan "
            "(or call attach_loki_adapter in your own lifespan handler) "
            "before serving requests."
        )
    return adapter

@lru_cache
def get_context_builder() -> ContextCollector:
    return ContextCollector(get_prometheus_adapter(), get_loki_adapter())



 
def _to_context_response(service: str, environment: str, result: CollectorResult) -> ContextResponse:
    return ContextResponse(
        service=service,
        environment=environment,
        context=result.context,
        had_metric_errors=result.had_metric_errors,
        had_log_errors=result.had_log_errors,
    )
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
    loki: LokiAdaptor = Depends(get_loki_adapter),
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
        "disk_metric": schema.disk_metric,
        "environment_label": schema.environment_label,
        "namespace_label": schema.namespace_label,
        "pod_label": schema.pod_label,
        "container_label": schema.container_label,
    }
    
    

@app.get("/services", response_model=ServiceListResponse)
async def list_services(
    prom: PrometheusAdapter = Depends(get_prometheus_adapter),
    loki: LokiAdaptor | None = Depends(get_loki_adapter),
) -> ServiceListResponse:
    """Discover distinct service names visible to each configured source."""
    prom_services: set[str] = set()
    prom_error = None
    try:
        prom_services = await prom.list_services()
    except Exception as exc:  # noqa: BLE001
        prom_error = str(exc)
 
    loki_services: set[str] = set()
    loki_error = None
    if loki is not None:
        try:
            loki_services = await loki.list_services()
        except Exception as exc:  # noqa: BLE001
            loki_error = str(exc)
 
    return ServiceListResponse(
        prometheus_services=sorted(prom_services),
        loki_services=sorted(loki_services),
        union=sorted(prom_services | loki_services),
        prometheus_error=prom_error,
        loki_error=loki_error,
    )
 
 
@app.get("/context/{service}", response_model=ContextResponse)
async def get_context(
    service: str,
    environment: str = Query(default="prod"),
    window_seconds: int = Query(default=300, gt=0, le=3600),
    builder: ContextCollector = Depends(get_context_builder),
) -> ContextResponse:
    """Build the flattened anomaly-detection context for one service."""
    try:
        result = await builder.build(service, environment, window_seconds)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"context build failed: {exc}") from exc
    return _to_context_response(service, environment, result)
 
 
@app.get("/contexts", response_model=BatchContextResponse)
async def get_all_contexts(
    environment: str = Query(default="prod"),
    window_seconds: int = Query(default=300, gt=0, le=3600),
    services: list[str] | None = Query(
        default=None, description="Restrict to these services; omit to auto-discover."
    ),
    builder: ContextCollector = Depends(get_context_builder),
) -> BatchContextResponse:
    """Discover services (or use the provided list) and build context for each."""
    service_set = set(services) if services else None
    target_services = service_set if service_set is not None else await builder.discover_services()
 
    results: dict[str, ContextResponse] = {}
    failed: list[str] = []
    raw_results = await builder.build_all(environment, window_seconds, services=target_services)
 
    for service in target_services:
        result = raw_results.get(service)
        if result is None:
            failed.append(service)
            continue
        results[service] = _to_context_response(service, environment, result)
 
    return BatchContextResponse(
        environment=environment,
        window_seconds=window_seconds,
        results=results,
        failed_services=failed,
    )
 
# Start the app
def main():
    uvicorn.run("compass.main:app", host="0.0.0.0", port=8000, reload=True)
