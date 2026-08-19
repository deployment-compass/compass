"""
Push-source entry points. Each route validates just enough to hand off,
normalizes synchronously (it's pure/cheap — no I/O), then enqueues on the
collector and returns immediately. The collector's bounded queue is what
absorbs any slowness downstream, not this route.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status

from compass.context.context_collector import CONTEXT_COLLECTOR_STATE_KEY, ContextCollector
from compass.ingestion.adaptors.github import GitHubActionsAdaptor
from compass.ingestion.adaptors.argocd import ArgoCDAdaptor
from compass.ingestion.adaptors.kubernetes_watch import KubernetesWatchAdaptor
from compass.ingestion.adaptors.base import AdaptorError
from compass.ingestion.event_collector import collector
from compass.investigation.window import collect_and_process
from compass.schemas.investigation import InvestigationWindowRequest

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])

_githubActions_adaptor = GitHubActionsAdaptor()
_argocd_adaptor = ArgoCDAdaptor()
_k8s_adaptor = KubernetesWatchAdaptor()


@router.post("/github", status_code=status.HTTP_202_ACCEPTED)
async def github_webhook(request: Request):
    raw = await request.json()
    try:
        event = _githubActions_adaptor.normalize(raw)
    except AdaptorError:
        logger.exception("bad github webhook payload")
        return {"status": "ignored"}
    await collector.ingest(event)
    return {"status": "accepted"}


@router.post("/argocd", status_code=status.HTTP_202_ACCEPTED)
async def argocd_webhook(request: Request):
    raw = await request.json()
    try:
        event = _argocd_adaptor.normalize(raw)
    except AdaptorError:
        logger.exception("bad argocd webhook payload")
        return {"status": "ignored"}
    await collector.ingest(event)
    return {"status": "accepted"}


@router.post("/kubernetes-event", status_code=status.HTTP_202_ACCEPTED)
async def kubernetes_event(request: Request):
    """Called by a small sidecar/relay that forwards watch-stream events over HTTP."""
    raw = await request.json()
    await collector.ingest_raw(_k8s_adaptor, raw)
    return {"status": "accepted"}


@router.post("/investigation-window", status_code=status.HTTP_202_ACCEPTED)
async def investigation_window_webhook(
    investigation: InvestigationWindowRequest, request: Request
):
    """Open a window, collect its context, and publish it to Layer 2 detection."""
    context_collector: ContextCollector | None = getattr(
        request.app.state, CONTEXT_COLLECTOR_STATE_KEY, None
    )
    if context_collector is None:
        raise HTTPException(status_code=503, detail="context collector is not ready")

    result = await collect_and_process(investigation, context_collector)
    return {
        "status": "accepted",
        "had_metric_errors": result.had_metric_errors,
        "had_log_errors": result.had_log_errors,
    }


