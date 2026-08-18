"""
Push-source entry points. Each route validates just enough to hand off,
normalizes synchronously (it's pure/cheap — no I/O), then enqueues on the
collector and returns immediately. The collector's bounded queue is what
absorbs any slowness downstream, not this route.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request, status

from compass.ingestion.adaptors.github import GitHubActionsAdaptor
from compass.ingestion.adaptors.argocd import ArgoCDAdaptor
from compass.ingestion.adaptors.kubernetes_watch import KubernetesWatchAdaptor
from compass.ingestion.adaptors.base import AdaptorError
from compass.src.compass.ingestion.event_collector import collector

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


