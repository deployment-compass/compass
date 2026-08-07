"""
Push-source entry points. Each route validates just enough to hand off,
normalizes synchronously (it's pure/cheap — no I/O), then enqueues on the
collector and returns immediately. The collector's bounded queue is what
absorbs any slowness downstream, not this route.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request, status

from compass.ingestion.adaptors.github import GitHubAdaptor
from compass.ingestion.adaptors.argocd import ArgoCDAdaptor
from compass.ingestion.adaptors.kubernetes_watch import KubernetesWatchAdaptor
from compass.ingestion.adaptors.test_reports import TestReportAdaptor
from compass.ingestion.adaptors.grafana_alerts import GrafanaAlertAdaptor
from compass.ingestion.adaptors.base import AdaptorError
from compass.ingestion.collector import collector

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])

_github_adaptor = GitHubAdaptor()
_argocd_adaptor = ArgoCDAdaptor()
_k8s_adaptor = KubernetesWatchAdaptor()
_test_report_adaptor = TestReportAdaptor()
_grafana_adaptor = GrafanaAlertAdaptor()


@router.post("/github", status_code=status.HTTP_202_ACCEPTED)
async def github_webhook(request: Request):
    raw = await request.json()
    try:
        event = _github_adaptor.normalize(raw)
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


@router.post("/test-report", status_code=status.HTTP_202_ACCEPTED)
async def test_report_webhook(request: Request):
    raw = await request.json()
    await collector.ingest_raw(_test_report_adaptor, raw)
    return {"status": "accepted"}


@router.post("/grafana-alerts", status_code=status.HTTP_202_ACCEPTED)
async def grafana_alerts_webhook(request: Request):
    """
    Point Grafana's webhook contact point at this route. A single call can
    batch several alerts (one per rule in the firing group), so this
    fans out to the collector individually — one bad alert entry doesn't
    drop the rest of the batch.
    """
    raw = await request.json()
    try:
        events = _grafana_adaptor.parse_batch(raw)
    except AdaptorError:
        logger.exception("bad grafana webhook payload")
        return {"status": "ignored"}

    accepted = 0
    for event in events:
        try:
            await collector.ingest(event)
            accepted += 1
        except Exception:
            logger.exception("failed to enqueue grafana alert, fingerprint in raw_ref=%s", event.raw_ref)

    return {"status": "accepted", "count": accepted, "total": len(events)}