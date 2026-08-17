"""
Canonical event shape (Section 9.1). Layer 1/2/3 never see a source-native
payload — only this. New sources add new EventSource / EventType members;
nothing downstream branches on source type.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EventSource(str, Enum):
    KUBERNETES = "kubernetes"
    GITHUB = "github"
    ARGOCD = "argocd"
    PROMETHEUS = "prometheus"
    LOKI = "loki"
    TEST_REPORT = "test_report"


class EventType(str, Enum):
    CRASH_LOOP_BACKOFF = "CrashLoopBackOff"
    OOM_KILLED = "OOMKilled"
    IMAGE_PULL_BACKOFF = "ImagePullBackOff"
    DEPLOY_STARTED = "deploy_started"
    DEPLOY_SUCCEEDED = "deploy_succeeded"
    DEPLOY_FAILED = "deploy_failed"
    ARGOCD_SYNC_STATUS = "argocd_sync_status"
    ALERT_FIRED = "alert_fired"
    ALERT_RESOLVED = "alert_resolved"
    TEST_RESULT = "test_result"


class NormalizedEvent(BaseModel):
    source: EventSource
    event_type: EventType
    raw_ref: str = Field(..., description="pointer/id back to the original source payload")
    service: str
    environment: str = "prod"
    deployment_id: Optional[str] = None
    commit_sha: Optional[str] = None
    dedupe_key: str = Field(..., description="stable per-event id used for idempotency")