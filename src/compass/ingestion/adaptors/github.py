"""
Push adaptor for GitHub deployment_status webhooks.
"""
from __future__ import annotations

from compass.ingestion.adaptors.base import AdaptorError,PushAdapter
from compass.schemas.events import EventSource, EventType, NormalizedEvent

_DEPLOY_EVENT_MAP = {
    "success": EventType.DEPLOY_SUCCEEDED,
    "failure": EventType.DEPLOY_FAILED,
    "in_progress": EventType.DEPLOY_STARTED,
}

class GitHubActionsAdaptor(PushAdapter):
    source = "githubActions"

    def normalize(self, raw: dict) -> NormalizedEvent:
        """
        raw is a GitHub deployment_status webhook payload, e.g.:
            {
            "status": "success",
            "app_name": "compass-core",
            "namespace": "compass-apps",
            "environment": "prod",
            "deployment": {
                "id": 123456789,
                "sha": "abc123def456",
                "run_id": "987654321"
            },
            "repository": {
                "name": "payments-api",
                "owner": "my-org",
                "branch": "main"
            },
            "commit_sha": "abc123def456",
            "previous_revision": "789xyz123abc",
            "triggered_by": "developer-username"
            }
        """
        try:
            deployment = raw["deployment"]
            status = raw["status"]
            event_type = _DEPLOY_EVENT_MAP.get(status)
            if event_type is None:
                raise AdaptorError(f"unrecognized action: {status!r}")

            return NormalizedEvent(
                source=EventSource.GITHUB,
                event_type=event_type,
                raw_ref=f"github:deployment:{deployment['id']}",
                service=raw.get("app_name") or raw["repository"]["name"],
                environment=raw.get("environment") or deployment.get("environment", "prod"),
                deployment_id=str(deployment["id"]),
                commit_sha=raw.get("commit_sha") or deployment.get("sha"),
                dedupe_key=f"github:{deployment['id']}:{status}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed GitHub webhook payload, missing {e}") from e