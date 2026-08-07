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


class GitHubAdaptor(PushAdapter):
    source = "github"

    def normalize(self, raw: dict) -> NormalizedEvent:
        """
        raw is a GitHub deployment_status webhook payload, e.g.:
        {
          "action": "created",
          "deployment": {"id": 123, "sha": "abc123", "environment": "prod"},
          "deployment_status": {"state": "success", "description": "..."},
          "repository": {"name": "payments-api"},
          "commits": [{"message": "fix: retry logic"}]
        }
        """
        try:
            deployment = raw["deployment"]
            status = raw["deployment_status"]["state"]
            event_type = _DEPLOY_EVENT_MAP.get(status)
            if event_type is None:
                raise AdaptorError(f"unrecognized deployment_status.state: {status!r}")

            commits = raw.get("commits", [])
            changed_files = [f for c in commits for f in c.get("files", [])]
            commit_message = commits[0]["message"] if commits else None

            return NormalizedEvent(
                source=EventSource.GITHUB,
                event_type=event_type,
                raw_ref=f"github:deployment:{deployment['id']}",
                service=raw["repository"]["name"],
                environment=deployment.get("environment", "prod"),
                deployment_id=str(deployment["id"]),
                commit_sha=deployment.get("sha"),
                changed_files=changed_files,
                commit_message=commit_message,
                dedupe_key=f"github:{deployment['id']}:{status}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed GitHub webhook payload, missing {e}") from e