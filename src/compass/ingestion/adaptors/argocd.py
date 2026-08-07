"""
Push adaptor for Argo CD sync-status webhooks.
"""
from __future__ import annotations

from compass.ingestion.adaptors.base import AdaptorError , PushAdapter
from compass.schemas.events import EventSource, EventType, NormalizedEvent



class ArgoCDAdaptor(PushAdapter):
    source = "argocd"

    def normalize(self, raw: dict) -> NormalizedEvent:
        """
        raw is an Argo CD sync-status webhook, e.g.:
        {
          "application": {"metadata": {"name": "payments-api"}},
          "status": {"sync": {"status": "OutOfSync"}, "operationState": {"syncResult": {"revision": "abc123"}}}
        }
        """
        try:
            app_name = raw["application"]["metadata"]["name"]
            revision = raw["status"]["operationState"]["syncResult"]["revision"]
            sync_status = raw["status"]["sync"]["status"]

            return NormalizedEvent(
                source=EventSource.ARGOCD,
                event_type=EventType.ARGOCD_SYNC_STATUS,
                raw_ref=f"argocd:{app_name}:{revision}",
                service=app_name,
                commit_sha=revision,
                dedupe_key=f"argocd:{app_name}:{revision}:{sync_status}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed Argo CD payload, missing {e}") from e