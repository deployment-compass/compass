"""
Push adaptor fed by a k8s watch stream.
Surfaces CrashLoopBackOff, OOMKilled, ImagePullBackOff.
"""
from __future__ import annotations

from compass.ingestion.adaptors.base import AdaptorError,PushAdapter
from compass.schemas.events import EventSource, EventType, NormalizedEvent

_REASON_MAP = {
    "CrashLoopBackOff": EventType.CRASH_LOOP_BACKOFF,
    "OOMKilled": EventType.OOM_KILLED,
    "ImagePullBackOff": EventType.IMAGE_PULL_BACKOFF,
}


class KubernetesWatchAdaptor(PushAdapter):
    source = "kubernetes"

    def normalize(self, raw: dict) -> NormalizedEvent:
        """
        raw is a single watch event from the pod status stream, e.g.:
        {
          "type": "MODIFIED",
          "object": {
            "metadata": {
              "name": "payments-api-7f8b9c-abcd",
              "namespace": "prod",
              "resourceVersion": "48213192",
              "labels": {"app": "payments-api"}
            },
            "status": {
              "containerStatuses": [
                {"state": {"waiting": {"reason": "CrashLoopBackOff"}}}
              ]
            }
          }
        }
        """
        try:
            obj = raw["object"]
            metadata = obj["metadata"]
            container_statuses = obj.get("status", {}).get("containerStatuses", [])

            reason = None
            for cs in container_statuses:
                waiting = cs.get("state", {}).get("waiting", {})
                terminated = cs.get("state", {}).get("terminated", {})
                if waiting.get("reason") in _REASON_MAP:
                    reason = waiting["reason"]
                    break
                if terminated.get("reason") in _REASON_MAP:
                    reason = terminated["reason"]
                    break

            if reason is None:
                raise AdaptorError("no recognized failure reason in containerStatuses")

            service = metadata.get("labels", {}).get("app", metadata["name"])
            resource_version = metadata["resourceVersion"]

            return NormalizedEvent(
                source=EventSource.KUBERNETES,
                event_type=_REASON_MAP[reason],
                raw_ref=f"k8s:pod:{metadata['namespace']}/{metadata['name']}",
                service=service,
                environment=metadata["namespace"],
                dedupe_key=f"k8s:{metadata['name']}:{resource_version}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed k8s watch event, missing {e}") from e