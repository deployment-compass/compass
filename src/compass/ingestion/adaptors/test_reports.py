"""
Push adaptor for test-run results attached to a deployment event
(e.g. a CI job posting its JUnit/pytest summary after a deploy).
"""
from __future__ import annotations

from compass.ingestion.adaptors.base import AdaptorError,PushAdapter
from compass.schemas.events import EventSource, EventType, NormalizedEvent


class TestReportAdaptor(PushAdapter):
    source = "test_report"

    def normalize(self, raw: dict) -> NormalizedEvent:
        """
        raw shape, e.g.:
        {
          "deployment_id": "8123",
          "service": "payments-api",
          "environment": "prod",
          "commit_sha": "abc123",
          "summary": {"total": 240, "failed": 3, "passed": 237}
        }
        """
        try:
            return NormalizedEvent(
                source=EventSource.TEST_REPORT,
                event_type=EventType.TEST_RESULT,
                raw_ref=f"test_report:{raw['deployment_id']}",
                service=raw["service"],
                environment=raw.get("environment", "prod"),
                deployment_id=str(raw["deployment_id"]),
                commit_sha=raw.get("commit_sha"),
                dedupe_key=f"test_report:{raw['deployment_id']}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed test report payload, missing {e}") from e