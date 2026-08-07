"""
Push adaptor for Grafana's unified alerting webhook contact point.

One adaptor covers alerts sourced from Prometheus metrics AND Loki log
queries — by the time an alert rule fires, Grafana has already evaluated
it and emits the same payload shape regardless of which datasource backed
the rule. The datasource only shows up as a label (e.g. labels.datasource
or labels.rulename convention), never a different schema. This mirrors
Section 1.2: "Alertmanager firing" is one trigger-event family.

A single webhook call can carry multiple alerts (Grafana batches by
group), so this adaptor returns a list, not one NormalizedEvent — the
one exception among the push adaptors, and callers need to iterate.
"""
from __future__ import annotations

from compass.ingestion.adaptors.base import AdaptorError,SourceAdapter
from compass.schemas.events import EventSource, EventType, NormalizedEvent

_STATUS_MAP = {
    "firing": EventType.ALERT_FIRED,
    "resolved": EventType.ALERT_RESOLVED,
}

# Label keys checked in order — lets you standardize on whichever your
# alert rules actually set without hardcoding one convention.
_SERVICE_LABEL_KEYS = ("service", "app", "job")
_ENV_LABEL_KEYS = ("environment", "env", "namespace")


class GrafanaAlertAdaptor(SourceAdapter):
    source = "grafana"

    def parse_batch(self, raw: dict) -> list[NormalizedEvent]:
        """
        raw is a Grafana unified-alerting webhook payload, e.g.:
        {
          "receiver": "compass-webhook",
          "status": "firing",
          "alerts": [
            {
              "status": "firing",
              "labels": {"alertname": "HighErrorRate", "service": "payments-api",
                         "environment": "prod", "datasource": "prometheus"},
              "annotations": {"summary": "Error rate above 5% for 5m"},
              "startsAt": "2026-08-07T10:00:00Z",
              "endsAt": "0001-01-01T00:00:00Z",
              "fingerprint": "a1b2c3d4",
              "generatorURL": "https://grafana/alerting/...",
              "dashboardURL": "https://grafana/d/...",
              "panelURL": "https://grafana/d/...?viewPanel=..."
            }
          ],
          "groupLabels": {"alertname": "HighErrorRate"},
          "commonLabels": {...},
          "externalURL": "https://grafana",
          "version": "1",
          "groupKey": "..."
        }

        Each entry in "alerts" becomes its own NormalizedEvent — a batch
        can mix firing and resolved alerts for the same rule group.
        """
        try:
            alerts = raw["alerts"]
        except KeyError as e:
            raise AdaptorError(f"malformed Grafana webhook payload, missing {e}") from e

        events: list[NormalizedEvent] = []
        for alert in alerts:
            events.append(self._normalize_one(alert))
        return events

    def _normalize_one(self, alert: dict) -> NormalizedEvent:
        try:
            labels = alert["labels"]
            status = alert["status"]
            fingerprint = alert["fingerprint"]

            event_type = _STATUS_MAP.get(status)
            if event_type is None:
                raise AdaptorError(f"unrecognized alert status: {status!r}")

            service = self._first_present(labels, _SERVICE_LABEL_KEYS)
            if service is None:
                raise AdaptorError(
                    f"alert {fingerprint} has none of {_SERVICE_LABEL_KEYS} in labels — "
                    "add one to the alert rule so Compass can route it"
                )
            environment = self._first_present(labels, _ENV_LABEL_KEYS) or "prod"

            return NormalizedEvent(
                source=EventSource.GRAFANA,
                event_type=event_type,
                raw_ref=f"grafana:alert:{fingerprint}",
                service=service,
                environment=environment,
                dedupe_key=f"grafana:{fingerprint}:{status}:{alert.get('startsAt')}",
            )
        except KeyError as e:
            raise AdaptorError(f"malformed Grafana alert entry, missing {e}") from e

    @staticmethod
    def _first_present(labels: dict, keys: tuple[str, ...]) -> str | None:
        for key in keys:
            if key in labels:
                return labels[key]
        return None