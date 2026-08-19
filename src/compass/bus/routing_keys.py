"""Central definitions and lookup helpers for Compass bus routing keys.

Components should ask this module for their routing key instead of embedding
broker-specific strings throughout the application.  This also makes a
component's output route explicit and easy to change in one place.
"""
from __future__ import annotations

from enum import StrEnum


class BusComponent(StrEnum):
    """Components which emit events on the Compass topic exchange."""

    EVENT_NORMALIZER = "event_normalizer"
    INVESTIGATION_WINDOW = "investigation_window"
    TRAINING_TRIGGER = "training_trigger"


class RoutingKeys:
    """The canonical topic routes used by Compass."""

    INGEST_NORMALIZED = "compass.ingest.normalized"
    # Kept deliberately spelled ``anomly`` to match the Layer 2 contract.
    INGEST_NORMALIZED_ANOMLY = "compass.ingest.normalized.anomly"
    TRAINING_TRIGGER = "compass.training.trigger"


_COMPONENT_ROUTING_KEYS: dict[BusComponent, str] = {
    BusComponent.EVENT_NORMALIZER: RoutingKeys.INGEST_NORMALIZED,
    BusComponent.INVESTIGATION_WINDOW: RoutingKeys.INGEST_NORMALIZED_ANOMLY,
    BusComponent.TRAINING_TRIGGER: RoutingKeys.TRAINING_TRIGGER,
}


def routing_key_for(component: BusComponent | str) -> str:
    """Return the configured output route for *component*.

    Accepting strings keeps integration points simple while rejecting unknown
    components before a message can be sent to an unintended topic.
    """
    try:
        component = BusComponent(component)
    except ValueError as exc:
        valid_components = ", ".join(item.value for item in BusComponent)
        raise ValueError(
            f"unknown bus component {component!r}; expected one of: {valid_components}"
        ) from exc
    return _COMPONENT_ROUTING_KEYS[component]
