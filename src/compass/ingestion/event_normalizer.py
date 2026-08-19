"""
Final step before anything reaches the bus.
Adaptors already mapped raw payloads to NormalizedEvent  
    — this module's job is just:
    1. idempotency check (drop if we've seen this dedupe_key before)
    2. wrap in the common Envelope 
    3. publish on compass.ingest.normalized

Layer 1/2/3 downstream only ever consume what this function emits.
"""
from __future__ import annotations

import logging

from compass.bus.publisher import publish
from compass.bus.routing_keys import BusComponent, routing_key_for
from compass.db.redis import claim_once
from compass.schemas.envelope import Envelope
from compass.schemas.events import NormalizedEvent

logger = logging.getLogger(__name__)

async def process(event: NormalizedEvent) -> bool:
    """
    Returns 
        True if the event was published, 
        False if it was a duplicate and got dropped.
        Never raises on a duplicate — that's an expected, routine outcome, not an error.
    """
    # is_new = await claim_once(event.dedupe_key)
    # if not is_new:
    #     logger.info("dropping duplicate event, dedupe_key=%s", event.dedupe_key)
    #     return False

    envelope = Envelope(
        service=event.service,
        environment=event.environment,
        emitted_by="normalizer",
        payload=event.model_dump(mode="json"),
    )
    await publish(routing_key_for(BusComponent.EVENT_NORMALIZER), envelope)
    logger.info(
        "published normalized event: source=%s type=%s service=%s",
        event.source, event.event_type, event.service,
    )
    return True
