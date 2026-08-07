from __future__ import annotations


import aio_pika

from compass.config import settings
from compass.schemas.envelope import Envelope

_connection: aio_pika.RobustConnection | None = None
_exchange: aio_pika.Exchange | None = None

EXCHANGE_NAME = "compass"


async def get_exchange() -> aio_pika.Exchange:
    global _connection, _exchange
    if _exchange is None:
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        channel = await _connection.channel()
        _exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
        )
    return _exchange


async def publish(routing_key: str, envelope: Envelope) -> None:
    """
    Publishes one envelope under the given routing key .
    Callers pass fully-qualified keys, e.g. 'compass.ingest.normalized' or
    'compass.cmd.layer1.evaluate' — this function doesn't validate the key
    naming convention, that's a code-review concern, not a runtime one.
    """
    exchange = await get_exchange()
    body = envelope.model_dump_json().encode("utf-8")
    message = aio_pika.Message(
        body=body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await exchange.publish(message, routing_key=routing_key)