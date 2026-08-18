"""
Collector owns backpressure: if the normalizer/bus is slow, this buffers
rather than dropping or blocking the source. A bounded asyncio.Queue does
that naturally — `put()` blocks the producer once the queue is full, which
for a push adaptor means the webhook handler's background task waits
briefly rather than the event being lost.

One Collector instance per process, started once at app startup and
stopped on shutdown.
"""

from __future__ import annotations

import asyncio
import logging

from compass.ingestion import event_normalizer
from compass.ingestion.adaptors.base import AdaptorError
from compass.schemas.events import NormalizedEvent

logger = logging.getLogger(__name__)


class Collector:
    def __init__(self, max_queue_size: int = 1000, num_workers: int = 4):
        self._queue: asyncio.Queue[NormalizedEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._num_workers = num_workers
        self._workers: list[asyncio.Task] = []

    async def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._worker_loop(worker_id=i))
            for i in range(self._num_workers)
        ]
        logger.info("collector started with %d workers", self._num_workers)

    async def stop(self) -> None:
        await self._queue.join()
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        logger.info("collector stopped")

    async def ingest(self, event: NormalizedEvent) -> None:
        """
        Called by webhook routes / watch stream handlers after an adaptor
        has already normalized a raw payload. Blocks (applies backpressure)
        if the queue is full rather than dropping the event.
        """
        await self._queue.put(event)

    async def ingest_raw(self, adaptor, raw: dict) -> None:
        """
        Convenience path for callers that haven't normalized yet — runs
        the adaptor's normalize() and enqueues on success. Malformed
        payloads are logged and dropped here rather than crashing the
        webhook handler.
        """
        try:
            event = adaptor.normalize(raw)
        except AdaptorError:
            logger.exception("failed to normalize payload from source=%s", adaptor.source)
            return
        await self.ingest(event)

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            event = await self._queue.get()
            try:
                await event_normalizer.process(event)
            except Exception:
                logger.exception(
                    "worker %d failed processing event dedupe_key=%s",
                    worker_id, event.dedupe_key,
                )
            finally:
                self._queue.task_done()


# module-level singleton, wired up in app lifespan (see main.py)
collector = Collector()