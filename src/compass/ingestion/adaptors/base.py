"""
Every adaptor implements this contract. the adaptor is the
only place that knows what a GitHub payload or a k8s watch event looks like.

Two shapes, same interface:
  - Push adaptors (github_argocd, kubernetes_watch): fed by a webhook route
    or a watch stream. normalize() is called synchronously on receipt.
  - Pull adaptors (prometheus, loki): NOT trigger sources. They expose
    query() instead of participating in fetch()/normalize(), and are called
    on demand by the Soak-window manager / Context Builder.
"""
from __future__ import annotations


from compass.schemas.events import NormalizedEvent
from abc import ABC, abstractmethod


class SourceAdapter(ABC):
    """Base class for all adapters."""
    source: str

class PushAdapter(SourceAdapter):
    """Adaptor for discrete, event-driven sources."""

    @abstractmethod
    def normalize(self, raw: dict) -> NormalizedEvent:
        """Map one raw payload onto the canonical event shape."""
        pass

class PullAdapter(SourceAdapter):
    """Adaptor for sources polled on demand (metrics, logs). Not a trigger source."""

    @abstractmethod
    async def query(
        self,
        service: str,
        environment: str,
        window_seconds: int,
    ) -> dict:
        """Pull data for a bounded window. Called by soak-window manager / context builder."""
        pass
    
    
class AdaptorError(Exception):
    """Raised when a raw payload can't be mapped to a NormalizedEvent."""