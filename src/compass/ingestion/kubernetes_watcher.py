"""
In-process Kubernetes watch loop.

Opens a Watch stream against the K8s API server (in-cluster or kubeconfig),
listens for Pod phase changes that indicate failure, and feeds each raw
watch event directly into the existing ingestion pipeline via
``collector.ingest_raw()``.

Only three event reasons survive the adaptor's normalize() call:
    - CrashLoopBackOff
    - OOMKilled
    - ImagePullBackOff

All other pod watch events raise AdaptorError inside ingest_raw() and are
silently dropped — no extra filtering is needed here.

Lifecycle
---------
    watcher = KubernetesWatcher(...)
    await watcher.start()   # spawns background task(s)
    ...
    await watcher.stop()    # cancels tasks, cleans up K8s client

Auth
----
Auto-detected by kubernetes-asyncio (same order as kubectl):
  1. In-cluster ServiceAccount token (``/var/run/secrets/kubernetes.io/...``)
  2. KUBECONFIG env-var path
  3. ``~/.kube/config``
Pass ``kubeconfig`` explicitly to override.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from compass.ingestion.adaptors.kubernetes_watch import KubernetesWatchAdaptor
from compass.ingestion.event_collector import collector

logger = logging.getLogger(__name__)


class KubernetesWatcher:
    """
    Async background watcher that streams pod events from the Kubernetes API
    and injects them into the Compass ingestion pipeline.

    Parameters
    ----------
    namespaces:
        List of namespaces to watch. Pass an empty list (the default) to watch
        all namespaces cluster-wide (requires a ClusterRole that permits
        ``list`` + ``watch`` on ``pods`` across all namespaces).
    kubeconfig:
        Absolute path to a kubeconfig file.  ``None`` triggers auto-detection:
        in-cluster ServiceAccount token first, then ``~/.kube/config``.
    reconnect_delay_seconds:
        Base delay before reconnecting a broken stream. Doubles on each
        consecutive failure up to ``_MAX_RECONNECT_DELAY_SECONDS``.
    """

    _MAX_RECONNECT_DELAY_SECONDS: float = 60.0

    def __init__(
        self,
        namespaces: Sequence[str] | None = None,
        kubeconfig: str | None = None,
        reconnect_delay_seconds: float = 5.0,
    ) -> None:
        self._namespaces: list[str] = list(namespaces) if namespaces else []
        self._kubeconfig = kubeconfig
        self._base_delay = reconnect_delay_seconds
        self._adaptor = KubernetesWatchAdaptor()
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Public lifecycle API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn background watch tasks and return immediately."""
        if self._namespaces:
            for ns in self._namespaces:
                task = asyncio.create_task(
                    self._run_with_retry(namespace=ns),
                    name=f"k8s-watcher:{ns}",
                )
                self._tasks.append(task)
            logger.info(
                "kubernetes watcher started for namespaces: %s",
                self._namespaces,
            )
        else:
            task = asyncio.create_task(
                self._run_with_retry(namespace=None),
                name="k8s-watcher:cluster-wide",
            )
            self._tasks.append(task)
            logger.info("kubernetes watcher started (cluster-wide)")

    async def stop(self) -> None:
        """Cancel all watch tasks and close the K8s API client."""
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self._close_k8s_client()
        logger.info("kubernetes watcher stopped")

    # ------------------------------------------------------------------
    # Internal: reconnect loop
    # ------------------------------------------------------------------

    async def _run_with_retry(self, namespace: str | None) -> None:
        """
        Wraps _watch_namespace() with exponential-backoff reconnect logic.
        Runs forever until the task is cancelled (i.e., on app shutdown).
        """
        delay = self._base_delay
        consecutive_failures = 0

        while True:
            try:
                await self._watch_namespace(namespace)
                # _watch_namespace only returns if the stream ends cleanly.
                # Treat that as a soft error and reconnect.
                logger.warning(
                    "k8s watch stream ended cleanly for namespace=%s, reconnecting in %.1fs",
                    namespace or "*",
                    delay,
                )
            except asyncio.CancelledError:
                # Shutdown — propagate so the task exits.
                raise
            except Exception:
                consecutive_failures += 1
                logger.exception(
                    "k8s watch stream error for namespace=%s (attempt #%d), "
                    "reconnecting in %.1fs",
                    namespace or "*",
                    consecutive_failures,
                    delay,
                )

            await asyncio.sleep(delay)
            # Exponential back-off capped at _MAX_RECONNECT_DELAY_SECONDS.
            delay = min(delay * 2, self._MAX_RECONNECT_DELAY_SECONDS)

    # ------------------------------------------------------------------
    # Internal: single watch session
    # ------------------------------------------------------------------

    async def _watch_namespace(self, namespace: str | None) -> None:
        """
        Open one Watch session against the K8s API.  This coroutine runs until
        the stream is closed (either by the server or due to an error), at
        which point it returns or raises so _run_with_retry can reconnect.

        Each raw watch event ``{"type": "MODIFIED", "object": {...}}`` is
        forwarded directly to ``collector.ingest_raw()``.  The existing
        ``KubernetesWatchAdaptor.normalize()`` filters out non-failure events
        by raising ``AdaptorError``; ``ingest_raw`` catches those and discards
        them, so no extra filtering is required here.
        """
        # Lazy import so that tests that don't install kubernetes-asyncio
        # can still import this module without an ImportError.
        try:
            from kubernetes_asyncio import client, config, watch  # type: ignore[import]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "kubernetes-asyncio is not installed. "
                "Add 'kubernetes-asyncio>=24.2.3' to your dependencies."
            ) from exc

        # Load credentials once per watch session.
        await self._load_k8s_config(config)

        v1 = client.CoreV1Api()
        w = watch.Watch()

        try:
            if namespace:
                stream = w.stream(
                    v1.list_namespaced_pod,
                    namespace=namespace,
                    timeout_seconds=0,  # 0 = server-side keep-alive, never times out
                )
            else:
                stream = w.stream(
                    v1.list_pod_for_all_namespaces,
                    timeout_seconds=0,
                )

            async for raw_event in stream:
                # raw_event shape: {"type": "ADDED"|"MODIFIED"|"DELETED", "object": V1Pod}
                # Serialize to a plain dict so the existing adaptor can handle it.
                raw_dict = self._serialize_event(raw_event)
                # Only MODIFIED events carry updated container statuses.
                if raw_dict.get("type") != "MODIFIED":
                    continue
                await collector.ingest_raw(self._adaptor, raw_dict)
        finally:
            await v1.api_client.close()

    # ------------------------------------------------------------------
    # Internal: helpers
    # ------------------------------------------------------------------

    async def _load_k8s_config(self, config) -> None:  # type: ignore[no-untyped-def]
        """
        Load K8s credentials via kubernetes-asyncio's auto-detection chain:
          1. Explicit kubeconfig path (if configured)
          2. In-cluster ServiceAccount token
          3. ~/.kube/config (KUBECONFIG env-var or default path)
        """
        if self._kubeconfig:
            await config.load_kube_config(config_file=self._kubeconfig)
            logger.debug("k8s: loaded kubeconfig from %s", self._kubeconfig)
        else:
            try:
                config.load_incluster_config()
                logger.debug("k8s: using in-cluster ServiceAccount token")
            except config.ConfigException:
                await config.load_kube_config()
                logger.debug("k8s: using ~/.kube/config (or KUBECONFIG env-var)")

    async def _close_k8s_client(self) -> None:
        """Best-effort close of any lingering kubernetes-asyncio client."""
        try:
            from kubernetes_asyncio import client  # type: ignore[import]
            await client.ApiClient().close()
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _serialize_event(raw_event: dict) -> dict:
        """
        Convert a kubernetes-asyncio watch event to a plain dict that matches
        the shape expected by KubernetesWatchAdaptor.normalize():

            {
              "type": "MODIFIED",
              "object": {
                "metadata": {
                    "name": ..., "namespace": ...,
                    "resourceVersion": ..., "labels": {...}
                },
                "status": {
                    "containerStatuses": [
                        {"state": {"waiting": {"reason": ...}}}
                    ]
                }
              }
            }

        kubernetes-asyncio returns the "object" field as a deserialized V1Pod
        model. We convert it to a dict via its ``to_dict()`` method, then
        convert snake_case keys to camelCase to match the raw K8s API shape
        that the adaptor was written for.
        """
        event_type = raw_event.get("type", "")
        obj = raw_event.get("object")

        if obj is None:
            return {"type": event_type, "object": {}}

        # kubernetes-asyncio provides .to_dict() on model objects.
        if hasattr(obj, "to_dict"):
            obj_dict = obj.to_dict()
        elif isinstance(obj, dict):
            obj_dict = obj
        else:
            obj_dict = {}

        # Re-shape to the camelCase format the adaptor expects.
        metadata_raw = obj_dict.get("metadata") or {}
        status_raw = obj_dict.get("status") or {}

        metadata = {
            "name": metadata_raw.get("name", ""),
            "namespace": metadata_raw.get("namespace", ""),
            "resourceVersion": metadata_raw.get("resource_version", ""),
            "labels": metadata_raw.get("labels") or {},
        }

        # Serialize containerStatuses from the sdk's snake_case model dicts.
        container_statuses: list[dict] = []
        for cs in status_raw.get("container_statuses") or []:
            state_raw = cs.get("state") or {}
            state: dict = {}

            waiting = state_raw.get("waiting")
            if waiting:
                state["waiting"] = {"reason": waiting.get("reason", "")}

            terminated = state_raw.get("terminated")
            if terminated:
                state["terminated"] = {"reason": terminated.get("reason", "")}

            running = state_raw.get("running")
            if running:
                state["running"] = {}

            container_statuses.append({"state": state})

        return {
            "type": event_type,
            "object": {
                "metadata": metadata,
                "status": {"containerStatuses": container_statuses},
            },
        }
