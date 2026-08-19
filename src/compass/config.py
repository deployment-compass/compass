# compass/config.py
from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="COMPASS_")

    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@localhost/"

    prometheus_url: str = "http://localhost:9090"
    prometheus_timeout_seconds: float = 5.0
    prometheus_cache_ttl_seconds: int = 300

    loki_url: str = "http://localhost:3100"
    loki_timeout_seconds: float = 5.0
    loki_cache_ttl_seconds: int = 300

    # Kubernetes in-process watcher
    # Set k8s_watch_enabled=True to run a watch loop inside Compass itself
    # instead of relying on an external sidecar relay.
    k8s_watch_enabled: bool = False
    # Namespaces to watch. Empty list = cluster-wide (requires ClusterRole).
    k8s_namespaces: list[str] = []
    # Path to kubeconfig. None = auto-detect (in-cluster token, then ~/.kube/config).
    k8s_kubeconfig: str | None = None
    # Seconds to wait before reconnecting a broken watch stream.
    k8s_watch_reconnect_delay_seconds: float = 5.0


settings = Settings()