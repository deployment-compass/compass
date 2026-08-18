from __future__ import annotations

from pydantic_settings import BaseSettings , SettingsConfigDict


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@localhost/"

    # Prometheus Configurations
    prometheus_url: str = "http://localhost:9090"
    prometheus_timeout_seconds: float = 5.0
    prometheus_cache_ttl_seconds: int = 300 
    
    # loki Configurations
    loki_url: str = "http://localhost:3100"
    loki_timeout_seconds: float = 5.0
    loki_cache_ttl_seconds: int = 300 
    
    
    
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()