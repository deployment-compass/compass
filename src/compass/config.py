from __future__ import annotations

from pydantic_settings import BaseSettings , SettingsConfigDict


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@localhost/"
    prometheus_url: str = "http://prometheus:9090"
    loki_url: str = "http://loki:3100"

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()