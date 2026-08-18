"""
Standalone script usage (no FastAPI) — create the adapter once, call
`query()` as many times as needed, `aclose()` once when done.

For FastAPI wiring (lifespan + Depends, including the combined
Prometheus+Loki case), see fastapi_integration.py and
fastapi_app_example.py.
"""
from __future__ import annotations

import asyncio
import json

from .loki_adaptor import LokiAdaptor
from .loki_models import LogSignalType

LOKI_URL = "http://localhost:3100"


async def context_builder_pattern() -> None:
    adapter = LokiAdaptor(LOKI_URL)

    context = await adapter.query(
        service="checkout-api", environment="prod", window_seconds=300
    )
    print("Log-derived context for anomaly model:")
    print(json.dumps(context, indent=2))

    await adapter.aclose()


async def fleet_wide_pattern() -> None:
    adapter = LokiAdaptor(LOKI_URL)
    schema = await adapter.get_schema()
    print(f"group_label={schema.group_label} base_selector={schema.stream_selector_base}")

    result = await adapter.collect(
        signals=(
            LogSignalType.EXCEPTION_RATE,
            LogSignalType.FATAL_RATE,
            LogSignalType.OOM_SIGNAL,
            LogSignalType.DEPENDENCY_CONNECTION_ERRORS,
        ),
        window="5m",
    )
    print(json.dumps(result.to_normalized_dict(), indent=2))
    await adapter.aclose()


if __name__ == "__main__":
    asyncio.run(context_builder_pattern())
