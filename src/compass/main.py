from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from compass.api import webhooks
from compass.ingestion.collector import collector

import uvicorn


@asynccontextmanager
async def lifespan(compass: FastAPI):
    await collector.start()
    yield
    await collector.stop()

app = FastAPI(
    title="Compass",
    version="1.0.0",
    description="Prototype built with FastAPI For DevopsDays hackthon 2026",
    lifespan=lifespan)




    
app.include_router(webhooks.router)



@app.get("/health")
def health_check():
    return {"status": "ok"}


def main():
    uvicorn.run("compass.main:app", host="0.0.0.0", port=8000, reload=True)