from fastapi import FastAPI
import uvicorn

app = FastAPI(
    title="Compass API",
    version="1...0",
    description="Prototype API built with FastAPI For DevopsDays hackthon 2026",
)


@app.get("/health")
def health_check():
    return {"status": "ok"}


def main():
    uvicorn.run("compass.main:app", host="0.0.0.0", port=8000, reload=True)