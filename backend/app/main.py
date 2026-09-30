"""FastAPI entry point.

Run:  python3 -m app.main
Serves the API/websocket and, in production, the built frontend from
frontend/dist.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket

from . import config
from .connections import serve
from .hub import Hub

hub = Hub()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await hub.startup()
    try:
        yield
    finally:
        await hub.shutdown()


app = FastAPI(title="Local Python Lab", lifespan=lifespan)


@app.get("/api/health")
async def health():
    return {"ok": True, "gen": hub.gen, "kernel_state": hub.kernel_state}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await serve(websocket, hub)


# Built frontend (optional during development; vite dev server otherwise).
DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if DIST.is_dir():
    from fastapi.staticfiles import StaticFiles

    app.mount("/", StaticFiles(directory=str(DIST), html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        ws_ping_interval=10,
        ws_ping_timeout=20,
    )
