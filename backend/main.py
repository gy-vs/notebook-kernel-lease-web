"""FastAPI 入口：REST + WebSocket，前端构建产物静态托管。

所有“会改变状态”的接口都经过 LockManager.require —— 后端是控制权的唯一裁决者。
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .bus import EventBus
from .db import DB
from .kernel import KernelService
from .lock import LockError, LockManager

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("app")


class State:
    db: DB
    bus: EventBus
    lock: LockManager
    kernel: KernelService


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = DB()
    await db.connect()
    bus = EventBus()
    lock = LockManager(db, bus)
    await lock.start()
    kernel = KernelService(db, bus)

    await db.mark_orphans()
    await db.insert_initial_cell_if_empty()

    state.db = db
    state.bus = bus
    state.lock = lock
    state.kernel = kernel
    try:
        yield
    finally:
        await kernel.shutdown_on_exit()
        await lock.stop()
        await db.close()


app = FastAPI(title="local-lab", lifespan=lifespan)


# ---------------- 模型 ----------------
class Token(BaseModel):
    client_id: str
    epoch: int


class CellCreate(BaseModel):
    token: Token
    source: str = ""
    after_id: str | None = None


class CellUpdate(BaseModel):
    token: Token
    source: str


class ExecuteBody(BaseModel):
    token: Token
    cell_id: str
    request_id: str


class ActionBody(BaseModel):
    token: Token


# ---------------- 锁校验 ----------------
async def require_token(token: Token) -> None:
    try:
        await state.lock.require_async(token.client_id, token.epoch)
    except LockError as e:
        raise HTTPException(status_code=409, detail={
            "code": e.code,
            "lock": e.snapshot,
        })


# ---------------- 快照 ----------------
async def build_snapshot() -> dict:
    cells = await state.db.list_cells()
    gens = await state.db.list_gens()
    executions = await state.db.list_executions()
    messages_by_exec: dict[str, list[dict]] = {}
    for d in await state.db.all_kernel_messages():
        messages_by_exec.setdefault(d["execution_id"], []).append({
            "seq": d["seq"],
            "msg_type": d["msg_type"],
            "content": d["content"],
            "gen_id": d["gen_id"],
        })
    retained = await state.db.retained_message_counts()
    max_seq = await state.db.max_event_seq()
    for ex in executions:
        ex["messages_retained"] = retained.get(ex["id"], 0)
    current_gen = await state.kernel.current_gen_id()
    kernel_alive = await state.kernel.is_alive()
    return {
        "cells": cells,
        "gens": gens,
        "executions": executions,
        "messages": messages_by_exec,
        "max_seq": max_seq,
        "lock": state.lock.snapshot(),
        "current_gen_id": current_gen,
        "kernel_alive": kernel_alive,
        "config": {
            "lease_seconds": config.LEASE_SECONDS,
            "heartbeat_seconds": config.HEARTBEAT_SECONDS,
            "disconnect_grace": config.DISCONNECT_GRACE,
            "max_kernel_messages": config.MAX_KERNEL_MESSAGES,
        },
    }


@app.get("/api/state")
async def get_state() -> dict:
    return await build_snapshot()


@app.get("/api/requests/{request_id}")
async def query_request(request_id: str) -> dict:
    """提交执行后没拿到响应时，用 request_id 查询真实状态（防重复执行）。"""
    ex = await state.db.get_execution_by_request(request_id)
    if ex is None:
        raise HTTPException(status_code=404, detail={"code": "UNKNOWN_REQUEST"})
    return {"execution": ex}


# ---------------- 单元编辑 ----------------
@app.post("/api/cells")
async def create_cell(body: CellCreate) -> dict:
    await require_token(body.token)
    cell = await state.db.create_cell(body.source, body.after_id)
    return {"cell": cell}


@app.patch("/api/cells/{cell_id}")
async def update_cell(cell_id: str, body: CellUpdate) -> dict:
    await require_token(body.token)
    if await state.db.get_cell(cell_id) is None:
        raise HTTPException(404, "cell not found")
    await state.db.update_cell_source(cell_id, body.source)
    return {"ok": True}


@app.post("/api/cells/{cell_id}/delete")
async def delete_cell_post(cell_id: str, body: CellUpdate) -> dict:
    await require_token(body.token)
    await state.db.delete_cell(cell_id)
    return {"ok": True}


# ---------------- 执行 / 中断 / 重启 ----------------
@app.post("/api/execute")
async def execute(body: ExecuteBody) -> dict:
    await require_token(body.token)
    existing = await state.db.get_execution_by_request(body.request_id)
    if existing is not None:
        # 同一个 request_id 永远只对应一次执行（幂等）
        return {"execution": existing, "deduped": True}
    cell = await state.db.get_cell(body.cell_id)
    if cell is None:
        raise HTTPException(404, "cell not found")
    if state.kernel.gen_id is None:
        # 惰性启动：第一个执行创建第一代内核
        await state.kernel.start_fresh()
    gen_id = state.kernel.gen_id
    ex = await state.db.create_execution(body.cell_id, gen_id, cell["source"])
    await state.db.remember_request(body.request_id, ex["id"])
    await state.kernel.submit(ex["id"], cell["source"])
    return {"execution": ex, "deduped": False}


@app.post("/api/interrupt")
async def interrupt(body: ActionBody) -> dict:
    await require_token(body.token)
    await state.kernel.interrupt_current()
    return {"ok": True}


@app.post("/api/restart")
async def restart(body: ActionBody) -> dict:
    await require_token(body.token)
    gen_id = await state.kernel.restart()
    return {"gen_id": gen_id}


# ---------------- 锁（HTTP 兜底，主通道在 WS） ----------------
@app.post("/api/lock/heartbeat")
async def lock_heartbeat(body: Token) -> dict:
    try:
        snap = await state.lock.heartbeat(body.client_id, body.epoch)
    except LockError as e:
        raise HTTPException(409, detail={"code": e.code, "lock": e.snapshot})
    return {"lock": snap}


# ---------------- WebSocket ----------------
@app.websocket("/api/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    # 先订阅再握手，避免事件在订阅前丢失
    q = state.bus.subscribe()
    await ws.accept()
    client_id: str | None = None
    hello = {
        "type": "hello",
        "server_time": time.time(),
        "config": {
            "lease_seconds": config.LEASE_SECONDS,
            "heartbeat_seconds": config.HEARTBEAT_SECONDS,
            "disconnect_grace": config.DISCONNECT_GRACE,
            "max_kernel_messages": config.MAX_KERNEL_MESSAGES,
        },
        "lock": state.lock.snapshot(),
    }
    await ws.send_json(hello)

    async def send_lock() -> None:
        await ws.send_json({"type": "lock", "lock": state.lock.snapshot()})

    async def handle(msg: dict) -> None:
        nonlocal client_id
        mtype = msg.get("type")
        if mtype == "hello":
            client_id = str(msg.get("client_id") or "") or None
            await send_lock()
        elif mtype == "sync":
            after = int(msg.get("after_seq", 0))
            missed = await state.db.events_after(after)
            for ev in missed:
                await ws.send_json({
                    "type": "event",
                    "seq": ev["seq"],
                    "kind": ev["kind"],
                    "data": ev["data"],
                })
            await ws.send_json({"type": "synced", "seq": missed[-1]["seq"]
                                if missed else after})
        elif mtype == "heartbeat":
            cid, epoch = msg.get("client_id"), int(msg.get("epoch", -1))
            try:
                await state.lock.heartbeat(cid, epoch)
            except LockError as e:
                await ws.send_json({"type": "lock_error",
                                    "code": e.code, "lock": e.snapshot})
        elif mtype == "acquire":
            cid = str(msg.get("client_id") or "")
            wait = bool(msg.get("wait", True))
            try:
                await state.lock.acquire(cid, wait=wait)
            except LockError as e:
                await ws.send_json({"type": "lock_error",
                                    "code": e.code, "lock": e.snapshot})
        elif mtype == "release":
            cid, epoch = msg.get("client_id"), int(msg.get("epoch", -1))
            try:
                await state.lock.release(cid, epoch)
            except LockError as e:
                await ws.send_json({"type": "lock_error",
                                    "code": e.code, "lock": e.snapshot})

    async def pump_bus() -> None:
        while True:
            message = await q.get()
            await ws.send_json(message)

    async def pump_ws() -> None:
        while True:
            msg = await ws.receive_json()
            await handle(msg)

    bus_task = asyncio.create_task(pump_bus())
    ws_task = asyncio.create_task(pump_ws())
    try:
        done, pending = await asyncio.wait(
            {bus_task, ws_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                log.debug("ws task ended: %r", exc)
    finally:
        state.bus.unsubscribe(q)
        if client_id:
            await state.lock.mark_disconnected(client_id)


# ---------------- 静态前端 ----------------
if config.STATIC_DIR.exists():
    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(config.STATIC_DIR / "index.html")

    app.mount("/assets",
              StaticFiles(directory=config.STATIC_DIR / "assets"),
              name="assets")

    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str) -> FileResponse:
        f = config.STATIC_DIR / full_path
        if f.is_file():
            return FileResponse(f)
        return FileResponse(config.STATIC_DIR / "index.html")
