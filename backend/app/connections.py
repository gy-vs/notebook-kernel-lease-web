"""One websocket per browser tab.

The connection is just a transport: it carries a session id (control lock
identity) but is not itself the lock, the kernel or an execution.  Losing it
(reload, short network blip) changes none of those.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import WebSocket, WebSocketDisconnect

from . import config


class Connection:
    def __init__(self, ws: WebSocket) -> None:
        self.ws = ws
        self.send_lock = asyncio.Lock()
        self.alive = True

    async def send(self, message: dict) -> None:
        if not self.alive:
            return
        try:
            async with self.send_lock:
                await self.ws.send_json(message)
        except Exception:
            self.alive = False


async def serve(websocket: WebSocket, hub) -> None:
    await websocket.accept()
    conn = Connection(websocket)
    session_id: str | None = None
    pong_deadline: float | None = None
    ping_task: asyncio.Task | None = None

    try:
        # First message must be a hello carrying any existing session id.
        raw = await websocket.receive_text()
        hello = json.loads(raw)
        if hello.get("type") != "hello":
            await conn.send({"type": "error", "error": "expected_hello"})
            return
        session_id = hello.get("session_id")

        info = hub.register_connection(session_id, conn)
        session_id = info["session_id"]
        await conn.send({"type": "hello_ack", "session_id": session_id,
                         "role": info["role"]})
        await conn.send(await hub.snapshot())

        async def ping_loop():
            nonlocal pong_deadline
            while conn.alive:
                await asyncio.sleep(config.PING_INTERVAL_SECONDS)
                pong_deadline = asyncio.get_running_loop().time() + config.PING_TIMEOUT_SECONDS
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    conn.alive = False
                    return

        ping_task = asyncio.create_task(ping_loop())

        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await conn.send({"type": "error", "error": "bad_json"})
                continue

            mtype = msg.get("type")
            if mtype == "pong":
                pong_deadline = None
                continue
            if mtype == "acquire":
                await hub.request_acquire(conn, session_id)
            elif mtype == "release":
                await hub.request_release(conn, session_id)
            elif mtype == "execute":
                await hub.execute(
                    conn, session_id, msg.get("token"), msg.get("req_id"),
                    msg.get("cell_id"), msg.get("source"),
                )
            elif mtype == "interrupt":
                await hub.interrupt(conn, session_id, msg.get("token"),
                                    msg.get("req_id"))
            elif mtype == "restart":
                await hub.restart(conn, session_id, msg.get("token"),
                                  msg.get("req_id"))
            elif mtype == "query_submission":
                await hub.query_submission(conn, msg.get("req_id"))
            elif mtype == "cell_update":
                await hub.cell_update(msg.get("cell_id"), msg.get("source", ""))
            elif mtype == "cell_add":
                await hub.cell_add(conn, msg.get("cell_id"), msg.get("after_id"))
            elif mtype == "cell_delete":
                await hub.cell_delete(msg.get("cell_id"))
            else:
                await conn.send({"type": "error", "error": f"unknown_type:{mtype}"})

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        conn.alive = False
        if ping_task:
            ping_task.cancel()
        if session_id is not None:
            hub.connection_closed(conn)
