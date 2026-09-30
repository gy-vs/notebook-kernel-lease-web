"""The Hub: joins persistence, the kernel thread, the control lock and the
websocket connections.  Everything a websocket can do goes through here, so
there is exactly one place where authorization is decided.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from . import config
from .control import Control
from .db import Database
from .kernel import KernelRunner

# Execution statuses considered "the kernel is still working on it".
RUNNING = "running"
# iopub message kinds we persist.
MSG_KINDS = {
    "stream",
    "execute_input",
    "execute_result",
    "display_data",
    "update_display_data",
    "error",
    "clear_output",
}


class Hub:
    def __init__(self) -> None:
        self.db = Database(config.DB_PATH)
        self.runner = KernelRunner()
        self.gen: int = 0
        self.kernel_state: str = "starting"  # starting|idle|busy|restarting|dead
        self._active: set[str] = set()      # execution ids with no terminal reply yet
        self._stopping = False
        self._pump_task: asyncio.Task | None = None
        self.control = Control(self._on_leadership_change)

    # ---- lifecycle ---------------------------------------------------------
    async def startup(self) -> None:
        await self.db.connect()

        # The kernel process is never restored across service restarts: any
        # execution left "running" belonged to a process that is gone.
        await self.db.mark_running_abandoned(time.time())

        gen = await self.db.get_meta("kernel_gen", 0)
        self.gen = int(gen) + 1
        await self.db.set_meta("kernel_gen", self.gen)

        self.runner.start()
        self.runner.wait_ready(timeout=30)
        self.kernel_state = "idle"

        if not await self.db.list_cells():
            cid = uuid.uuid4().hex
            await self.db.create_cell(
                cid,
                time.time(),
                0,
                "# Welcome\n# This is a real, persistent ipykernel.\n"
                "# Variables defined in one cell are live in later cells.\n"
                "x = 6 * 7\nprint('first print happens before any refresh')\nx",
            )

        self._pump_task = asyncio.create_task(self._pump())

    async def shutdown(self) -> None:
        self._stopping = True
        if self._pump_task:
            self._pump_task.cancel()
        self.runner.shutdown()
        await self.db.close()

    # ---- connections -------------------------------------------------------
    def register_connection(self, session_id: str | None, conn) -> dict:
        """Bind the websocket.  Returns {session_id, role payload}."""
        sid = self.control.register(session_id, conn)
        return {"session_id": sid, "role": self.control.role_payload(sid)}

    def connection_closed(self, conn) -> None:
        leaders = self.control.unregister(conn)
        for sid in leaders:
            asyncio.create_task(self._schedule_grace(sid))

    async def _schedule_grace(self, session_id: str) -> None:
        await self.control.leader_disconnected(session_id, asyncio.get_running_loop())

    def broadcast(self, message: dict) -> None:
        for sess in self.control.state.sessions.values():
            if sess.conn is not None:
                asyncio.create_task(sess.conn.send(message))

    def _on_leadership_change(self, old_id: str | None, new_id: str | None, reason: str) -> None:
        payload = {"type": "control_changed", "reason": reason}
        payload.update(self.control.snapshot())
        # Every tab re-evaluates its role; the old leader specifically sees
        # role=viewer and must stand down even if its UI disagreed.
        for sess in self.control.state.sessions.values():
            if sess.conn is not None:
                msg = dict(payload)
                msg["role"] = self.control.role_payload(sess.id)
                if sess.id == old_id and new_id != old_id:
                    msg["lost_control"] = True
                asyncio.create_task(sess.conn.send(msg))

    # ---- snapshots ---------------------------------------------------------
    async def snapshot(self) -> dict:
        cells = await self.db.list_cells()
        executions = await self.db.list_executions()
        messages = await self.db.list_messages([e["id"] for e in executions])
        return {
            "type": "snapshot",
            "cells": cells,
            "executions": executions,
            "messages": messages,
            "gen": self.gen,
            "kernel_state": self.kernel_state,
            "retention": {
                "messages_per_execution": config.MESSAGES_PER_EXEC_LIMIT,
                "executions_per_cell": config.EXECS_PER_CELL_LIMIT,
            },
        }

    # ---- control requests --------------------------------------------------
    async def request_acquire(self, conn, session_id: str) -> None:
        role = await self.control.acquire(session_id)
        await conn.send({"type": "control_update", **self.control.role_payload(session_id)})
        if role == "leader":
            # Was already leader (e.g. reconnect during grace); nothing else.
            return
        self.broadcast(
            {"type": "control_changed", **self.control.snapshot(),
             "role": self.control.role_payload(session_id)}
        )

    async def request_release(self, conn, session_id: str) -> None:
        await self.control.release(session_id)
        await conn.send({"type": "control_update", **self.control.role_payload(session_id)})

    def _check_leader(self, conn, session_id: str, token: dict | None, req_id: str,
                      kind: str) -> bool:
        """The single authorization gate.  UI button state is irrelevant."""
        if self.control.authorized(token):
            return True
        # Record the rejected submission so the client can see *why* if it
        # Record the rejected submission so the client can ask why later.
        asyncio.create_task(
            self.db.insert_submission(
                req_id, session_id, kind, time.time(), False, "not_leader"
            )
        )
        asyncio.create_task(
            conn.send(
                {
                    "type": "request_rejected",
                    "req_id": req_id,
                    "kind": kind,
                    "reason": "not_leader",
                    "role": self.control.role_payload(session_id),
                }
            )
        )
        return False

    # ---- execute -----------------------------------------------------------
    async def execute(
        self, conn, session_id: str, token: dict | None, req_id: str,
        cell_id: str, source: str,
    ) -> None:
        if not self._check_leader(conn, session_id, token, req_id, "execute"):
            return
        if self.kernel_state in ("restarting", "dead", "starting"):
            await self.db.insert_submission(
                req_id, session_id, "execute", time.time(), False,
                f"kernel_{self.kernel_state}",
            )
            await conn.send({
                "type": "request_rejected", "req_id": req_id, "kind": "execute",
                "reason": f"kernel_{self.kernel_state}",
                "role": self.control.role_payload(session_id),
            })
            return

        # Idempotent submit: a retried req_id never runs the cell twice.
        existing = await self.db.get_submission(req_id)
        if existing:
            await conn.send({
                "type": "execute_ack",
                "req_id": req_id,
                "execution_id": existing["execution_id"],
                "duplicate": True,
            })
            return

        cells = {c["id"]: c for c in await self.db.list_cells()}
        if cell_id not in cells:
            await self.db.insert_submission(
                req_id, session_id, "execute", time.time(), False, "no_such_cell"
            )
            await conn.send({
                "type": "request_rejected", "req_id": req_id, "kind": "execute",
                "reason": "no_such_cell",
            })
            return

        exec_id = uuid.uuid4().hex
        now = time.time()
        source = source if source is not None else cells[cell_id]["source"]
        await self.db.create_execution(
            exec_id, cell_id, req_id, session_id, self.gen, now, source
        )
        await self.db.insert_submission(
            req_id, session_id, "execute", now, True, None, exec_id
        )
        # Keep the cell's editor source aligned with what was run.
        if source != cells[cell_id]["source"]:
            await self.db.update_cell(cell_id, source, now)
            self.broadcast({"type": "cell_updated", "cell_id": cell_id,
                            "source": source, "updated_at": now})

        self.runner.cmd("execute", (exec_id, source))
        self._active.add(exec_id)

        await conn.send({"type": "execute_ack", "req_id": req_id,
                         "execution_id": exec_id})
        self.broadcast({
            "type": "execution_created",
            "execution": await self.db.get_execution(exec_id),
        })

    async def interrupt(self, conn, session_id: str, token: dict | None, req_id: str) -> None:
        if not self._check_leader(conn, session_id, token, req_id, "interrupt"):
            return
        existing = await self.db.get_submission(req_id)
        if existing:
            await conn.send({"type": "request_ack", "req_id": req_id, "kind": "interrupt",
                             "duplicate": True})
            return
        await self.db.insert_submission(req_id, session_id, "interrupt", time.time(), True, None)
        self.runner.cmd("interrupt", None)
        await conn.send({"type": "request_ack", "req_id": req_id, "kind": "interrupt"})

    async def restart(self, conn, session_id: str, token: dict | None, req_id: str) -> None:
        if not self._check_leader(conn, session_id, token, req_id, "restart"):
            return
        existing = await self.db.get_submission(req_id)
        if existing:
            await conn.send({"type": "request_ack", "req_id": req_id, "kind": "restart",
                             "duplicate": True})
            return
        await self.db.insert_submission(req_id, session_id, "restart", time.time(), True, None)
        self.runner.cmd("restart", None)
        await conn.send({"type": "request_ack", "req_id": req_id, "kind": "restart"})

    async def query_submission(self, conn, req_id: str) -> None:
        sub = await self.db.get_submission(req_id)
        if not sub:
            await conn.send({"type": "submission_status", "req_id": req_id, "known": False})
            return
        payload: dict[str, Any] = {
            "type": "submission_status",
            "req_id": req_id,
            "known": True,
            "kind": sub["kind"],
            "accepted": bool(sub["accepted"]),
            "reject_reason": sub["reject_reason"],
            "execution_id": sub["execution_id"],
        }
        if sub["execution_id"]:
            payload["execution"] = await self.db.get_execution(sub["execution_id"])
        await conn.send(payload)

    # ---- cell editing ------------------------------------------------------
    async def cell_update(self, cell_id: str, source: str) -> None:
        now = time.time()
        if await self.db.update_cell(cell_id, source, now):
            self.broadcast({"type": "cell_updated", "cell_id": cell_id,
                            "source": source, "updated_at": now})

    async def cell_add(self, conn, cell_id: str | None, after_id: str | None) -> None:
        cid = cell_id or uuid.uuid4().hex
        cells = await self.db.list_cells()
        if after_id and any(c["id"] == after_id for c in cells):
            after = next(c for c in cells if c["id"] == after_id)
            ord_ = after["ord"] + 1
        else:
            ord_ = max((c["ord"] for c in cells), default=-1) + 1
        now = time.time()
        await self.db.create_cell(cid, now, ord_, "")
        cell = next(c for c in await self.db.list_cells() if c["id"] == cid)
        self.broadcast({"type": "cell_added", "cell": cell})

    async def cell_delete(self, cell_id: str) -> None:
        await self.db.delete_cell(cell_id)
        self.broadcast({"type": "cell_deleted", "cell_id": cell_id})

    # ---- kernel event pump -------------------------------------------------
    async def _pump(self) -> None:
        """Poll the runner thread's event queue and process events serially in
        asyncio, in the exact order the kernel produced them."""
        loop = asyncio.get_running_loop()
        while not self._stopping:
            events = await loop.run_in_executor(None, self.runner.drain_events)
            for ev in events:
                try:
                    await self._handle_kernel_event(ev)
                except Exception as e:  # never let the pump die
                    self.broadcast({"type": "server_error", "error": str(e)})
            await asyncio.sleep(0.02)

    async def _handle_kernel_event(self, ev: dict) -> None:
        t = ev["type"]

        if t == "kernel_status":
            # During a restart we ignore ordinary busy/idle churn.
            if self.kernel_state in ("restarting", "dead", "starting"):
                return
            self.kernel_state = "busy" if ev["state"] == "busy" else "idle"
            self.broadcast({"type": "kernel_status", "state": self.kernel_state})
            return

        if t in ("restart_begin", "kernel_died"):
            # New world: every running execution belonged to the old process.
            for exec_id in list(self._active):
                await self.db.finish_execution(exec_id, "abandoned", time.time())
                ex = await self.db.get_execution(exec_id)
                self.broadcast({"type": "execution_finished", "execution": ex})
            self._active.clear()
            self.gen += 1
            await self.db.set_meta("kernel_gen", self.gen)
            self.kernel_state = "restarting" if t == "restart_begin" else "dead"
            self.broadcast({"type": "gen_changed", "gen": self.gen,
                            "reason": "restart" if t == "restart_begin" else "died"})
            self.broadcast({"type": "kernel_status", "state": self.kernel_state})
            return

        if t in ("restart_end", "kernel_ready"):
            self.kernel_state = "idle"
            self.broadcast({"type": "kernel_status", "state": "idle"})
            return

        if t == "kernel_message":
            await self._persist_and_relay(ev)
            return

        if t == "execute_reply":
            await self._finish_execution(ev)
            return

        # restart_failed, kernel_start_failed, interrupt_failed, channel_error,
        # runner_error, runner_stopped
        self.broadcast({"type": "kernel_notice", "event": t,
                        "error": ev.get("error")})

    async def _persist_and_relay(self, ev: dict) -> None:
        exec_id = ev["execution_id"]
        kind = ev["kind"]
        if kind not in MSG_KINDS:
            return
        # An execution can only receive messages while it is genuinely active.
        execution = await self.db.get_execution(exec_id)
        if not execution or execution["status"] != RUNNING:
            return

        msg_id = await self.db.add_message(
            exec_id, self.gen, kind, ev.get("content", {})
        )
        dropped = 0
        if await self.db.count_messages(exec_id) > config.MESSAGES_PER_EXEC_LIMIT:
            dropped = await self.db.prune_execution_messages(
                exec_id, config.MESSAGES_PER_EXEC_LIMIT
            )
        relay = {
            "type": "execution_message",
            "id": msg_id,
            "execution_id": exec_id,
            "gen": self.gen,
            "kind": kind,
            "content": ev.get("content", {}),
        }
        if ev.get("display_id"):
            relay["display_id"] = ev["display_id"]
        self.broadcast(relay)
        if dropped:
            ex = await self.db.get_execution(exec_id)
            self.broadcast({"type": "execution_truncated",
                            "execution_id": exec_id,
                            "dropped_count": ex["dropped_count"],
                            "limit": config.MESSAGES_PER_EXEC_LIMIT})

    async def _finish_execution(self, ev: dict) -> None:
        exec_id = ev["execution_id"]
        execution = await self.db.get_execution(exec_id)
        if not execution or execution["status"] != RUNNING:
            self._active.discard(exec_id)
            return

        status = ev["status"]
        if status == "ok":
            final = "ok"
        elif status == "aborted":
            final = "aborted"
        elif ev.get("ename") == "KeyboardInterrupt":
            final = "interrupted"
        else:
            final = "error"

        now = time.time()
        await self.db.finish_execution(exec_id, final, now)
        # A long output burst may have pushed an earlier error traceback out
        # of the retention window; cap history per cell too.
        removed = await self.db.prune_cell_executions(
            execution["cell_id"], config.EXECS_PER_CELL_LIMIT
        )
        for rid in removed:
            self.broadcast({"type": "execution_evicted", "execution_id": rid})
        self._active.discard(exec_id)
        self.broadcast({
            "type": "execution_finished",
            "execution": await self.db.get_execution(exec_id),
        })
