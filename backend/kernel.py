"""内核服务：用 jupyter_client 管理 ipykernel 的多个“代（generation）”。

关键点：
- 内核变量真实存活在同一个 ipykernel 进程里，执行之间共享状态；
- 每次重启是一个新的 gen_id，旧代的执行与结果留在数据库但与新内核隔离；
- 所有输出按“父消息 id → execution_id”归属，不按连接/窗口归属；
- iopub 消息先持久化、再广播，断线重连通过 seq 回读补齐。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from jupyter_client import AsyncKernelManager

from .db import DB, now

log = logging.getLogger("kernel")

PERSISTED_MSG_TYPES = {
    "stream",
    "display_data",
    "update_display_data",
    "execute_result",
    "error",
    "clear_output",
}


class _Pending:
    def __init__(self, execution_id: str, gen_id: str, code: str) -> None:
        self.execution_id = execution_id
        self.gen_id = gen_id
        self.code = code
        self.msg_id: str | None = None
        self.idle = asyncio.Event()
        self.got_error = False
        self.interrupt_requested = False
        self.reply_status: str | None = None
        self.execution_count: int | None = None
        self.finished = False


class KernelService:
    def __init__(self, db: DB, bus: Any) -> None:
        self.db = db
        self.bus = bus

        self.gen_id: str | None = None
        self.km: AsyncKernelManager | None = None
        self.kc = None
        self._tasks: list[asyncio.Task] = []
        self._stopping_gen: str | None = None

        self._queue: asyncio.Queue[_Pending] = asyncio.Queue()
        self._by_msg: dict[str, _Pending] = {}
        self._current: _Pending | None = None
        self._lock = asyncio.Lock()

    # ---------------- 生命周期 ----------------
    async def start_fresh(self) -> str:
        """确保有一个活着的内核代；并发调用只会启动一次。"""
        async with self._lock:
            if self.gen_id is not None and self.km is not None:
                try:
                    if await self.km.is_alive():
                        return self.gen_id
                except Exception:  # noqa: BLE001
                    pass
            return await self._start_new_gen_locked()

    async def _start_new_gen_locked(self) -> str:
        gen = await self.db.create_gen()
        self.gen_id = gen["id"]
        km = AsyncKernelManager(kernel_name="python3")
        try:
            await km.start_kernel()
        except Exception:
            log.exception("启动内核失败")
            await self.db.mark_gen_ended(gen["id"], status="ended", reason="start_failed")
            raise
        kc = km.client()
        kc.start_channels()
        try:
            await kc.wait_for_ready(timeout=30)
        except Exception:
            log.exception("内核通道未就绪")
            await km.shutdown_kernel()
            await self.db.mark_gen_ended(gen["id"], status="ended", reason="start_failed")
            raise

        self.km = km
        self.kc = kc
        self._stopping_gen = None
        await self.db.update_gen(gen["id"], status="alive")
        await self.broadcast_status()

        self._tasks = [
            asyncio.create_task(self._iopub_loop(gen["id"])),
            asyncio.create_task(self._shell_loop(gen["id"])),
            asyncio.create_task(self._worker(gen["id"])),
            asyncio.create_task(self._watchdog(gen["id"])),
        ]
        return gen["id"]

    async def broadcast_status(self) -> None:
        await self.bus.publish({
            "type": "kernel_status",
            "current_gen_id": self.gen_id,
            "kernel_alive": self.km is not None,
        })

    async def stop_current(self, reason: str) -> None:
        """停止当前代（不新建）。"""
        async with self._lock:
            await self._stop_current_locked(reason)

    async def _stop_current_locked(self, reason: str) -> None:
        gen_id = self.gen_id
        if gen_id is None:
            return
        self._stopping_gen = gen_id

        # 排队中的执行全部作废
        drained: list[_Pending] = []
        while not self._queue.empty():
            drained.append(self._queue.get_nowait())
        for p in drained:
            await self._finish(p, status="interrupted", end_reason=reason)

        # 正在运行的执行标记为 interrupted
        cur = self._current
        if cur and gen_id == cur.gen_id:
            await self._finish(cur, status="interrupted", end_reason=reason)

        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks = []
        self._by_msg.clear()
        self._current = None

        if self.kc is not None:
            try:
                self.kc.stop_channels()
            except Exception:  # noqa: BLE001
                pass
            self.kc = None
        if self.km is not None:
            try:
                await self.km.shutdown_kernel(now=True)
            except Exception:  # noqa: BLE001
                log.exception("关闭内核异常")
            self.km = None

        await self.db.mark_gen_ended(gen_id, reason=reason)
        if self.gen_id == gen_id:
            self.gen_id = None
        await self.broadcast_status()

    async def restart(self) -> str:
        async with self._lock:
            await self._stop_current_locked("restarted")
            return await self._start_new_gen_locked()

    async def shutdown_on_exit(self) -> None:
        await self.stop_current("server_stop")

    # ---------------- 执行提交 ----------------
    async def submit(self, execution_id: str, code: str) -> None:
        if self.gen_id is None:
            # 内核尚未启动：惰性启动
            await self.start_fresh()
        p = _Pending(execution_id, self.gen_id, code)
        await self._queue.put(p)

    async def interrupt_current(self) -> None:
        cur = self._current
        if cur and self.kc is not None:
            cur.interrupt_requested = True
            try:
                await self.km.interrupt_kernel()
            except Exception:  # noqa: BLE001
                log.exception("中断内核失败")

    # ---------------- 内部循环 ----------------
    async def _worker(self, gen_id: str) -> None:
        while True:
            p = await self._queue.get()
            if p.gen_id != gen_id or self._stopping_gen == gen_id:
                continue
            self._current = p
            try:
                p.msg_id = self.kc.execute(
                    p.code, allow_stdin=False, silent=False, store_history=True
                )
                self._by_msg[p.msg_id] = p
                await self.db.update_execution(
                    p.execution_id, status="running", started_at=now()
                )
                await p.idle.wait()
            except Exception:  # noqa: BLE001
                log.exception("提交执行失败")
                await self._finish(p, status="interrupted", end_reason="submit_failed")
            finally:
                if p.msg_id:
                    self._by_msg.pop(p.msg_id, None)
                self._current = None

    async def _iopub_loop(self, gen_id: str) -> None:
        while True:
            try:
                msg = await self.kc.get_iopub_msg()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                if self._stopping_gen == gen_id:
                    return
                log.exception("读取 iopub 失败")
                await asyncio.sleep(0.2)
                continue
            if self._stopping_gen == gen_id:
                return
            header = msg.get("header", {})
            msg_type = header.get("msg_type")
            parent_id = (msg.get("parent_header") or {}).get("msg_id")
            p = self._by_msg.get(parent_id) if parent_id else None
            content = msg.get("content", {}) or {}

            if msg_type == "status":
                state = content.get("execution_state")
                if p is not None and state == "idle":
                    p.idle.set()
                    if p.interrupt_requested:
                        await self._finish(p, "interrupted", "interrupted")
                    elif p.got_error or p.reply_status == "error":
                        await self._finish(p, "error", "reply")
                    else:
                        await self._finish(p, "ok", "reply",
                                           execute_count=p.execution_count)
                # 全局内核状态也记一笔，便于页面展示 starting/alive
                continue

            if p is None or msg_type not in PERSISTED_MSG_TYPES:
                continue

            data = self._extract_content(msg_type, content)
            seq = await self.db.append_event("kernel_message", {
                "execution_id": p.execution_id,
                "gen_id": gen_id,
                "msg_type": msg_type,
                "content": data,
            })
            await self.db.update_execution(p.execution_id, _inc_messages=1)
            if msg_type == "error":
                p.got_error = True
                await self.db.update_execution(
                    p.execution_id,
                    ename=data.get("ename"),
                    evalue=data.get("evalue"),
                    _no_event=True,
                )
            await self.db.gc_journal()
            await self.bus.publish({
                "type": "event",
                "seq": seq,
                "kind": "kernel_message",
                "data": {
                    "execution_id": p.execution_id,
                    "gen_id": gen_id,
                    "msg_type": msg_type,
                    "content": data,
                },
            })

    async def _shell_loop(self, gen_id: str) -> None:
        while True:
            try:
                msg = await self.kc.get_shell_msg()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                if self._stopping_gen == gen_id:
                    return
                await asyncio.sleep(0.2)
                continue
            if self._stopping_gen == gen_id:
                return
            parent_id = (msg.get("parent_header") or {}).get("msg_id")
            p = self._by_msg.get(parent_id) if parent_id else None
            if p is None:
                continue
            content = msg.get("content", {}) or {}
            if msg["header"]["msg_type"] == "execute_reply":
                p.reply_status = content.get("status")
                cnt = content.get("execution_count")
                if cnt is not None:
                    p.execution_count = cnt

    async def _watchdog(self, gen_id: str) -> None:
        """内核进程意外死亡时，把运行中的执行标记为 interrupted，而不是永远 running。"""
        while True:
            await asyncio.sleep(1.0)
            if self._stopping_gen == gen_id or self.gen_id != gen_id:
                return
            km = self.km
            if km is None:
                continue
            try:
                alive = await km.is_alive()
            except Exception:  # noqa: BLE001
                alive = True
            if not alive:
                log.warning("内核进程 %s 异常退出", gen_id)
                cur = self._current
                if cur and cur.gen_id == gen_id:
                    await self._finish(cur, "interrupted", "kernel_crashed")
                # 清空排队
                while not self._queue.empty():
                    p = self._queue.get_nowait()
                    if p.gen_id == gen_id:
                        await self._finish(p, "interrupted", "kernel_crashed")
                await self.db.mark_gen_ended(gen_id, reason="crashed")
                self.gen_id = None
                self.km = None
                await self.broadcast_status()
                return

    @staticmethod
    def _extract_content(msg_type: str, content: dict) -> dict:
        """只保留前端需要的字段，避免存储整包协议消息。"""
        if msg_type == "stream":
            return {"name": content.get("name", "stdout"), "text": content.get("text", "")}
        if msg_type in ("display_data", "update_display_data", "execute_result"):
            return {
                "data": content.get("data", {}),
                "metadata": content.get("metadata", {}),
                "transient": content.get("transient", {}),
            }
        if msg_type == "error":
            return {
                "ename": content.get("ename"),
                "evalue": content.get("evalue"),
                "traceback": content.get("traceback", []),
            }
        if msg_type == "clear_output":
            return {"wait": bool(content.get("wait", False))}
        return {}

    async def _finish(
        self,
        p: _Pending,
        status: str,
        end_reason: str,
        execute_count: int | None = None,
    ) -> None:
        ex = await self.db.get_execution(p.execution_id)
        if p.finished or (ex is not None and ex["status"] in
                          ("ok", "error", "interrupted")):
            p.idle.set()
            p.finished = True
            return
        p.finished = True
        fields: dict[str, Any] = {
            "status": status,
            "end_reason": end_reason,
            "ended_at": now(),
        }
        if execute_count is not None:
            fields["execute_count"] = execute_count
        if p.execution_count is not None:
            fields["execute_count"] = p.execution_count
        p.idle.set()
        await self.db.update_execution(p.execution_id, **fields)

    async def current_gen_id(self) -> str | None:
        return self.gen_id

    async def is_alive(self) -> bool:
        if self.km is None:
            return False
        try:
            return bool(await self.km.is_alive())
        except Exception:  # noqa: BLE001
            return False
