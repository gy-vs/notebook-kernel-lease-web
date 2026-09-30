"""执行控制权：带 fencing token(epoch) 的租约锁。

语义：
- 同一时刻只有一个 client_id 可以持锁；所有变更操作后端都强制校验 epoch；
- 持锁连接断开给 DISCONNECT_GRACE 秒宽限（刷新页面用），宽限内同 client
  带着原 token 重连/心跳即恢复；过期后 epoch +1，旧 token 永久作废，
  网络恢复也无法凭旧状态重新干预内核；
- epoch 只增不减，epoch 被 bump 的窗口进入 lost 态，必须重新申请。
"""
from __future__ import annotations

import asyncio
from typing import Any

from . import config
from .db import DB, now


class LockError(Exception):
    def __init__(self, code: str, snapshot: dict) -> None:
        super().__init__(code)
        self.code = code
        self.snapshot = snapshot


class LockManager:
    def __init__(self, db: DB, bus: Any) -> None:
        self.db = db
        self.bus = bus

        self.epoch: int = 0
        self.state: str = "free"          # free | held | grace
        self.holder: str | None = None
        self.held_epoch: int = 0
        self.lease_expires: float = 0.0
        self.grace_until: float = 0.0
        self.waiters: list[str] = []      # client_id FIFO
        self._reaper: asyncio.Task | None = None

    async def start(self) -> None:
        st = await self.db.get_meta("lock_state")
        if st:
            self.epoch = st["epoch"]
        self._reaper = asyncio.create_task(self._reap_loop())

    async def stop(self) -> None:
        if self._reaper:
            self._reaper.cancel()

    def snapshot(self) -> dict:
        return {
            "state": self.state,
            "epoch": self.epoch,
            "holder": self.holder,
            "held_epoch": self.held_epoch,
            "lease_expires": self.lease_expires,
            "waiters": list(self.waiters),
        }

    async def _persist_and_emit(self, event: str = "lock_changed") -> None:
        snap = self.snapshot()
        await self.db.set_meta("lock_state", snap)
        # lock 事件也进 journal，保证重放与广播一致
        seq = await self.db.append_event(event, {"lock": snap})
        await self.bus.publish({"type": "event", "seq": seq, "kind": event,
                                "data": {"lock": snap}})

    def _valid_token(self, client_id: str, epoch: int) -> bool:
        return (
            self.state in ("held", "grace")
            and self.holder == client_id
            and self.held_epoch == epoch
        )

    async def require_async(self, client_id: str | None, epoch: int | None) -> None:
        """变更类接口的后端强制校验（异步版：恢复时广播）。"""
        if not client_id or epoch is None or not self._valid_token(client_id, epoch):
            raise LockError("LOCK_LOST", self.snapshot())
        if self.state == "grace":
            self.state = "held"
            self.lease_expires = max(self.lease_expires,
                                     now() + config.LEASE_SECONDS)
            self.grace_until = 0
            await self._persist_and_emit()

    def require(self, client_id: str | None, epoch: int | None) -> None:
        """同步校验（REST 路径请用 require_async）。"""
        if not client_id or epoch is None or not self._valid_token(client_id, epoch):
            raise LockError("LOCK_LOST", self.snapshot())

    async def acquire(self, client_id: str, wait: bool = True) -> dict:
        # 1) 自己本来就持有（含宽限期恢复）
        if self._valid_token(client_id, self.held_epoch):
            self.state = "held"
            self.lease_expires = max(self.lease_expires,
                                     now() + config.LEASE_SECONDS)
            self.waiters = [c for c in self.waiters if c != client_id]
            await self._persist_and_emit()
            return self.snapshot()

        # 2) 锁空闲
        if self.state == "free":
            self._grant(client_id)
            await self._persist_and_emit()
            return self.snapshot()

        # 3) 别人持有 / 宽限中：排队
        if client_id not in self.waiters:
            self.waiters.append(client_id)
            await self._persist_and_emit()
        if not wait:
            raise LockError("LOCK_BUSY", self.snapshot())
        deadline = now() + 25.0
        while now() < deadline:
            await asyncio.sleep(0.4)
            if self._valid_token(client_id, self.held_epoch):
                return self.snapshot()
            # 持锁者已变更且排队被清（比如自己 token 作废）则继续等
        raise LockError("LOCK_BUSY", self.snapshot())

    def _grant(self, client_id: str) -> None:
        self.epoch += 1
        self.state = "held"
        self.holder = client_id
        self.held_epoch = self.epoch
        self.lease_expires = now() + config.LEASE_SECONDS
        self.waiters = [c for c in self.waiters if c != client_id]

    async def heartbeat(self, client_id: str, epoch: int) -> dict:
        if not self._valid_token(client_id, epoch):
            raise LockError("LOCK_LOST", self.snapshot())
        if self.state == "grace":
            # 同一个持锁者在宽限内带着同一 epoch 重连续约：恢复持有
            self.state = "held"
            self.lease_expires = now() + config.LEASE_SECONDS
            self.grace_until = 0
            await self._persist_and_emit()
            return self.snapshot()
        self.state = "held"
        self.lease_expires = now() + config.LEASE_SECONDS
        return self.snapshot()

    async def release(self, client_id: str, epoch: int) -> dict:
        if not self._valid_token(client_id, epoch):
            raise LockError("LOCK_LOST", self.snapshot())
        await self._make_free("released")
        return self.snapshot()

    async def mark_disconnected(self, client_id: str) -> None:
        if self.state != "held" or self.holder != client_id:
            return
        # 进入宽限但不 bump epoch：同一 client 带着同一个 epoch 在宽限内
        # 重新连接（acquire/heartbeat）即可恢复；宽限一过立即 bump 作废。
        self.state = "grace"
        self.holder = client_id
        self.grace_until = now() + config.DISCONNECT_GRACE
        await self._persist_and_emit()

    async def _make_free(self, reason: str) -> None:
        self.state = "free"
        self.holder = None
        self.held_epoch += 1  # 让旧 token 立即作废
        self.lease_expires = 0
        self.grace_until = 0
        next_waiter = self.waiters.pop(0) if self.waiters else None
        await self._persist_and_emit()
        if next_waiter:
            # 队列首位直接获得锁（其 WebSocket 会收到事件并进入 held）
            self._grant(next_waiter)
            await self._persist_and_emit("lock_granted")

    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            t = now()
            if self.state == "grace" and t >= self.grace_until:
                await self._make_free("grace_expired")
            elif self.state == "held" and t >= self.lease_expires:
                await self._make_free("lease_expired")

    async def position(self, client_id: str) -> int | None:
        if self._valid_token(client_id, self.held_epoch):
            return 0
        if client_id in self.waiters:
            return self.waiters.index(client_id) + 1
        return None
