"""进程内发布订阅：所有 WebSocket 连接收到同一份有序事件。"""
from __future__ import annotations

import asyncio
from typing import Any


class EventBus:
    def __init__(self) -> None:
        self._subs: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    async def publish(self, message: Any) -> None:
        dead = []
        for q in self._subs:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                # 慢客户端：不阻塞内核处理，客户端靠 seq 缺口回退到快照
                dead.append(q)
        for q in dead:
            self._subs.discard(q)
