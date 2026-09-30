"""Execution control: exactly one session may drive the kernel.

The lock lives entirely on the server.  Button-disabled states in the UI are
only a hint; the Hub rejects execute/interrupt/restart from any other session.

States for a session:
    leader  - holds the token; its requests are accepted
    waiting - queued to take over when the current leader goes away
    viewer  - may observe everything but not execute

When the leader's websocket drops, its session enters a grace period (a
reloading page keeps control).  If it does not reconnect, the head of the
wait queue is promoted.  A reconnecting session only gets its leader status
back while its token is still valid; once deposed, the old session id can
never regain the same token — promotion mints a new epoch, so stale tabs
that come back online with an old token are rejected.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field

from . import config


@dataclass
class Session:
    id: str
    conn: object = None  # current live Connection, or None during grace
    lost_notified: bool = False


@dataclass
class Leadership:
    session_id: str
    epoch: int
    since: float
    grace_task: asyncio.Task | None = None


@dataclass
class ControlState:
    leader: Leadership | None = None
    sessions: dict[str, Session] = field(default_factory=dict)
    wait_queue: list[str] = field(default_factory=list)

    def role_of(self, session_id: str) -> str:
        if self.leader and self.leader.session_id == session_id:
            return "leader"
        if session_id in self.wait_queue:
            return "waiting"
        return "viewer"


class Control:
    """Pure state machine; the Hub supplies callbacks for promotions."""

    def __init__(self, on_change) -> None:
        self.state = ControlState()
        self._on_change = on_change  # async callback(session_id or None, event)

    def token_of(self, session_id: str) -> dict | None:
        s = self.state
        if s.leader and s.leader.session_id == session_id:
            return {"session_id": session_id, "epoch": s.leader.epoch}
        return None

    def register(self, session_id: str | None, conn) -> str:
        """Bind a websocket to a session. Returns the session id."""
        s = self.state
        if session_id is None:
            session_id = uuid.uuid4().hex
        sess = s.sessions.get(session_id)
        if sess is None:
            sess = Session(id=session_id)
            s.sessions[session_id] = sess
        sess.conn = conn
        # If this session is leader inside its grace window, reconnect wins.
        if s.leader and s.leader.session_id == session_id and s.leader.grace_task:
            s.leader.grace_task.cancel()
            s.leader.grace_task = None
            sess.lost_notified = False
        return session_id

    def unregister(self, conn) -> list[str]:
        """A websocket went away. Returns [session_id] if a grace timer for a
        deposed leader must be scheduled."""
        s = self.state
        for sess in s.sessions.values():
            if sess.conn is conn:
                sess.conn = None
                if s.leader and s.leader.session_id == sess.id and not s.leader.grace_task:
                    return [sess.id]
        return []

    async def acquire(self, session_id: str) -> str:
        s = self.state
        role = s.role_of(session_id)
        if role == "leader":
            return "leader"
        if role == "viewer":
            s.wait_queue.append(session_id)
            return "waiting"
        return role  # already waiting

    async def release(self, session_id: str) -> None:
        s = self.state
        if session_id in s.wait_queue:
            s.wait_queue.remove(session_id)
        if s.leader and s.leader.session_id == session_id:
            self._depose_and_promote(reason="leader_released")

    async def leader_disconnected(self, session_id: str, loop) -> asyncio.Task:
        async def grace():
            await asyncio.sleep(config.LEADER_GRACE_SECONDS)
            sess = self.state.sessions.get(session_id)
            # Reconnected during grace?
            if sess and sess.conn is not None:
                return
            self._depose_and_promote(reason="leader_timeout")

        task = loop.create_task(grace())
        self.state.leader.grace_task = task
        return task

    def _depose_and_promote(self, reason: str) -> None:
        s = self.state
        old_id = s.leader.session_id if s.leader else None
        if s.leader and s.leader.grace_task:
            s.leader.grace_task.cancel()
            s.leader.grace_task = None

        new_id: str | None = None
        while s.wait_queue:
            cand = s.wait_queue.pop(0)
            sess = s.sessions.get(cand)
            if sess is not None:
                new_id = cand
                break

        new_epoch = (s.leader.epoch + 1) if s.leader else 1
        s.leader = (
            Leadership(session_id=new_id, epoch=new_epoch, since=time.time())
            if new_id
            else None
        )
        # Old leader session learns it has been deposed the next time it
        # talks to us; its old epoch no longer validates.
        if old_id:
            old = s.sessions.get(old_id)
            if old:
                old.lost_notified = False
        self._on_change(old_id, new_id, reason)

    def authorized(self, token: dict | None) -> bool:
        s = self.state
        if not token or not s.leader:
            return False
        return (
            token.get("session_id") == s.leader.session_id
            and token.get("epoch") == s.leader.epoch
        )

    def snapshot(self) -> dict:
        s = self.state
        return {
            "leader": s.leader.session_id if s.leader else None,
            "epoch": s.leader.epoch if s.leader else 0,
            "wait_queue": list(s.wait_queue),
        }

    def role_payload(self, session_id: str) -> dict:
        return {
            "role": self.state.role_of(session_id),
            "leader": self.state.leader.session_id if self.state.leader else None,
            "epoch": self.state.leader.epoch if self.state.leader else 0,
            "wait_queue": list(self.state.wait_queue),
            "grace_seconds": config.LEADER_GRACE_SECONDS,
        }
