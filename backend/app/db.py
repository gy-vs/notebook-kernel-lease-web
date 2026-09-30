"""SQLite persistence.

What lives here (deliberately separate from the websocket connection):
- notebook cells (source code)
- executions: one row per accepted execute submission
- messages: retained kernel messages attributed to an execution
- submissions: client request ids for idempotency / status queries
- meta: kernel generation counter

A cell, an execution and a websocket connection are different things and
never share a row.
"""
from __future__ import annotations

import json
from typing import Any

import aiosqlite

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cells (
    id         TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    ord        INTEGER NOT NULL,
    source     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS executions (
    id            TEXT PRIMARY KEY,
    cell_id       TEXT NOT NULL REFERENCES cells(id) ON DELETE CASCADE,
    req_id        TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    gen           INTEGER NOT NULL,
    started_at    REAL NOT NULL,
    finished_at   REAL,
    status        TEXT NOT NULL,            -- running|ok|error|aborted|interrupted|abandoned
    dropped_count INTEGER NOT NULL DEFAULT 0,
    source        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_exec_cell ON executions(cell_id, started_at);
CREATE INDEX IF NOT EXISTS idx_exec_status ON executions(status);

CREATE TABLE IF NOT EXISTS messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT, -- global ordering
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    gen          INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    content      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_exec ON messages(execution_id, id);

CREATE TABLE IF NOT EXISTS submissions (
    req_id       TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL,
    kind         TEXT NOT NULL,               -- execute|interrupt|restart
    created_at   REAL NOT NULL,
    accepted     INTEGER NOT NULL DEFAULT 0,
    reject_reason TEXT,
    execution_id TEXT
);
"""


class Database:
    def __init__(self, path) -> None:
        self.path = str(path)

    async def connect(self) -> None:
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA foreign_keys=ON")
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.executescript(SCHEMA)
        await self.db.commit()

    async def close(self) -> None:
        await self.db.close()

    # ---- meta -------------------------------------------------------------
    async def get_meta(self, key: str, default: Any = None) -> Any:
        async with self.db.execute("SELECT value FROM meta WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
        return json.loads(row["value"]) if row else default

    async def set_meta(self, key: str, value: Any) -> None:
        await self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        await self.db.commit()

    # ---- cells ------------------------------------------------------------
    async def list_cells(self) -> list[dict]:
        async with self.db.execute("SELECT * FROM cells ORDER BY ord, created_at") as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def create_cell(self, cell_id: str, now: float, ord_: int, source: str = "") -> None:
        await self.db.execute(
            "INSERT INTO cells(id,created_at,updated_at,ord,source) VALUES(?,?,?,?,?)",
            (cell_id, now, now, ord_, source),
        )
        await self.db.commit()

    async def update_cell(self, cell_id: str, source: str, now: float) -> bool:
        cur = await self.db.execute(
            "UPDATE cells SET source=?, updated_at=? WHERE id=?",
            (source, now, cell_id),
        )
        await self.db.commit()
        return cur.rowcount > 0

    async def delete_cell(self, cell_id: str) -> None:
        await self.db.execute("DELETE FROM cells WHERE id=?", (cell_id,))
        await self.db.commit()

    # ---- submissions ------------------------------------------------------
    async def get_submission(self, req_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM submissions WHERE req_id=?", (req_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def insert_submission(
        self,
        req_id: str,
        session_id: str,
        kind: str,
        now: float,
        accepted: bool,
        reason: str | None,
        execution_id: str | None = None,
    ) -> None:
        await self.db.execute(
            "INSERT INTO submissions(req_id,session_id,kind,created_at,accepted,reject_reason,execution_id)"
            " VALUES(?,?,?,?,?,?,?)",
            (req_id, session_id, kind, now, int(accepted), reason, execution_id),
        )
        await self.db.commit()

    # ---- executions -------------------------------------------------------
    async def create_execution(
        self,
        exec_id: str,
        cell_id: str,
        req_id: str,
        session_id: str,
        gen: int,
        now: float,
        source: str,
    ) -> None:
        await self.db.execute(
            "INSERT INTO executions(id,cell_id,req_id,session_id,gen,started_at,status,source)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (exec_id, cell_id, req_id, session_id, gen, now, "running", source),
        )
        await self.db.commit()

    async def get_execution(self, exec_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM executions WHERE id=?", (exec_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def list_executions(self) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM executions ORDER BY started_at"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def finish_execution(self, exec_id: str, status: str, now: float) -> None:
        await self.db.execute(
            "UPDATE executions SET status=?, finished_at=? WHERE id=?",
            (status, now, exec_id),
        )
        await self.db.commit()

    async def mark_running_abandoned(self, now: float) -> list[str]:
        """Executions left running with no kernel to finish them (restart or
        service boot) are resolved as 'abandoned'."""
        async with self.db.execute(
            "UPDATE executions SET status='abandoned', finished_at=? "
            "WHERE status='running' RETURNING id",
            (now,),
        ) as cur:
            rows = await cur.fetchall()
        await self.db.commit()
        return [r["id"] for r in rows]

    # ---- messages ---------------------------------------------------------
    async def add_message(
        self, execution_id: str, gen: int, kind: str, content: dict
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO messages(execution_id,gen,kind,content) VALUES(?,?,?,?)",
            (execution_id, gen, kind, json.dumps(content, ensure_ascii=False)),
        )
        await self.db.commit()
        return cur.lastrowid

    async def list_messages(self, execution_ids: list[str]) -> list[dict]:
        if not execution_ids:
            return []
        qmarks = ",".join("?" * len(execution_ids))
        async with self.db.execute(
            f"SELECT * FROM messages WHERE execution_id IN ({qmarks}) ORDER BY id",
            execution_ids,
        ) as cur:
            rows = await cur.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["content"] = json.loads(d["content"])
            out.append(d)
        return out

    async def count_messages(self, execution_id: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) c FROM messages WHERE execution_id=?", (execution_id,)
        ) as cur:
            return (await cur.fetchone())["c"]

    async def prune_execution_messages(self, execution_id: str, keep: int) -> int:
        """Keep only the newest *keep* messages. Returns dropped count."""
        async with self.db.execute(
            "DELETE FROM messages WHERE execution_id=? AND id NOT IN ("
            "  SELECT id FROM messages WHERE execution_id=? ORDER BY id DESC LIMIT ?"
            ")",
            (execution_id, execution_id, keep),
        ) as cur:
            dropped = cur.rowcount
        if dropped:
            await self.db.execute(
                "UPDATE executions SET dropped_count=dropped_count+? WHERE id=?",
                (dropped, execution_id),
            )
            await self.db.commit()
        return dropped

    async def prune_cell_executions(self, cell_id: str, keep: int) -> list[str]:
        """Delete oldest executions for a cell beyond the keep window, plus
        all their messages. Returns removed execution ids."""
        async with self.db.execute(
            "SELECT id FROM executions WHERE cell_id=? ORDER BY started_at DESC "
            "LIMIT -1 OFFSET ?",
            (cell_id, keep),
        ) as cur:
            old = [r["id"] for r in await cur.fetchall()]
        if old:
            qmarks = ",".join("?" * len(old))
            await self.db.execute(
                f"DELETE FROM messages WHERE execution_id IN ({qmarks})", old
            )
            await self.db.execute(
                f"DELETE FROM executions WHERE id IN ({qmarks})", old
            )
            await self.db.commit()
        return old

    async def running_executions(self) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM executions WHERE status='running' ORDER BY started_at"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]
