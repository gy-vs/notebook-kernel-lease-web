"""SQLite 持久化层。

所有结构化变更都写进 journal（带全局单调递增 seq）：
- 前端重连时按 seq 回读，保证“保存”与“连接”分开；
- 内核消息（stream/display/...）作为 kernel_message 事件持久化，
  超出保留范围时只淘汰最旧的内核消息，并由执行记录里的计数推导缺口。
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any

import aiosqlite

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cells (
    id TEXT PRIMARY KEY,
    order_idx INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS kernel_gens (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    status TEXT NOT NULL,            -- starting | alive | ended
    reason TEXT
);
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    cell_id TEXT NOT NULL,
    gen_id TEXT NOT NULL,
    num INTEGER NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,            -- queued | running | ok | error | interrupted
    execute_count INTEGER,
    queued_at REAL NOT NULL,
    started_at REAL,
    ended_at REAL,
    end_reason TEXT,                 -- reply | error | interrupted | orphaned
    ename TEXT,
    evalue TEXT,
    messages_total INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS client_requests (
    request_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_journal_kind ON journal(kind);
CREATE INDEX IF NOT EXISTS idx_exec_cell ON executions(cell_id);
CREATE INDEX IF NOT EXISTS idx_exec_gen ON executions(gen_id);
"""

_message_kinds = {"kernel_message"}


def now() -> float:
    return time.time()


def new_id() -> str:
    return uuid.uuid4().hex


class DB:
    def __init__(self) -> None:
        self.conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(config.DB_PATH)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()

    # ---------- 基础 ----------
    async def _one(self, sql: str, args: tuple = ()) -> Any:
        async with self.conn.execute(sql, args) as cur:
            return await cur.fetchone()

    async def get_meta(self, key: str, default: Any = None) -> Any:
        row = await self._one("SELECT value FROM meta WHERE key=?", (key,))
        return default if row is None else json.loads(row["value"])

    async def set_meta(self, key: str, value: Any) -> None:
        await self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        await self.conn.commit()

    # ---------- journal ----------
    async def append_event(self, kind: str, data: dict) -> int:
        cur = await self.conn.execute(
            "INSERT INTO journal(kind,data,created_at) VALUES(?,?,?)",
            (kind, json.dumps(data, ensure_ascii=False), now()),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def events_after(self, seq: int, limit: int = 100000) -> list[dict]:
        async with self.conn.execute(
            "SELECT seq,kind,data FROM journal WHERE seq>? ORDER BY seq LIMIT ?",
            (seq, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [
            {"seq": r["seq"], "kind": r["kind"], "data": json.loads(r["data"])}
            for r in rows
        ]

    async def gc_journal(self) -> int:
        """淘汰最旧的内核消息，返回删除条数。结构事件始终保留。"""
        row = await self._one(
            "SELECT COUNT(*) AS n FROM journal WHERE kind='kernel_message'"
        )
        count = row["n"]
        if count <= config.MAX_KERNEL_MESSAGES:
            return 0
        excess = count - config.MAX_KERNEL_MESSAGES
        await self.conn.execute(
            "DELETE FROM journal WHERE seq IN ("
            "  SELECT seq FROM journal WHERE kind='kernel_message'"
            "  ORDER BY seq LIMIT ?)",
            (excess,),
        )
        await self.conn.commit()
        return excess

    async def journal_low_watermark(self) -> int:
        """当前仍保留的最小内核消息 seq（全部被淘汰时为 0）。"""
        row = await self._one(
            "SELECT MIN(seq) AS s FROM journal WHERE kind='kernel_message'"
        )
        return (row["s"] or 0)

    # ---------- cells ----------
    async def list_cells(self) -> list[dict]:
        async with self.conn.execute(
            "SELECT * FROM cells ORDER BY order_idx, rowid"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_cell(self, cell_id: str) -> dict | None:
        row = await self._one("SELECT * FROM cells WHERE id=?", (cell_id,))
        return dict(row) if row else None

    async def create_cell(self, source: str = "", after_id: str | None = None) -> dict:
        cells = await self.list_cells()
        if after_id is None:
            order_idx = (max((c["order_idx"] for c in cells), default=0) + 1)
        else:
            ids = [c["id"] for c in cells]
            i = ids.index(after_id)
            order_idx = cells[i]["order_idx"] + 1
            # 让后续单元的 order_idx 后移
            await self.conn.execute(
                "UPDATE cells SET order_idx=order_idx+1 WHERE order_idx>=?",
                (order_idx,),
            )
        cell = {
            "id": new_id(),
            "order_idx": order_idx,
            "source": source,
            "created_at": now(),
            "updated_at": now(),
        }
        await self.conn.execute(
            "INSERT INTO cells(id,order_idx,source,created_at,updated_at)"
            " VALUES(?,?,?,?,?)",
            (cell["id"], cell["order_idx"], cell["source"],
             cell["created_at"], cell["updated_at"]),
        )
        await self.conn.commit()
        await self.append_event("cell_created", {"cell": cell})
        return cell

    async def update_cell_source(self, cell_id: str, source: str) -> None:
        await self.conn.execute(
            "UPDATE cells SET source=?, updated_at=? WHERE id=?",
            (source, now(), cell_id),
        )
        await self.conn.commit()
        await self.append_event(
            "cell_updated", {"id": cell_id, "source": source}
        )

    async def delete_cell(self, cell_id: str) -> None:
        await self.conn.execute("DELETE FROM cells WHERE id=?", (cell_id,))
        await self.conn.commit()
        await self.append_event("cell_deleted", {"id": cell_id})

    # ---------- kernel generations ----------
    async def list_gens(self) -> list[dict]:
        async with self.conn.execute(
            "SELECT * FROM kernel_gens ORDER BY seq"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def create_gen(self) -> dict:
        nxt = (await self.get_meta("gen_seq", 0)) + 1
        await self.set_meta("gen_seq", nxt)
        gen = {
            "id": new_id(),
            "seq": nxt,
            "started_at": now(),
            "ended_at": None,
            "status": "starting",
            "reason": None,
        }
        await self.conn.execute(
            "INSERT INTO kernel_gens(id,seq,started_at,status,reason)"
            " VALUES(?,?,?,?,?)",
            (gen["id"], gen["seq"], gen["started_at"], gen["status"], gen["reason"]),
        )
        await self.conn.commit()
        await self.append_event("gen_started", {"gen": gen})
        return gen

    async def update_gen(self, gen_id: str, **fields: Any) -> None:
        if not fields:
            return
        keys = ", ".join(f"{k}=?" for k in fields)
        await self.conn.execute(
            f"UPDATE kernel_gens SET {keys} WHERE id=?",
            (*fields.values(), gen_id),
        )
        await self.conn.commit()
        gen = await self._one("SELECT * FROM kernel_gens WHERE id=?", (gen_id,))
        await self.append_event(
            "gen_updated", {"gen": dict(gen) if gen else {"id": gen_id, **fields}}
        )

    async def mark_gen_ended(
        self, gen_id: str, status: str = "ended", reason: str | None = None
    ) -> None:
        await self.update_gen(
            gen_id, status=status, ended_at=now(), reason=reason
        )

    async def mark_orphans(self) -> None:
        """服务重启时调用。

        - 上一个进程留下的未结束代全部标记为 ended；
        - 任何属于已结束代、却仍处于 queued/running 的执行标记为
          interrupted(orphaned)，页面不会把它们显示成“仍在运行”。
        """
        async with self.conn.execute(
            "SELECT id FROM kernel_gens WHERE status IN ('starting','alive')"
        ) as cur:
            live = [r["id"] for r in await cur.fetchall()]
        for gen_id in live:
            await self.mark_gen_ended(gen_id, reason="server_restart")

        async with self.conn.execute(
            "SELECT e.id FROM executions e JOIN kernel_gens g ON g.id=e.gen_id"
            " WHERE e.status IN ('queued','running') AND g.status='ended'"
        ) as cur:
            stuck = [r["id"] for r in await cur.fetchall()]
        for ex_id in stuck:
            await self.update_execution(
                ex_id,
                status="interrupted",
                end_reason="orphaned",
                ended_at=now(),
            )

    # ---------- executions ----------
    async def create_execution(self, cell_id: str, gen_id: str, source: str) -> dict:
        nxt = (await self.get_meta("exec_seq", 0)) + 1
        await self.set_meta("exec_seq", nxt)
        ex = {
            "id": new_id(),
            "cell_id": cell_id,
            "gen_id": gen_id,
            "num": nxt,
            "source": source,
            "status": "queued",
            "execute_count": None,
            "queued_at": now(),
            "started_at": None,
            "ended_at": None,
            "end_reason": None,
            "ename": None,
            "evalue": None,
            "messages_total": 0,
        }
        await self.conn.execute(
            "INSERT INTO executions(id,cell_id,gen_id,num,source,status,"
            "execute_count,queued_at,started_at,ended_at,end_reason,ename,evalue,"
            "messages_total) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ex["id"], ex["cell_id"], ex["gen_id"], ex["num"], ex["source"],
             ex["status"], ex["execute_count"], ex["queued_at"], ex["started_at"],
             ex["ended_at"], ex["end_reason"], ex["ename"], ex["evalue"],
             ex["messages_total"]),
        )
        await self.conn.commit()
        await self.append_event("execution_created", {"execution": ex})
        return ex

    async def get_execution(self, ex_id: str) -> dict | None:
        row = await self._one("SELECT * FROM executions WHERE id=?", (ex_id,))
        return dict(row) if row else None

    async def get_execution_by_request(self, request_id: str) -> dict | None:
        row = await self._one(
            "SELECT e.* FROM client_requests cr"
            " JOIN executions e ON e.id=cr.execution_id"
            " WHERE cr.request_id=?",
            (request_id,),
        )
        return dict(row) if row else None

    async def remember_request(self, request_id: str, execution_id: str) -> None:
        await self.conn.execute(
            "INSERT OR IGNORE INTO client_requests(request_id,execution_id,created_at)"
            " VALUES(?,?,?)",
            (request_id, execution_id, now()),
        )
        await self.conn.commit()

    async def update_execution(self, ex_id: str, **fields: Any) -> None:
        if not fields:
            return
        no_event = fields.pop("_no_event", False)
        # 追加消息计数（dispatch 用）
        inc = fields.pop("_inc_messages", 0)
        sets = [f"{k}=?" for k in fields]
        args: list[Any] = list(fields.values())
        if inc:
            sets.append("messages_total=messages_total+?")
            args.append(inc)
        args.append(ex_id)
        await self.conn.execute(
            f"UPDATE executions SET {', '.join(sets)} WHERE id=?", args
        )
        await self.conn.commit()
        row = await self._one("SELECT * FROM executions WHERE id=?", (ex_id,))
        if row and not no_event:
            await self.append_event(
                "execution_updated", {"execution": dict(row)}
            )

    async def list_executions(self) -> list[dict]:
        async with self.conn.execute(
            "SELECT * FROM executions ORDER BY num"
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def retained_message_counts(self) -> dict[str, int]:
        async with self.conn.execute(
            "SELECT data FROM journal WHERE kind='kernel_message'"
        ) as cur:
            rows = await cur.fetchall()
        counts: dict[str, int] = {}
        for r in rows:
            eid = json.loads(r["data"]).get("execution_id")
            counts[eid] = counts.get(eid, 0) + 1
        return counts

    async def max_event_seq(self) -> int:
        row = await self._one("SELECT COALESCE(MAX(seq),0) AS s FROM journal")
        return row["s"]

    async def all_kernel_messages(self) -> list[dict]:
        async with self.conn.execute(
            "SELECT seq,data FROM journal WHERE kind='kernel_message'"
            " ORDER BY seq"
        ) as cur:
            rows = await cur.fetchall()
        out = []
        for r in rows:
            d = json.loads(r["data"])
            out.append({"seq": r["seq"], **d})
        return out

    async def insert_initial_cell_if_empty(self) -> None:
        row = await self._one("SELECT COUNT(*) AS n FROM cells")
        if row["n"] == 0:
            await self.create_cell(
                "# 在两个标签页打开本页面试试：\n"
                "# 只有持有执行控制权的窗口可以运行代码\n"
                "x = 21\n"
                "print('你好，ipykernel')\n"
                "x * 2"
            )
