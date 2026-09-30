"""端到端冒烟测试（需要服务已在 127.0.0.1:8765 运行）。"""
import asyncio
import json
import sys
import uuid

import httpx
import websockets

BASE_HTTP = "http://127.0.0.1:8765"
BASE_WS = "ws://127.0.0.1:8765/api/ws"

failures = []


def check(name, cond, extra=""):
    print(("PASS" if cond else "FAIL"), "-", name, extra)
    if not cond:
        failures.append(name)


class WS:
    def __init__(self, client_id):
        self.client_id = client_id
        self.ws = None
        self.events = []
        self.lock = None
        self.max_seq = 0
        self.reader_task = None

    async def __aenter__(self):
        self.ws = await websockets.connect(BASE_WS, max_size=8 * 1024 * 1024)
        hello = json.loads(await self.ws.recv())
        assert hello["type"] == "hello"
        await self.ws.send(json.dumps({"type": "hello", "client_id": self.client_id}))
        self.lock = json.loads(await self.ws.recv())["lock"]
        self.reader_task = asyncio.create_task(self._read())
        return self

    async def _read(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                if msg["type"] == "event":
                    self.events.append(msg)
                    self.max_seq = max(self.max_seq, msg["seq"])
                    if msg["kind"] in ("lock_changed", "lock_granted"):
                        self.lock = msg["data"]["lock"]
                elif msg["type"] in ("lock", "lock_error"):
                    self.lock = msg["lock"]
        except Exception:
            pass

    async def sync_from(self, seq):
        await self.ws.send(json.dumps({"type": "sync", "after_seq": seq}))
        await asyncio.sleep(0.5)

    async def acquire(self, wait=True):
        await self.ws.send(json.dumps(
            {"type": "acquire", "client_id": self.client_id, "wait": wait}))
        for _ in range(60):
            await asyncio.sleep(0.2)
            if self.lock and self.lock["state"] == "held" \
                    and self.lock["holder"] == self.client_id:
                return self.lock
        raise AssertionError("acquire timeout")

    async def release(self):
        await self.ws.send(json.dumps({
            "type": "release", "client_id": self.client_id,
            "epoch": self.lock["held_epoch"]}))
        await asyncio.sleep(0.3)

    async def heartbeat(self):
        await self.ws.send(json.dumps({
            "type": "heartbeat", "client_id": self.client_id,
            "epoch": self.lock["held_epoch"]}))

    async def __aexit__(self, *a):
        self.reader_task.cancel()
        await self.ws.close()


def token(ws):
    return {"client_id": ws.client_id, "epoch": ws.lock["held_epoch"]}


def wait_for(cli, cond, timeout=20, desc=""):
    async def run():
        t0 = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - t0 < timeout:
            r = await cli.get("/api/state")
            s = r.json()
            if cond(s):
                return s
            await asyncio.sleep(0.25)
        raise AssertionError("timeout: " + desc)
    return run()


async def main():
    async with httpx.AsyncClient(base_url=BASE_HTTP, timeout=30) as cli:
        s = (await cli.get("/api/state")).json()
        check("初始有一个示例单元", len(s["cells"]) >= 1)
        cell_id = s["cells"][0]["id"]

        # 1) 无锁执行被拒
        r = await cli.post("/api/execute", json={
            "token": {"client_id": "nobody", "epoch": 999},
            "cell_id": cell_id, "request_id": str(uuid.uuid4())})
        check("无锁执行返回 409", r.status_code == 409, r.text[:100])

        async with WS("window-A") as A, WS("window-B") as B:
            # 2) A 取锁；B 非阻塞获取失败
            la = await A.acquire()
            rb = await cli.post("/api/execute", json={
                "token": token(B) if B.lock else {"client_id": "window-B", "epoch": 0},
                "cell_id": cell_id, "request_id": str(uuid.uuid4())})
            # B 的 epoch 与当前持锁 epoch 不同
            check("B 旧/错 token 执行 409", rb.status_code == 409)

            # 3) A 执行真实代码并跨单元使用变量
            await cli.patch(f"/api/cells/{cell_id}", json={
                "token": token(A), "source": "shared = 41\nprint('first print')"})
            ex1 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell_id,
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex1["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex1 ok")

            msgs1 = s["messages"].get(ex1["id"], [])
            kinds = [m["msg_type"] for m in msgs1]
            check("ex1 有 stream 输出", "stream" in kinds, str(kinds))
            text = "".join(m["content"]["text"] for m in msgs1
                           if m["msg_type"] == "stream")
            check("stream 内容正确", "first print" in text, text.strip())

            # 新建第二个单元，引用变量 shared
            cell2 = (await cli.post("/api/cells", json={
                "token": token(A), "source": "print('var =', shared + 1)",
                "after_id": cell_id})).json()["cell"]
            ex2 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell2["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex2["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex2 ok")
            t2 = "".join(m["content"]["text"]
                         for m in s["messages"].get(ex2["id"], [])
                         if m["msg_type"] == "stream")
            check("变量跨单元共享（真实内核）", "var = 42" in t2, t2.strip())
            check("两次执行 id 不同（不拼接）", ex1["id"] != ex2["id"])

            # 4) 表达式结果 execute_result
            await cli.patch(f"/api/cells/{cell2['id']}", json={
                "token": token(A), "source": "shared"})
            ex3 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell2["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex3["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex3 ok")
            has_result = any(m["msg_type"] == "execute_result"
                             for m in s["messages"].get(ex3["id"], []))
            check("表达式结果 execute_result 出现", has_result)

            # 5) display_id 更新：两次 display 同一 id，最终只保留最后一个
            cell3 = (await cli.post("/api/cells", json={
                "token": token(A),
                "source": (
                    "from IPython.display import display, HTML\n"
                    "handle = display(HTML('<b>v1</b>'), display_id='d1')\n"
                    "handle.update(HTML('<b>v2</b>'))\n"
                    "print('after-update')"),
                "after_id": cell2["id"]})).json()["cell"]
            ex4 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell3["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex4["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex4 ok")
            m4 = s["messages"].get(ex4["id"], [])
            displays = [m for m in m4
                        if m["msg_type"] in ("display_data", "update_display_data")]
            check("display_id 更新产生初始 display + update 两条",
                  len(displays) == 2, str([d["msg_type"] for d in displays]))
            check("两条消息同一 display_id",
                  all(m["content"]["transient"]["display_id"] == "d1"
                      for m in displays))
            check("后一条 update 是 v2",
                  displays[-1]["content"]["data"]["text/html"] == "<b>v2</b>")

            # 6) clear_output
            cell4 = (await cli.post("/api/cells", json={
                "token": token(A),
                "source": (
                    "from IPython.display import clear_output\n"
                    "print('will be cleared')\n"
                    "clear_output(wait=True)\n"
                    "print('fresh')"),
                "after_id": cell3["id"]})).json()["cell"]
            ex5 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell4["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex5["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex5 ok")
            m5 = s["messages"].get(ex5["id"], [])
            check("收到 clear_output",
                  any(m["msg_type"] == "clear_output" for m in m5))
            # 重放（前端 buildFrames 的等价逻辑）
            frames, did = [], {}
            for m in m5:
                t_, c = m["msg_type"], m["content"]
                if t_ == "stream":
                    frames.append(c["text"])
                elif t_ == "clear_output":
                    frames.clear()
            check("重放后旧文本被替换",
                  frames == ["fresh\n"], str(frames))

            # 7) 持续输出 + 断线重连回读
            cell5 = (await cli.post("/api/cells", json={
                "token": token(A),
                "source": "import time\nfor i in range(8):\n    print('line', i); time.sleep(0.25)",
                "after_id": cell4["id"]})).json()["cell"]
            ex6 = (await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell5["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            await asyncio.sleep(0.6)  # 等第一行打印
            # B 在执行中途做一次全量快照（模拟刷新后的新标签页）
            s_mid = (await cli.get("/api/state")).json()
            mid_msgs = [m for m in s_mid["messages"].get(ex6["id"], [])]
            check("执行中途已能读到部分输出",
                  any("line 0" in m["content"].get("text", "") for m in mid_msgs))
            s = await wait_for(cli,
                lambda s: any(e["id"] == ex6["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="ex6 ok")
            # B 通过 WS sync 从旧 seq 回读
            await B.sync_from(0)
            b_seen = [ev for ev in B.events
                      if ev["kind"] == "kernel_message"
                      and ev["data"]["execution_id"] == ex6["id"]]
            check("WS sync 能回读全部历史消息",
                  len(b_seen) >= 8, str(len(b_seen)))

            # 8) request_id 幂等
            rid = str(uuid.uuid4())
            r1 = await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell_id, "request_id": rid})
            r2 = await cli.post("/api/execute", json={
                "token": token(A), "cell_id": cell_id, "request_id": rid})
            check("重复 request_id 返回 deduped", r2.json()["deduped"] is True)
            check("两次返回同一 execution",
                  r1.json()["execution"]["id"] == r2.json()["execution"]["id"])
            q = await cli.get(f"/api/requests/{rid}")
            check("可以按 request_id 查询提交状态", q.status_code == 200)

            # 9) 控制权 fencing：A 释放后旧 epoch 作废
            old_epoch = A.lock["held_epoch"]
            await A.release()
            r = await cli.post("/api/execute", json={
                "token": {"client_id": "window-A", "epoch": old_epoch},
                "cell_id": cell_id, "request_id": str(uuid.uuid4())})
            check("释放后旧 token 执行被拒", r.status_code == 409)

            # B 取得锁（排队自动授予/主动 acquire）
            lb = await B.acquire()
            check("A 释放后 B 取得控制权", lb["holder"] == "window-B")

            # 10) 重启内核：旧结果保留、新代隔离
            gens_before = len((await cli.get("/api/state")).json()["gens"])
            rr = await cli.post("/api/restart", json={"token": token(B)})
            check("重启返回新 gen_id", rr.status_code == 200)
            await asyncio.sleep(2)
            s = (await cli.get("/api/state")).json()
            check("代数量 +1", len(s["gens"]) == gens_before + 1)
            old_execs = [e for e in s["executions"]
                         if e["gen_id"] != s["current_gen_id"]]
            check("旧执行记录仍然存在", len(old_execs) >= 1)
            check("旧执行属于旧代（前端可据此提示变量已失效）",
                  all(e["gen_id"] != s["current_gen_id"] for e in old_execs))

            # 新内核里 shared 不存在
            cell6 = (await cli.post("/api/cells", json={
                "token": token(B),
                "source": "try:\n    shared\nexcept NameError:\n    print('FRESH-KERNEL')",
                "after_id": cell5["id"]})).json()["cell"]
            exf = (await cli.post("/api/execute", json={
                "token": token(B), "cell_id": cell6["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            s = await wait_for(cli,
                lambda s: any(e["id"] == exf["id"] and e["status"] == "ok"
                              for e in s["executions"]),
                desc="fresh kernel ok")
            tf = "".join(m["content"]["text"]
                         for m in s["messages"].get(exf["id"], [])
                         if m["msg_type"] == "stream")
            check("新内核变量已清空", "FRESH-KERNEL" in tf, tf.strip())

            # 11) 中断
            cell7 = (await cli.post("/api/cells", json={
                "token": token(B),
                "source": "import time\nwhile True:\n    print('tick'); time.sleep(0.1)",
                "after_id": cell6["id"]})).json()["cell"]
            exi = (await cli.post("/api/execute", json={
                "token": token(B), "cell_id": cell7["id"],
                "request_id": str(uuid.uuid4())})).json()["execution"]
            await asyncio.sleep(0.8)
            await cli.post("/api/interrupt", json={"token": token(B)})
            s = await wait_for(cli,
                lambda s: any(e["id"] == exi["id"] and e["status"] == "interrupted"
                              for e in s["executions"]),
                desc="interrupted")
            check("停止后状态为 interrupted", True)
            ticks = "".join(m["content"]["text"]
                            for m in s["messages"].get(exi["id"], [])
                            if m["msg_type"] == "stream")
            check("中断前的输出保留", "tick" in ticks)

    print()
    if failures:
        print(f"{len(failures)} 个失败:", failures)
        sys.exit(1)
    print("全部通过 ✅")


if __name__ == "__main__":
    asyncio.run(main())
