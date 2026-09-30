# 本地 Python 实验笔记（local-lab）

一个只在本机使用的全栈应用：后端用 Python + `jupyter_client` 管理真实的
`ipykernel`，前端用 React + TypeScript。同一份笔记可以在两个标签页打开：
一个持有执行控制权写代码、运行/停止/重启，另一个只读查看结果。

## 解决的核心问题（设计要点）

1. **真实内核，不是一次性子进程**
   变量在单元之间真实共享，因为所有执行都提交给同一个 `ipykernel`
   （`jupyter_client` 标准协议，不自行重写内核通信）。刷新页面或短暂断线
   都不会重启 Python 进程。

2. **控制权是后端裁决的单写者租约锁，不靠按钮禁用或使用者自觉**
   - 同一时刻只有一个 `client_id` 持锁；所有变更接口（执行、编辑、中断、
     重启）后端都强制校验 fencing token（`client_id + epoch`），错误 token
     一律 `409 LOCK_LOST`。
   - 状态三态可见：**空闲 / 持有 / 宽限（断线等待恢复）**，页面顶栏实时显示。
   - 持锁连接断开给 3 秒宽限（刷新页面用），宽限内同窗口带同一 epoch 重连即恢复，
     期间其他窗口仍不能介入；宽限一过 epoch 立即作废，旧窗口网络恢复后也不能
     凭旧状态重新干预内核。
   - 持锁窗口退出后，排队中的查看窗口自动获得锁（或点“请求控制权”获取）。
   - 每个标签页的身份存在 `sessionStorage`（独立、刷新保留、关闭即清），
     避免两个标签页共享身份。

3. **结果的保存与页面的连接是两件事**
   每条内核消息先持久化到 SQLite journal（带全局单调 `seq`），再广播给
   WebSocket。重连时按 `seq` 回读断线期间的全部事件，所以打印第一行后刷新，
   恢复时仍能看到已产生内容与后续结果，而不是只有重连后的半段。

4. **输出有可回读的执行归属**
   消息按内核父消息 id 归属到具体的一次执行（`execution_id`），绝不按
   “最后连接的请求”归属。同一单元重新运行生成一张**新的执行卡片**，
   新旧结果分开展示，文字不会拼接。

5. **`display_id` 更新与 `clear_output` 语义在回读时一致**
   前端按 `seq` 重放消息：`update_display_data` 原地更新同 display_id 的帧；
   `clear_output` 清空此前帧。刷新后的页面与一直在线的页面表达同一个结果。

6. **有明确的输出保留范围，且缺了会明说**
   journal 最多保留 `MAX_KERNEL_MESSAGES`（默认 3000）条内核消息，超出淘汰
   最旧的；结构化事件（单元/执行/代/锁）始终保留。页面通过
   `messages_total` 与“仍保留条数”区分“断线暂缺”和“已超范围永久淘汰”，
   分别给出明确提示，绝不把不完整内容显示成完整执行。

7. **重启内核 = 新的一代（generation）**
   代码与历史结果全部保留；新内核有独立的 `gen_id` 和独立进程，旧执行卡片
   标注“旧内核结果，变量不保证存在”。重启会把正在运行/排队的执行标记为
   `interrupted`。

8. **提交未确认可查询，不会因网络抖动重复运行**
   每次执行由前端生成 `request_id`；后端对 `request_id` 幂等
   （`client_requests` 表）。提交后没收到响应时，前端调用
   `GET /api/requests/{id}` 查询，绝不重发执行。

9. **服务重启不假装旧进程还在运行**
   启动时把所有遗留的未结束代标记为 `ended(server_restart)`，把悬挂的
   queued/running 执行标记为 `interrupted(orphaned)`，页面明确显示
   “旧内核已结束，无法继续”，而不是一直转圈。

## 运行

需要 Python 3.11+（已用 3.11 验证）和 Node 18+。

```bash
# 1) Python 依赖
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2) 前端依赖并构建（产物输出到 frontend_dist/，由后端托管）
cd frontend
npm install
npm run build
cd ..

# 3) 启动
./run.sh
# 或：.venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8765
```

打开 <http://127.0.0.1:8765> ，再用浏览器“复制标签页”或同一地址再开一个窗口。

前端开发模式（热更新，API 走 Vite 代理到 8765）：

```bash
# 终端 A：起后端
.venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8765
# 终端 B：
cd frontend && npm run dev   # http://127.0.0.1:5173
```

## 两个窗口的演示流程

1. 窗口 A 点 **取得控制权**，窗口 B 显示“其他窗口持有控制权（只读）”。
2. 在 A 写 `shared = 41` 运行，再建单元写 `print(shared + 1)` 运行 → 输出 42，
   验证真实内核的跨单元状态。
3. 在 B 里所有编辑/运行按钮禁用；即使绕过按钮直接打 API，后端返回 409。
4. 运行一个持续打印的单元，打印几行后**在 A 刷新页面**：重连后能看到
   刷新前已打印的行和后续行，且这次执行是一张连续的卡片。
5. 顶栏 **停止** 中断当前执行；**重启内核** 后旧结果保留但标为旧代，
   新内核里引用 `shared` 会 `NameError`。
6. A 关闭标签页：B 排队等待后自动获得控制权（或点请求控制权），继续操作。

## 测试

端到端测试需要先把服务跑在 8765：

```bash
PYTHONPATH=. .venv/bin/python tests/e2e_runner.py
```

覆盖：无锁/错 token 拒绝、真实执行与变量共享、执行归属与不拼接、
execute_result、`display_id`/`update_display_data`、`clear_output` 重放、
中途输出与 WS 回读、`request_id` 幂等与状态查询、释放后旧 token 失效、
控制权交接、重启代际隔离与变量清空、中断、中断前输出保留。

## 代码结构

```
backend/
  config.py   端口、租约时长(10s)、心跳(3s)、断线宽限(3s)、保留范围(3000)
  db.py       SQLite：cells / kernel_gens / executions / client_requests / journal
  lock.py     单写者租约锁（epoch fencing、宽限、排队交接、reaper）
  kernel.py   jupyter_client 管理 ipykernel 代际；iopub 监听→持久化→广播；执行队列
  bus.py      进程内发布订阅
  main.py     FastAPI：REST(全部过锁校验) + WebSocket(事件流/锁/回读) + 静态托管
frontend/src/
  store.ts    journal 增量状态机（按 seq 去重）
  ws.ts       WebSocket 连接、重连回读、心跳、宽限恢复
  output.ts   消息重放（display_id / clear_output）
  components/ TopBar（连接/控制权/内核状态/停止/重启）、CellView、ExecutionCard、Mime
```

## 范围外（按需求）

- 不做密码登录与交互式 stdin（`allow_stdin=False`）；
- 不做文件目录、账号后台、多人协同编辑；
- 服务重启不恢复旧 Python 进程，只如实标注旧执行已结束。
