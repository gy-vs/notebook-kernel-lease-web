import React, { useEffect, useState } from "react";
import type { LockState } from "../types";
import { myClientId, acquireLock, releaseLock, getConnStatus } from "../ws";
import { api, ApiError, type LockToken } from "../net";

export const TopBar: React.FC<{
  lock: LockState | null;
  online: boolean;
  currentGenSeq: number | null;
  kernelAlive: boolean;
  busy: boolean;
}> = ({ lock, online, currentGenSeq, kernelAlive, busy }) => {
  const me = myClientId();
  const [, tick] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  // 租约倒计时强制刷新
  useEffect(() => {
    const t = setInterval(() => tick((x) => x + 1), 1000);
    return () => clearInterval(t);
  }, []);

  const holder = lock?.holder;
  const heldByMe = lock?.state === "held" && holder === me;
  const graceForMe = lock?.state === "grace" && holder === me;
  const waiting =
    lock?.state === "held" &&
    holder !== me &&
    lock.waiters.includes(me);
  const token: LockToken | null = heldByMe && lock
    ? { client_id: me, epoch: lock.held_epoch }
    : null;

  const doInterrupt = async () => {
    if (!token) return;
    setErr(null);
    try {
      await api.interrupt(token);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) setErr("控制权已失效");
    }
  };
  const doRestart = async () => {
    if (!token) return;
    if (!confirm("重启内核？代码和历史结果会保留，但变量会全部清空（新内核与旧内核分开）。")) return;
    setErr(null);
    try {
      await api.restart(token);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) setErr("控制权已失效");
    }
  };
  const addCell = async () => {
    if (!token) return;
    try {
      await api.createCell(token, "", null);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) setErr("控制权已失效");
    }
  };

  const leaseLeft = lock
    ? Math.max(0, Math.ceil(lock.lease_expires - Date.now() / 1000))
    : 0;

  return (
    <header className="topbar">
      <div className="brand">🐍 本地实验笔记</div>

      <div className={`conn conn-${getConnStatus()}`}>
        <span className="dot" />
        {online ? "已连接" : "连接断开，重连中…（结果由后端保存）"}
      </div>

      <div className={`kernel ${kernelAlive ? "on" : "off"}`}>
        内核：{kernelAlive ? `运行中（第 ${currentGenSeq ?? "?"} 代）` : "未启动 / 已停止"}
        {busy && <span className="kernel-busy"> busy</span>}
      </div>

      <div className="lock-widget">
        {heldByMe ? (
          <>
            <span className="badge badge-held">
              持有控制权 · 租约 {leaseLeft}s
            </span>
            <button className="btn" onClick={() => lock && releaseLock(lock.held_epoch)}>
              释放
            </button>
          </>
        ) : graceForMe ? (
          <span className="badge badge-grace">控制权宽限中，等待重连…</span>
        ) : waiting ? (
          <span className="badge badge-wait">
            排队等待控制权（前面 {lock!.waiters.indexOf(me)} 个窗口）
          </span>
        ) : (
          <>
            <span className="badge badge-none">
              {lock?.state === "free"
                ? "控制权空闲"
                : holder
                  ? "其他窗口持有控制权（只读）"
                  : "无控制权"}
            </span>
            <button
              className="btn btn-primary"
              disabled={!online}
              onClick={() => { setErr(null); acquireLock(true); }}
            >
              {lock?.state === "free" ? "取得控制权" : "请求控制权"}
            </button>
          </>
        )}
      </div>

      <div className="actions">
        <button className="btn" disabled={!token} title="在末尾新增单元" onClick={() => void addCell()}>
          ＋ 新单元
        </button>
        <button className="btn btn-warn" disabled={!token || !busy}
          title="中断当前执行（停止按钮）" onClick={() => void doInterrupt()}>
          ■ 停止
        </button>
        <button className="btn btn-danger" disabled={!token}
          title="重启内核：旧结果保留，变量清空" onClick={() => void doRestart()}>
          ↻ 重启内核
        </button>
      </div>

      {err && <span className="banner banner-warn topbar-err">{err}</span>}
    </header>
  );
};
