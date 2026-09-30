import React from "react";
import type { Execution, KernelMessage } from "../types";
import { buildFrames, plainTraceback } from "../output";
import { Mime } from "./Mime";

const STATUS_TEXT: Record<string, string> = {
  queued: "排队中…",
  running: "运行中…",
  ok: "完成",
  error: "出错",
  interrupted: "已停止",
};

const END_REASON_TEXT: Record<string, string> = {
  reply: "",
  interrupted: "被中断",
  orphaned: "服务重启，此执行所在的旧内核已结束，无法继续",
  kernel_crashed: "内核进程崩溃，执行终止",
  submit_failed: "提交失败",
  restarted: "内核已重启，执行终止",
  server_stop: "服务已停止",
};

export const ExecutionCard: React.FC<{
  ex: Execution;
  messages: KernelMessage[];
  genLabel: string;
  isCurrentGen: boolean;
}> = ({ ex, messages, genLabel, isCurrentGen }) => {
  const frames = buildFrames(messages);
  // messages_total：后端累计的真实条数；retained：仍保留条数；local：本页回读到的
  const localCount = messages.length;
  const retained = ex.messages_retained ?? localCount;
  const missing = Math.max(0, retained - localCount);
  const pruned = Math.max(0, ex.messages_total - retained);
  const stillRunning = ex.status === "queued" || ex.status === "running";
  const endReason = END_REASON_TEXT[ex.end_reason ?? ""] ?? "";

  return (
    <div className={`exec exec-${ex.status} ${isCurrentGen ? "" : "exec-oldgen"}`}>
      <div className="exec-head">
        <span className="exec-num">#{ex.num}</span>
        <span className={`exec-status dot-${ex.status}`}>
          {stillRunning && <span className="spinner" />}
          {STATUS_TEXT[ex.status]}
        </span>
        <span className="exec-meta">
          {ex.execute_count != null ? `In[${ex.execute_count}]` : ""} · 内核 {genLabel}
          {!isCurrentGen ? "（旧）" : ""}
        </span>
      </div>

      {!isCurrentGen && ex.status === "ok" && (
        <div className="banner banner-warn">
          这是旧内核的结果。重启后变量已清空，新内核中不保证这些变量仍然存在。
        </div>
      )}

      {ex.end_reason === "orphaned" && (
        <div className="banner banner-warn">{endReason}</div>
      )}
      {ex.status === "interrupted" && ex.end_reason && ex.end_reason !== "orphaned" && (
        <div className="banner banner-info">{endReason}</div>
      )}

      <div className="frames">
        {frames.map((f, i) => {
          if (f.kind === "stream") {
            return (
              <pre key={i} className={`frame-stream frame-${f.name}`}>{f.text}</pre>
            );
          }
          if (f.kind === "error") {
            return (
              <pre key={i} className="frame-error">
                {f.ename ? `${f.ename}: ${f.evalue ?? ""}\n` : ""}
                {plainTraceback(f.traceback)}
              </pre>
            );
          }
          return (
            <div key={i} className={`frame-${f.kind}`}>
              <Mime data={f.data} metadata={f.metadata} />
            </div>
          );
        })}
      </div>

      {missing > 0 && (
        <div className="banner banner-missing">
          ⚠ 输出不完整：后端仍保留 {retained} 条消息，本页只回读到{" "}
          {localCount} 条，缺少 {missing} 条（断线期间产生、尚未补齐）。
          {stillRunning ? "执行仍在进行，后续消息会继续显示。" : ""}
        </div>
      )}
      {pruned > 0 && (
        <div className="banner banner-missing">
          ⚠ 有 {pruned} 条最早的输出已超出保留范围被淘汰，上面的内容不是本次执行的完整输出。
        </div>
      )}
      {missing === 0 && pruned === 0 && ex.messages_total > 0 && stillRunning && (
        <div className="banner banner-info">已回读全部已产生的输出，等待后续…</div>
      )}
    </div>
  );
};
