import React, { useEffect, useRef, useState } from "react";
import type { Cell, Execution, KernelGen, KernelMessage, LockState } from "../types";
import { useStore } from "../store";
import { api, ApiError, newRequestId, type LockToken } from "../net";
import { myClientId } from "../ws";
import { ExecutionCard } from "./ExecutionCard";

function genLabel(g: KernelGen | undefined): string {
  return g ? `#${g.seq}` : "?";
}

export const CellView: React.FC<{
  cell: Cell;
  lock: LockState | null;
  isHolder: boolean;
  online: boolean;
}> = ({ cell, lock, isHolder, online }) => {
  const executions = useStore((s) =>
    [...s.executions.values()]
      .filter((e) => e.cell_id === cell.id)
      .sort((a, b) => b.num - a.num),
  );
  const gens = useStore((s) => s.gens);
  const currentGenId = useStore((s) => s.currentGenId);

  const [draft, setDraft] = useState(cell.source);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const taRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    // 远端（持有者）改动时同步非脏草稿
    if (!dirty) setDraft(cell.source);
  }, [cell.source]); // eslint-disable-line react-hooks/exhaustive-deps

  const token: LockToken | null =
    isHolder && lock ? { client_id: myClientId(), epoch: lock.held_epoch } : null;

  const autosize = () => {
    const el = taRef.current;
    if (el) {
      el.style.height = "0px";
      el.style.height = el.scrollHeight + "px";
    }
  };
  useEffect(autosize, [draft]);

  const save = async () => {
    if (!dirty || !token) return;
    setSaving(true);
    try {
      await api.updateCell(token, cell.id, draft);
      setDirty(false);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) setSubmitError("控制权已失效，无法保存");
      else setSubmitError("保存失败（网络）");
    } finally {
      setSaving(false);
    }
  };

  const run = async () => {
    if (!token) return;
    setSubmitError(null);
    // 先保存，再执行
    if (dirty) {
      try {
        await api.updateCell(token, cell.id, draft);
        setDirty(false);
      } catch (e) {
        if (e instanceof ApiError && e.status === 409) {
          setSubmitError("控制权已失效，未执行");
        } else {
          setSubmitError("保存失败，未执行");
        }
        return;
      }
    }
    const requestId = newRequestId();
    try {
      await api.execute(token, cell.id, requestId);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setSubmitError("控制权已失效，执行被后端拒绝");
        return;
      }
      // 网络失败：不确定后端是否收到，查询幂等状态，绝不重复执行
      setSubmitError("提交未确认，正在查询后端状态…");
      try {
        const r = await api.queryRequest(requestId);
        setSubmitError(r
          ? `执行已被后端接收（#${r.execution.num}），无需重复提交`
          : null);
      } catch {
        setSubmitError("无法确认提交状态：恢复连接后请手动查询，不要重复点击运行");
      }
    }
  };

  const del = async () => {
    if (!token || !confirm("删除这个单元及其全部历史结果？")) return;
    try {
      await api.deleteCell(token, cell.id);
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) setSubmitError("控制权已失效");
    }
  };

  const running = executions.some(
    (e) => e.status === "running" || e.status === "queued",
  );

  return (
    <section className="cell">
      <div className="cell-toolbar">
        <button
          className="btn btn-run"
          disabled={!isHolder || !online}
          title={isHolder ? "运行（Ctrl+Enter）" : "取得执行控制权后才能运行"}
          onClick={() => void run()}
        >
          {running ? "运行中 ●" : "▶ 运行"}
        </button>
        <button
          className="btn"
          disabled={!isHolder || !online}
          title="在下方插入新单元"
          onClick={() => token && api.createCell(token, "", cell.id)}
        >
          ＋
        </button>
        <button
          className="btn btn-danger"
          disabled={!isHolder || !online}
          title="删除单元"
          onClick={() => void del()}
        >
          删除
        </button>
        {dirty && (
          <span className="dirty-hint">{saving ? "保存中…" : "未保存（失焦自动保存）"}</span>
        )}
        {!isHolder && <span className="readonly-hint">只读</span>}
      </div>

      <textarea
        ref={taRef}
        className="cell-editor"
        value={draft}
        spellCheck={false}
        readOnly={!isHolder}
        placeholder={isHolder ? "输入 Python 代码…" : "只读：当前窗口没有执行控制权"}
        onChange={(e) => { setDraft(e.target.value); setDirty(true); }}
        onBlur={() => void save()}
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            void run();
          }
          if ((e.ctrlKey || e.metaKey) && e.key === "s") {
            e.preventDefault();
            void save();
          }
        }}
      />

      {submitError && <div className="banner banner-warn">{submitError}</div>}

      <div className="exec-list">
        {executions.map((ex: Execution) => (
          <ExecutionCardWrapper
            key={ex.id}
            ex={ex}
            eid={ex.id}
            label={genLabel(gens.get(ex.gen_id))}
            isCurrentGen={ex.gen_id === currentGenId}
          />
        ))}
      </div>
    </section>
  );
};

const ExecutionCardWrapper: React.FC<{
  ex: Execution;
  eid: string;
  label: string;
  isCurrentGen: boolean;
}> = ({ ex, eid, label, isCurrentGen }) => {
  const msgs = useStoreMsg(eid);
  return (
    <ExecutionCard ex={ex} messages={msgs} genLabel={label} isCurrentGen={isCurrentGen} />
  );
};

// 每个执行单独订阅自己的消息桶
function useStoreMsg(eid: string): KernelMessage[] {
  return useStore((s) => s.messages.get(eid) ?? []);
}
