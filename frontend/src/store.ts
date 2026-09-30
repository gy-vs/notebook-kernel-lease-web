import { useSyncExternalStore } from "react";
import type {
  Cell,
  Execution,
  JournalEvent,
  KernelGen,
  KernelMessage,
  LockState,
  Snapshot,
} from "./types";

/**
 * 全量状态 + journal 增量更新的单一 store。
 * 消息按 execution_id 分桶，且只依据 journal 事件追加（执行归属来自后端）。
 */
export interface AppState {
  loaded: boolean;
  maxSeq: number;
  cells: Map<string, Cell>;
  cellOrder: string[];
  gens: Map<string, KernelGen>;
  executions: Map<string, Execution>;
  messages: Map<string, KernelMessage[]>; // execution_id -> 按 seq 有序
  lock: LockState | null;
  currentGenId: string | null;
  kernelAlive: boolean;
  config: Snapshot["config"] | null;
}

let state: AppState = {
  loaded: false,
  maxSeq: 0,
  cells: new Map(),
  cellOrder: [],
  gens: new Map(),
  executions: new Map(),
  messages: new Map(),
  lock: null,
  currentGenId: null,
  kernelAlive: false,
  config: null,
};

const listeners = new Set<() => void>();
function emit() {
  for (const l of listeners) l();
}
function setState(mut: (s: AppState) => void) {
  const draft: AppState = { ...state, cells: new Map(state.cells),
    cellOrder: [...state.cellOrder], gens: new Map(state.gens),
    executions: new Map(state.executions), messages: new Map(state.messages) };
  mut(draft);
  state = draft;
  emit();
}

function resortCells(s: AppState) {
  s.cellOrder = [...s.cells.values()]
    .sort((a, b) => a.order_idx - b.order_idx)
    .map((c) => c.id);
}

function deriveCurrentGen(s: AppState): string | null {
  // 服务器内存里的内核必然是最新的 alive/starting 代；ended 代不算
  let best: KernelGen | null = null;
  for (const g of s.gens.values()) {
    if (g.status === "ended") continue;
    if (!best || g.seq > best.seq) best = g;
  }
  return best?.id ?? null;
}

export function loadSnapshot(snap: Snapshot) {
  setState((s) => {
    s.loaded = true;
    s.maxSeq = snap.max_seq;
    s.cells = new Map(snap.cells.map((c) => [c.id, c]));
    resortCells(s);
    s.gens = new Map(snap.gens.map((g) => [g.id, g]));
    s.executions = new Map(snap.executions.map((e) => [e.id, e]));
    s.messages = new Map();
    for (const [eid, msgs] of Object.entries(snap.messages)) {
      s.messages.set(eid, [...msgs].sort((a, b) => a.seq - b.seq));
    }
    s.lock = snap.lock;
    // 以服务器内存中的内核为准；快照缺失时从 alive 代推导
    s.currentGenId = snap.current_gen_id ?? deriveCurrentGen(s);
    s.kernelAlive = snap.kernel_alive;
    s.config = snap.config;
  });
}

export function applyEvent(ev: JournalEvent) {
  if (ev.seq <= state.maxSeq) return; // 同步与实时重叠时去重
  const { kind, data } = ev;
  setState((s) => {
    s.maxSeq = Math.max(s.maxSeq, ev.seq);
    switch (kind) {
      case "cell_created": {
        const c: Cell = data.cell;
        s.cells.set(c.id, c);
        resortCells(s);
        break;
      }
      case "cell_updated":
        if (s.cells.has(data.id)) {
          s.cells.set(data.id, { ...s.cells.get(data.id)!, source: data.source,
            updated_at: Date.now() / 1000 });
        }
        break;
      case "cell_deleted": {
        s.cells.delete(data.id);
        resortCells(s);
        break;
      }
      case "gen_started":
      case "gen_updated": {
        s.gens.set(data.gen.id, data.gen as KernelGen);
        s.currentGenId = deriveCurrentGen(s);
        break;
      }
      case "execution_created":
      case "execution_updated": {
        const ex: Execution = data.execution;
        const prev = s.executions.get(ex.id);
        s.executions.set(ex.id, { ...prev, ...ex });
        break;
      }
      case "kernel_message": {
        const m: KernelMessage = {
          seq: ev.seq,
          msg_type: data.msg_type,
          content: data.content,
          gen_id: data.gen_id,
        };
        const bucket = s.messages.get(data.execution_id) ?? [];
        s.messages.set(data.execution_id, [...bucket, m]);
        // 实时消息：后端仍保留数即此刻已存储的总数
        const exNow = s.executions.get(data.execution_id);
        if (exNow) {
          s.executions.set(data.execution_id, {
            ...exNow,
            messages_retained: Math.max(exNow.messages_retained ?? 0, bucket.length + 1),
          });
        }
        break;
      }
      case "lock_changed":
      case "lock_granted":
        s.lock = data.lock;
        break;
    }
  });
}

export function setLiveKernelInfo(genId: string | null, alive: boolean) {
  setState((s) => {
    s.currentGenId = genId;
    s.kernelAlive = alive;
  });
}

/** WS 直接下发的状态。kernel_status 更新内核；lock 只更新锁。 */
export function applyLockState(lock: LockState) {
  setState((s) => {
    s.lock = lock;
  });
}

export function getState() {
  return state;
}

export function subscribe(cb: () => void) {
  listeners.add(cb);
  return () => listeners.delete(cb);
}

export function useStore<T>(selector: (s: AppState) => T): T {
  return useSyncExternalStore(
    subscribe,
    () => selector(state),
    () => selector(state),
  );
}
