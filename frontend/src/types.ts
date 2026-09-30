export interface Cell {
  id: string;
  order_idx: number;
  source: string;
  created_at: number;
  updated_at: number;
}

export interface KernelGen {
  id: string;
  seq: number;
  started_at: number;
  ended_at: number | null;
  status: "starting" | "alive" | "ended";
  reason: string | null;
}

export type ExecStatus =
  | "queued"
  | "running"
  | "ok"
  | "error"
  | "interrupted";

export interface Execution {
  id: string;
  cell_id: string;
  gen_id: string;
  num: number;
  source: string;
  status: ExecStatus;
  execute_count: number | null;
  queued_at: number;
  started_at: number | null;
  ended_at: number | null;
  end_reason: string | null;
  ename: string | null;
  evalue: string | null;
  messages_total: number;
  messages_retained?: number;
}

export interface StreamContent {
  name: "stdout" | "stderr";
  text: string;
}
export interface DisplayContent {
  data: Record<string, unknown>;
  metadata: Record<string, unknown>;
  transient?: { display_id?: string };
}
export interface ErrorContent {
  ename: string | null;
  evalue: string | null;
  traceback: string[];
}
export interface ClearContent {
  wait: boolean;
}
export type KernelContent =
  | StreamContent
  | DisplayContent
  | ErrorContent
  | ClearContent;

export interface KernelMessage {
  seq: number;
  msg_type:
    | "stream"
    | "display_data"
    | "update_display_data"
    | "execute_result"
    | "error"
    | "clear_output";
  content: KernelContent;
  gen_id: string;
}

export interface LockState {
  state: "free" | "held" | "grace";
  epoch: number;
  holder: string | null;
  held_epoch: number;
  lease_expires: number;
  waiters: string[];
}

export interface JournalEvent {
  seq: number;
  kind: string;
  data: any;
}

export interface Snapshot {
  cells: Cell[];
  gens: KernelGen[];
  executions: Execution[];
  messages: Record<string, KernelMessage[]>;
  max_seq: number;
  lock: LockState;
  current_gen_id: string | null;
  kernel_alive: boolean;
  config: {
    lease_seconds: number;
    heartbeat_seconds: number;
    disconnect_grace: number;
    max_kernel_messages: number;
  };
}
