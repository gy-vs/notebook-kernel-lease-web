import type { KernelMessage } from "./types";

export interface StreamFrame {
  kind: "stream";
  name: "stdout" | "stderr";
  text: string;
}
export interface DisplayFrame {
  kind: "display" | "result";
  data: Record<string, unknown>;
  metadata: Record<string, unknown>;
  displayId: string | null;
}
export interface ErrorFrame {
  kind: "error";
  ename: string | null;
  evalue: string | null;
  traceback: string[];
}
export type Frame = StreamFrame | DisplayFrame | ErrorFrame;

/**
 * 按消息顺序重放一次执行的全部消息，得到与一直在线时一致的最终表达：
 * - stream 相邻且同名时合并；
 * - display_data/execute_result 带 display_id 时更新同一个帧；
 * - clear_output 清空此前累积的全部帧（wait=True 在重放语义下等价）。
 */
export function buildFrames(messages: KernelMessage[]): Frame[] {
  const frames: Frame[] = [];
  const displayIndex = new Map<string, number>();

  for (const m of messages) {
    const c = m.content as any;
    switch (m.msg_type) {
      case "stream": {
        const last = frames[frames.length - 1];
        if (last && last.kind === "stream" && last.name === (c.name ?? "stdout")) {
          last.text += c.text ?? "";
        } else {
          frames.push({ kind: "stream", name: c.name ?? "stdout", text: c.text ?? "" });
        }
        break;
      }
      case "display_data":
      case "update_display_data":
      case "execute_result": {
        const displayId: string | null = c?.transient?.display_id ?? null;
        const kind: DisplayFrame["kind"] =
          m.msg_type === "execute_result" ? "result" : "display";
        const frame: DisplayFrame = {
          kind,
          data: c.data ?? {},
          metadata: c.metadata ?? {},
          displayId,
        };
        // update_display_data 只能更新已存在的 display_id（Jupyter 协议语义）
        if (displayId !== null && displayIndex.has(displayId)) {
          frames[displayIndex.get(displayId)!] = frame;
        } else if (m.msg_type !== "update_display_data") {
          if (displayId !== null) displayIndex.set(displayId, frames.length);
          frames.push(frame);
        }
        break;
      }
      case "error":
        frames.push({
          kind: "error",
          ename: c.ename ?? null,
          evalue: c.evalue ?? null,
          traceback: c.traceback ?? [],
        });
        break;
      case "clear_output":
        frames.length = 0;
        displayIndex.clear();
        break;
    }
  }
  return frames;
}

// ANSI 转义（traceback 里带颜色码）
// eslint-disable-next-line no-control-regex
const ANSI_RE = /\x1b\[[0-9;]*m/g;

export function plainTraceback(tb: string[]): string {
  return tb.map((l) => l.replace(ANSI_RE, "")).join("\n");
}
