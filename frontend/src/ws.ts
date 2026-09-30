import { api, getClientId } from "./net";
import { applyEvent, applyLockState, getState, loadSnapshot, setLiveKernelInfo } from "./store";

export type ConnStatus = "connecting" | "online" | "offline";

type Listener = () => void;
const connListeners = new Set<Listener>();
let connStatus: ConnStatus = "offline";
function setConn(s: ConnStatus) {
  if (s === connStatus) return;
  connStatus = s;
  connListeners.forEach((l) => l());
}
export const onConnChange = (l: Listener) => {
  connListeners.add(l);
  return () => connListeners.delete(l);
};
export const getConnStatus = () => connStatus;

let ws: WebSocket | null = null;
let didInitialLoad = false;
let reconnectDelay = 400;
let stopped = false;
let clientId = "";
let lastHeldEpoch: number | null = null;

function send(obj: unknown) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

async function handleMessage(msg: any) {
  switch (msg.type) {
    case "hello":
      break;
    case "event":
      applyEvent({ seq: msg.seq, kind: msg.kind, data: msg.data });
      if (msg.kind === "lock_changed" || msg.kind === "lock_granted") {
        rememberHeldEpoch(msg.data?.lock);
      }
      break;
    case "synced":
      // 重叠事件已在 applyEvent 里按 seq 去重
      break;
    case "kernel_status":
      setLiveKernelInfo(msg.current_gen_id ?? null, Boolean(msg.kernel_alive));
      break;
    case "lock":
      if (msg.lock) {
        applyLock(msg.lock);
        rememberHeldEpoch(msg.lock);
      }
      break;
    case "lock_error":
      if (msg.lock) applyLock(msg.lock);
      break;
  }
}

function applyLock(lock: any) {
  applyLockState(lock as never);
}

function rememberHeldEpoch(lock: any) {
  if (!lock) return;
  if (lock.state === "held" && lock.holder === clientId) {
    lastHeldEpoch = lock.held_epoch;
  } else if (lock.state === "free") {
    lastHeldEpoch = null;
  }
}

async function onOpen() {
  setConn("connecting");
  reconnectDelay = 400;
  send({ type: "hello", client_id: clientId });
  if (!didInitialLoad) {
    try {
      const snap = await api.state();
      loadSnapshot(snap);
      didInitialLoad = true;
    } catch {
      scheduleReconnect();
      return;
    }
  } else {
    // 重连：先回读断线期间缺失的事件（含锁事件），再尝试从宽限期恢复
    send({ type: "sync", after_seq: getState().maxSeq });
    await new Promise((r) => setTimeout(r, 250));
    const lk = getState().lock;
    if (lk && (lk.state === "grace" || lk.state === "held")
        && lk.holder === clientId) {
      send({ type: "heartbeat", client_id: clientId, epoch: lk.held_epoch });
    } else if (lastHeldEpoch !== null) {
      // 宽限已过：旧 token 会被拒（lock_error），UI 转为“重新请求”
      send({ type: "heartbeat", client_id: clientId, epoch: lastHeldEpoch });
    }
  }
  setConn("online");
}

function scheduleReconnect() {
  if (stopped) return;
  setConn("offline");
  const delay = Math.min(reconnectDelay, 5000);
  reconnectDelay = Math.min(reconnectDelay * 1.6, 5000);
  setTimeout(connect, delay);
}

function connect() {
  if (stopped) return;
  setConn("connecting");
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/api/ws`);
  ws.onopen = () => { void onOpen(); };
  ws.onmessage = (e) => { void handleMessage(JSON.parse(e.data)); };
  ws.onclose = () => scheduleReconnect();
  ws.onerror = () => { ws?.close(); };
}

export function startNet() {
  if (stopped === false && ws) return;
  stopped = false;
  clientId = getClientId();
  connect();
}

// ---------------- 锁操作 ----------------
export function acquireLock(wait = true) {
  send({ type: "acquire", client_id: clientId, wait });
}
export function releaseLock(epoch: number) {
  send({ type: "release", client_id: clientId, epoch });
}
export function heartbeat(epoch: number) {
  send({ type: "heartbeat", client_id: clientId, epoch });
}

// ---------------- 心跳循环 ----------------
setInterval(() => {
  const s = getState();
  if (!s.lock || connStatus !== "online") return;
  if (s.lock.holder === clientId && s.lock.state === "held") {
    heartbeat(s.lock.held_epoch);
  }
}, 2500);

export const myClientId = () => clientId;
