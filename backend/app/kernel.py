"""Owns the ipykernel subprocess on a dedicated thread.

Everything that touches pyzmq sockets happens here; the asyncio layer talks
to it only through a thread-safe command queue and an event queue.  The
kernel communication protocol itself is 100% jupyter_client — we never speak
the wire protocol ourselves.
"""
from __future__ import annotations

import queue
import threading
import time
import traceback
from typing import Any

from jupyter_client import KernelManager


def _content(msg: dict) -> dict:
    return msg.get("content", {}) or {}


class KernelRunner:
    def __init__(self) -> None:
        self.commands: "queue.Queue[tuple[str, Any]]" = queue.Queue()
        self.events: "queue.Queue[dict]" = queue.Queue()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, name="kernel-runner", daemon=True)

    # ---- used from the asyncio side ---------------------------------------
    def start(self) -> None:
        self.thread.start()

    def wait_ready(self, timeout: float = 30) -> None:
        self._ready.wait(timeout)

    def cmd(self, name: str, payload: Any = None) -> None:
        self.commands.put((name, payload))

    def drain_events(self) -> list[dict]:
        out: list[dict] = []
        try:
            while True:
                out.append(self.events.get_nowait())
        except queue.Empty:
            return out

    def shutdown(self) -> None:
        self._stop.set()
        self.cmd("shutdown")
        self.thread.join(timeout=10)

    # ---- the thread --------------------------------------------------------
    def _emit(self, etype: str, **payload: Any) -> None:
        self.events.put({"type": etype, "ts": time.time(), **payload})

    def _run(self) -> None:
        km = KernelManager(kernel_name="python3")
        km.start_kernel()
        try:
            kc = km.client()
            kc.start_channels()
            try:
                kc.wait_for_ready(timeout=30)
            except Exception:
                pass  # we'll still try to work; events will reveal problems

            # msg_id -> execution_id for requests currently in flight
            in_flight: dict[str, str] = {}
            self._ready.set()
            self._emit("kernel_ready")

            poll = queue.Empty
            while not self._stop.is_set():
                # 1) commands
                try:
                    while True:
                        name, payload = self.commands.get_nowait()
                        if name == "shutdown":
                            self._stop.set()
                            break
                        elif name == "execute":
                            exec_id, code = payload
                            msg_id = kc.execute(code, reply=False, stop_on_error=False)
                            in_flight[msg_id] = exec_id
                        elif name == "interrupt":
                            try:
                                km.interrupt_kernel()
                            except Exception as e:
                                self._emit("interrupt_failed", error=str(e))
                        elif name == "restart":
                            in_flight.clear()
                            self._emit("restart_begin")
                            try:
                                kc.stop_channels()
                                km.restart_kernel(now=True)
                                kc.start_channels()
                                try:
                                    kc.wait_for_ready(timeout=30)
                                except Exception:
                                    pass
                                # flush anything stale queued on the sockets
                                self._drain_quiet(kc)
                                self._emit("restart_end")
                            except Exception as e:
                                self._emit("restart_failed", error=str(e))
                except queue.Empty:
                    pass
                if self._stop.is_set():
                    break

                # 2) iopub messages (transformed, attributed)
                self._pump_iopub(kc, in_flight)

                # 3) shell replies (authoritative terminal status)
                self._pump_shell(kc, in_flight)

                # 4) kernel death detection -> auto restart
                if not km.is_alive():
                    in_flight.clear()
                    self._emit("kernel_died")
                    if self._stop.is_set():
                        break
                    try:
                        km.start_kernel()
                        kc.start_channels()
                        try:
                            kc.wait_for_ready(timeout=30)
                        except Exception:
                            pass
                        self._drain_quiet(kc)
                        self._emit("kernel_ready")
                    except Exception as e:
                        self._emit("kernel_start_failed", error=str(e))
                        time.sleep(1.0)

                time.sleep(0.01)
        except Exception:
            self._emit("runner_error", error=traceback.format_exc())
        finally:
            try:
                kc.stop_channels()
            except Exception:
                pass
            try:
                km.shutdown_kernel(now=True)
            except Exception:
                pass
            self._emit("runner_stopped")

    @staticmethod
    def _drain_quiet(kc) -> None:
        for ch in (kc.iopub_channel, kc.shell_channel):
            try:
                while ch.msg_ready():
                    ch.get_msg()
            except Exception:
                pass

    def _pump_iopub(self, kc, in_flight: dict[str, str]) -> None:
        ch = kc.iopub_channel
        try:
            while ch.msg_ready():
                msg = ch.get_msg()
                self._handle_iopub(msg, in_flight)
        except Exception as e:
            self._emit("channel_error", error=f"iopub: {e}")

    def _pump_shell(self, kc, in_flight: dict[str, str]) -> None:
        ch = kc.shell_channel
        try:
            while ch.msg_ready():
                msg = ch.get_msg()
                parent = msg.get("parent_header", {}) or {}
                msg_id = parent.get("msg_id")
                exec_id = in_flight.pop(msg_id, None)
                if msg.get("msg_type") != "execute_reply" or exec_id is None:
                    continue
                c = _content(msg)
                self._emit(
                    "execute_reply",
                    execution_id=exec_id,
                    status=c.get("status", "ok"),
                    execution_count=c.get("execution_count"),
                    ename=c.get("ename"),
                    evalue=c.get("evalue"),
                    traceback=c.get("traceback"),
                )
        except Exception as e:
            self._emit("channel_error", error=f"shell: {e}")

    def _handle_iopub(self, msg: dict, in_flight: dict[str, str]) -> None:
        parent = msg.get("parent_header", {}) or {}
        msg_type = msg.get("msg_type")
        c = _content(msg)

        # status messages are kernel-global, not attributed to a request.
        if msg_type == "status":
            state = c.get("execution_state")
            if state in ("busy", "idle"):
                self._emit("kernel_status", state=state)
            return

        if msg_type not in (
            "stream",
            "execute_input",
            "execute_result",
            "display_data",
            "update_display_data",
            "error",
            "clear_output",
        ):
            return

        exec_id = in_flight.get(parent.get("msg_id"))
        if exec_id is None:
            # Output with no known parent request (e.g. residual after a
            # restart).  Dropped on purpose rather than mis-attributed.
            return

        ev: dict[str, Any] = {"type": "kernel_message", "execution_id": exec_id,
                             "kind": msg_type, "content": c}
        if msg_type == "display_data" or msg_type == "update_display_data":
            ev["display_id"] = c.get("transient", {}).get("display_id")
        if msg_type == "execute_input":
            ev["msg_id"] = parent.get("msg_id")
        self._emit(**ev)
