"""全局配置：端口、租约时间、保留范围等。"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "frontend_dist"
DB_PATH = DATA_DIR / "lab.db"

HOST = "127.0.0.1"
PORT = 8765

# ---- 执行控制权（租约锁）----
LEASE_SECONDS = 10.0        # 持锁后超过该时长没有心跳即失效
HEARTBEAT_SECONDS = 3.0     # 建议前端心跳间隔
DISCONNECT_GRACE = 3.0      # 持锁连接断开后的宽限（留给刷新/短暂断线）

# ---- 消息保留范围 ----
# 日志里最多保留多少条内核消息；超出后最旧的消息被淘汰。
MAX_KERNEL_MESSAGES = 3000
# 每次垃圾回收额外保留的结构事件（cell/lock/kernel 等）下限
MIN_STRUCTURAL_EVENTS = 200
