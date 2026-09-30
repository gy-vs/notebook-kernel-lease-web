"""Configuration for the local notebook app."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("NB_DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "notebook.db"
CONN_INFO_PATH = DATA_DIR / "connection.json"  # leftover connection file is cleaned on shutdown

# How many messages are retained per execution.  Older messages are pruned,
# and the execution is flagged so the UI can show a "truncated" notice.
MESSAGES_PER_EXEC_LIMIT = int(os.environ.get("NB_MESSAGES_PER_EXEC", "1000"))
# How many executions are kept per cell.
EXECS_PER_CELL_LIMIT = int(os.environ.get("NB_EXECS_PER_CELL", "30"))
# How many characters of source are stored on the execution row for the
# submission-query response / debug.
SOURCE_PREVIEW_LEN = 2000

# Leader lease: after a leader tab's websocket drops, its session keeps the
# lock for this long (it may just be reloading the page).
LEADER_GRACE_SECONDS = float(os.environ.get("NB_GRACE_SECONDS", "6"))
# Application-level websocket keepalive.
PING_INTERVAL_SECONDS = 10.0
PING_TIMEOUT_SECONDS = 20.0

HOST = os.environ.get("NB_HOST", "127.0.0.1")
PORT = int(os.environ.get("NB_PORT", "8765"))
