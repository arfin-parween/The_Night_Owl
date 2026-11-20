"""Central settings: paths, index names, env loading, small state file."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("NIGHTOWL_HOME", Path(__file__).resolve().parents[1]))


def _load_dotenv() -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()

OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
DASHBOARDS_URL = os.environ.get("DASHBOARDS_URL", "http://localhost:5601")
OS_DISABLED = os.environ.get("NIGHTOWL_OS_DISABLED") == "1"

OPENCLAW_HOOK_URL = os.environ.get("OPENCLAW_HOOK_URL", "http://127.0.0.1:18789/hooks/agent")
OPENCLAW_HOOK_TOKEN = os.environ.get("OPENCLAW_HOOK_TOKEN", "")
OPENCLAW_AGENT_ID = os.environ.get("OPENCLAW_AGENT_ID", "main")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
ORDERS_DIR = RAW_DIR / "orders"
STORES_FILE = RAW_DIR / "stores" / "stores.csv"
OUTPUT_DIR = ROOT / "output"
LOG_DIR = ROOT / "logs"
CONFIG_FILE = ROOT / "config" / "pipeline.yaml"
DEFAULT_CONFIG_FILE = ROOT / "config" / "pipeline.default.yaml"
STATE_FILE = ROOT / ".nightowl_state.json"

PIPELINE = "daily_city_sales"

IDX_LOGS = "nightowl-logs"
IDX_RUNS = "nightowl-runs"
IDX_CHANGES = "nightowl-changes"
IDX_INCIDENTS = "nightowl-incidents"
IDX_LINEAGE = "nightowl-lineage"
IDX_AUDIT = "nightowl-audit"
ALL_INDICES = [IDX_LOGS, IDX_RUNS, IDX_CHANGES, IDX_INCIDENTS, IDX_LINEAGE, IDX_AUDIT]

EMBED_PIPELINE = "nightowl-embed"
HYBRID_PIPELINE = "nightowl-hybrid"
EMBED_MODEL = "huggingface/sentence-transformers/all-MiniLM-L6-v2"
# 1.0.1, not 1.0.2: the 1.0.2 TorchScript artifact is traced with PyTorch 2.x and fails to deploy on
# OpenSearch 2.19's bundled PyTorch 1.13.1 ("Unknown builtin op: aten::scaled_dot_product_attention").
EMBED_MODEL_VERSION = "1.0.1"
# SHA-256 of the TorchScript zip per version, used to reuse an already-registered copy on re-runs.
EMBED_MODEL_HASHES = {"1.0.1": "c15f0d2e62d872be5b5bc6c84d2e0f4921541e29fefbef51d59cc10a8ae30e0f"}
EMBED_DIM = 384


def now() -> datetime:
    return datetime.now(timezone.utc)


def today() -> str:
    """Business date in IST (the pipeline's dt partition)."""
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()


def iso(dt: datetime | None = None) -> str:
    return (dt or now()).isoformat(timespec="seconds")


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def save_state(**updates) -> dict:
    state = load_state()
    state.update(updates)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return state
