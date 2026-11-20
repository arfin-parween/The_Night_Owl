"""Structured, OpenSearch-native logging for pipeline runs and simulated services.

Every event goes to (1) the console, (2) logs/events.jsonl, (3) OpenSearch (if up).
If OpenSearch is down the run never breaks; `nightowl ship-logs` can backfill later.
"""
from __future__ import annotations

import json
import sys

from . import os_client, settings

COLORS = {"INFO": "\033[36m", "WARN": "\033[33m", "ERROR": "\033[31m", "OK": "\033[32m"}
RESET = "\033[0m"


def _jsonl_path():
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    return settings.LOG_DIR / "events.jsonl"


def emit(doc: dict, echo: bool = True) -> None:
    doc.setdefault("@timestamp", settings.iso())
    with _jsonl_path().open("a", encoding="utf-8") as f:
        f.write(json.dumps(doc, default=str) + "\n")
    if echo:
        level = doc.get("level", "INFO")
        tty = sys.stderr.isatty()
        color, reset = (COLORS.get(level, ""), RESET) if tty else ("", "")
        stage = doc.get("stage", "")
        print(f"{color}[{level:<5}]{reset} {doc.get('service','')} {stage}: {doc.get('message','')}", file=sys.stderr)
    if os_client.available():
        try:
            os_client.index_doc(settings.IDX_LOGS, doc)
        except Exception as exc:  # never let logging kill the pipeline
            print(f"  (could not ship log to OpenSearch: {exc})", file=sys.stderr)


class RunLogger:
    def __init__(self, pipeline: str, run_id: str, dt: str):
        self.base = {"service": f"spark:{pipeline}", "pipeline": pipeline, "run_id": run_id, "dt": dt}

    def log(self, level: str, stage: str, message: str, **fields) -> None:
        emit({**self.base, "level": level, "stage": stage, "message": message, **fields})

    def info(self, stage, message, **f):
        self.log("INFO", stage, message, **f)

    def warn(self, stage, message, **f):
        self.log("WARN", stage, message, **f)

    def error(self, stage, message, **f):
        self.log("ERROR", stage, message, **f)
