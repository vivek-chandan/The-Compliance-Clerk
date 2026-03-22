from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _append_jsonl(payload: dict, log_path: str) -> None:
    target = Path(log_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def log_llm(prompt: str, response: str, metadata: dict | None = None, log_path: str = "logs/llm_logs.jsonl") -> None:
    _append_jsonl(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "prompt": prompt,
            "response": response,
            "metadata": metadata or {},
        },
        log_path,
    )


def log_schema_error(
    master_key: str,
    group_type: str,
    error_message: str,
    raw_response: str,
    log_path: str = "logs/schema_errors.jsonl",
) -> None:
    _append_jsonl(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "master_key": master_key,
            "group_type": group_type,
            "error": error_message,
            "raw_response": raw_response,
        },
        log_path,
    )
