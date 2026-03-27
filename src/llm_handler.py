from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from openai import OpenAI


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_MODELS = {
    "openai": "gpt-4.1-mini",
    "openrouter": "openai/gpt-4.1-mini",
}
PROVIDER_KEY_ENV = {
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
}
_RUNTIME_LLM_DISABLED = False
_RUNTIME_LLM_DISABLE_REASON = ""


def _context_mode(context: str) -> str:
    lowered = (context or "").strip().lower()
    if "vision" in lowered:
        return "vision"
    if "text" in lowered or "audit" in lowered:
        return "text"
    return "unknown"


def _log_runtime_disable_event(reason: str, context: str) -> None:
    """Persist a one-time runtime event when LLM is disabled for this run."""
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    event_path = log_dir / "llm_runtime_events.jsonl"

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "llm_disabled_for_run",
        "reason": reason,
        "context": context,
        "llm_mode": _context_mode(context),
        "provider": provider_name(),
    }

    with open(event_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def load_local_env(env_path: Path = PROJECT_ROOT / ".env") -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def llm_available() -> bool:
    if _RUNTIME_LLM_DISABLED:
        return False

    load_local_env()
    api_key = os.getenv(required_api_key_env(), "")
    return bool(api_key and "your_api_key_here" not in api_key.lower())


def register_llm_error(error: Exception, context: str = "") -> None:
    """Disable further LLM calls for this run after quota/credit failures."""
    global _RUNTIME_LLM_DISABLED, _RUNTIME_LLM_DISABLE_REASON

    message = str(error).lower()
    quota_like = (
        "error code: 402" in message
        or "insufficient_quota" in message
        or "more credits" in message
        or "quota" in message
        or "rate limit" in message
    )

    if quota_like and not _RUNTIME_LLM_DISABLED:
        _RUNTIME_LLM_DISABLED = True
        _RUNTIME_LLM_DISABLE_REASON = str(error)
        _log_runtime_disable_event(_RUNTIME_LLM_DISABLE_REASON, context)
        scope = f" ({context})" if context else ""
        print(f"Disabling LLM for this run after provider quota/credit error{scope}.")


def _client() -> OpenAI:
    load_local_env()
    provider = provider_name()
    api_key = os.getenv(required_api_key_env())
    if provider == "openrouter":
        return OpenAI(
            api_key=api_key,
            base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        )
    return OpenAI(api_key=api_key)


def provider_name() -> str:
    load_local_env()
    return os.getenv("LLM_PROVIDER", "openai").strip().lower()


def required_api_key_env() -> str:
    return PROVIDER_KEY_ENV.get(provider_name(), "OPENAI_API_KEY")
