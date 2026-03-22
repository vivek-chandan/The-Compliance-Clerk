from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional, Tuple

from openai import APIError, AuthenticationError, OpenAI
from pydantic import ValidationError

from src.logger import log_llm, log_schema_error
from src.schema import CandidateRecord, GroupType, PageText, normalize_payload_keys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_MAX_OUTPUT_TOKENS = 2000
DEFAULT_MODELS = {
    "openai": "gpt-4.1-mini",
    "openrouter": "openai/gpt-4.1-mini",
}
PROVIDER_KEY_ENV = {
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
}
SYSTEM_PROMPT = (
    "You are a Land Records Auditor. You will receive a 'Candidate Record' "
    "(partially filled by regex) and 'Merged Context' (snippets). Your job is "
    "to fill missing values and correct errors in the Candidate Record based "
    "ONLY on the provided context. Return ONLY JSON."
)


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
    load_local_env()
    api_key = os.getenv(required_api_key_env(), "")
    return bool(api_key and "your_api_key_here" not in api_key.lower())


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


def _build_user_prompt(
    candidate_record: CandidateRecord,
    pages: list[PageText],
    relevant_fields: list[str],
    missing_fields: list[str],
) -> str:
    snippets = []
    for page in pages:
        snippets.append(f"[{page.filename} | page {page.page_number} | {page.source}]\n{page.text}")

    return json.dumps(
        {
            "candidate_record": candidate_record.to_output_dict(),
            "relevant_fields": relevant_fields,
            "missing_fields": missing_fields,
            "merged_context": "\n\n".join(snippets),
            "instructions": [
                "Verify every non-empty candidate value against the context.",
                "Correct incorrect values.",
                "If a field is missing in the candidate record but clearly present in the snippets, fill it.",
                "Fill only values supported by the snippets.",
                "Leave unsupported fields empty.",
                "Do not copy challan values into NA fields or NA values into challan fields.",
                "Prefer exact values from the snippets even if the candidate record is blank or wrong.",
                "Return JSON with the same keys as the candidate record.",
            ],
        },
        ensure_ascii=True,
        indent=2,
    )


def _parse_record(response_text: str) -> CandidateRecord:
    cleaned = response_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    payload = json.loads(cleaned.strip())
    payload = normalize_payload_keys(payload)
    return CandidateRecord.model_validate(payload)


def audit_candidate_record(
    candidate_record: CandidateRecord,
    pages: list[PageText],
    group_type: GroupType,
    master_key: str,
    relevant_fields: list[str],
    missing_fields: list[str],
) -> Tuple[CandidateRecord, Optional[str], Optional[str]]:
    if not llm_available() or not pages:
        return candidate_record, None, None

    prompt = _build_user_prompt(candidate_record, pages, relevant_fields, missing_fields)
    model_name = os.getenv("LLM_MODEL", DEFAULT_MODELS.get(provider_name(), DEFAULT_MODEL))
    try:
        response = _client().chat.completions.create(
            model=model_name,
            temperature=0,
            max_completion_tokens=int(os.getenv("LLM_MAX_OUTPUT_TOKENS", DEFAULT_MAX_OUTPUT_TOKENS)),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
    except (AuthenticationError, APIError) as error:
        log_schema_error(
            master_key=master_key,
            group_type=group_type.value if hasattr(group_type, "value") else str(group_type),
            error_message=f"LLM request failed: {error}",
            raw_response="",
        )
        return candidate_record, prompt, None

    log_llm(
        prompt=prompt,
        response=content,
        metadata={
            "group_type": group_type.value if hasattr(group_type, "value") else str(group_type),
            "master_key": master_key,
            "page_count": len(pages),
        },
    )

    try:
        return _parse_record(content), prompt, content
    except (json.JSONDecodeError, ValidationError) as first_error:
        retry_prompt = (
            f"{prompt}\n\nPrevious response failed schema validation. "
            "Return ONLY valid JSON with the same keys and string values."
        )
        retry_response = _client().chat.completions.create(
            model=model_name,
            temperature=0,
            max_completion_tokens=int(os.getenv("LLM_MAX_OUTPUT_TOKENS", DEFAULT_MAX_OUTPUT_TOKENS)),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": retry_prompt},
            ],
        )
        retry_content = retry_response.choices[0].message.content or "{}"
        log_llm(
            prompt=retry_prompt,
            response=retry_content,
            metadata={
                "group_type": group_type.value if hasattr(group_type, "value") else str(group_type),
                "master_key": master_key,
                "retry": True,
            },
        )
        try:
            return _parse_record(retry_content), retry_prompt, retry_content
        except (json.JSONDecodeError, ValidationError) as second_error:
            log_schema_error(
                master_key=master_key,
                group_type=group_type.value if hasattr(group_type, "value") else str(group_type),
                error_message=str(second_error),
                raw_response=retry_content,
            )
            return candidate_record, prompt, content
