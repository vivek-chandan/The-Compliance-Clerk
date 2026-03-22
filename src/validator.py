from __future__ import annotations

import json

from src.schema import CandidateRecord, normalize_payload_keys


def clean_json(output: str) -> str:
    if not output:
        raise ValueError("Empty JSON payload")

    cleaned = output.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    return cleaned.strip()


def validate_output(output: str) -> CandidateRecord:
    payload = json.loads(clean_json(output))
    return CandidateRecord.model_validate(normalize_payload_keys(payload))


def validate_record(payload: dict) -> CandidateRecord:
    return CandidateRecord.model_validate(normalize_payload_keys(payload))
