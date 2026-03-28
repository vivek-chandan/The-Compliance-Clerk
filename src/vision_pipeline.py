from __future__ import annotations

import base64
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

from openai import APIError, AuthenticationError
from PIL import Image

from src.llm_handler import _client, llm_available, provider_name, register_llm_error
from src.logger import log_llm, log_schema_error
from src.parser import get_target_pages
from src.perf import log_timing, render_page_image, vision_json_cache
from src.schema import CandidateRecord, DocumentType, ProcessingCluster, normalize_payload_keys


NA_ORDER_FIRST_PAGE_PROMPT = """
You are extracting structured land information from the FIRST PAGE of a Gujarat NA Order document.

This is an NA Order first page. The important information is located in fixed positions.

Extract the following fields if visible on this page:
- na_order_no
- order_date
- district
- taluka
- village
- survey_number
- block_number
- land_area

WHERE TO FIND INFORMATION
na_order_no
Located at the TOP of the page near:
"Hukam No." or "Order No."

order_date
Located near the top section of the page.
Format: DD/MM/YYYY

district, taluka, village, survey_number, block_number, land_area
These appear together in a paragraph describing the land.
Look for a sentence containing:
District ___ Taluka ___ Village ___ Survey/Block No ___ Area ___ sq.m.

Extract values from that paragraph.

EXTRACTION RULES
Extract only values visible on this page
Do not guess values
Remove commas from numbers
PRESERVE LEADING ZEROS in all numeric values (e.g., "04047" not "4047")
Land area should be numeric only but preserve leading zeros
Return STRICT JSON only with ALL values as strings
If value not present, return empty string

OUTPUT FORMAT - ALL VALUES MUST BE STRINGS:
{
    "na_order_no": "",
    "order_date": "",
    "survey_number": "",
    "block_number": "",
    "village": "",
    "taluka": "",
    "district": "",
    "land_area": ""
}
""".strip()

NA_LEASE_ANNEXURE_PROMPT = """
You are extracting land parcel information from an ANNEXURE-I page of a Gujarat NA Lease document.

Annexure-I usually contains "Description of Subject Land".

From this page extract:
- lease_deed_no
- lease_date
- survey_number
- village
- taluka
- district
- lease_area (CRITICAL: This is the PRIMARY field)

WHERE TO FIND INFORMATION

lease_deed_no
Look in the TOP RIGHT CORNER inside the rectangular stamp/box.
Examples of formats inside that box:
- 141 / 35 / 54 with year 2026 nearby
- 141/2026
Extract only the Lease Deed document number and year.
Return it in this final format: "141/2026"
If the box shows multiple slash-separated values like "141 / 35 / 54", use only the FIRST number as the document number.
If the year near the box and the footer date year do not match, use the footer date year.

lease_date
Look at the BOTTOM CENTER or BOTTOM RIGHT footer.
Example:
- Page 33 of 41, Date: 21-01-2026
Extract that Date value as lease_date.
Return date in DD/MM/YYYY format.

survey_number, village, taluka, district
Look in the first table under "Description of Subject Lands".

lease_area
Look in the FIRST TABLE only.
The first table columns are like:
No | District | Taluka | Village | Owner | R.S.No Old | New | Area in SQM
Extract the value from the LAST COLUMN: "Area in SQM".
Example:
- 04047

Area may be written as these not strikly fixed labels, but always look for a numeric value followed by "sq.m", "sq meter", "hectare" or similar units. Examples:
- Area : 16792
- Area Sq. Meter : 1092
- Area (Sq.m.) : 0604
- Land Area : 16810
- Total Area : 34976
- Plot Area : 11000
- Area Hectare : 1.4792
- Area in SQM : 04047

CRITICAL FOR AREA EXTRACTION - lease_area FIELD:
The Annexure-I page contains an area measurement that MUST be extracted to "lease_area".
Always extract the value from the first table's "Area in SQM" column into "lease_area".
Extract numeric value only (no units).
Do NOT take boundary numbers, page numbers, or other survey numbers as lease_area.

If area is in square meters or hectare, return the number only (no units).

EXTRACTION RULES
Extract only values visible on this page
Do not guess values
Remove commas from numbers
PRESERVE LEADING ZEROS in all numeric values (e.g., "04047" not "4047", "0604" not "604")
Return STRICT JSON only with ALL values as strings (not numbers)
If value not present, return empty string

OUTPUT FORMAT - ALL VALUES MUST BE STRINGS:
{
    "lease_deed_no": "",
    "lease_date": "",
    "survey_number": "",
    "village": "",
    "taluka": "",
    "district": "",
    "lease_area": "04047"
}

CRITICAL: If the area value is "04047", you MUST return "lease_area": "04047" as a STRING, NOT as a number 4047.
""".strip()

NUMERIC_PRIORITY_FIELDS = {
    "survey no",
    "Block Number",
    "NA Order No.",
    "Area in NA Order",
    "Land Area",
}

TEXT_PRIORITY_FIELDS = {
    "village",
}


def select_vision_pages(cluster: ProcessingCluster, identity_cards: Iterable[object]) -> List[tuple[str, str, int, str]]:
    """Select fixed target pages for vision processing and skip unknown documents."""
    pages_to_process: List[tuple[str, str, int, str]] = []
    for card in identity_cards:
        doc_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
        if doc_type == DocumentType.UNKNOWN.value:
            continue

        target_pages = get_target_pages(card.file_path, doc_type)
        if target_pages:
            pages_to_process.append((card.file_path, card.filename, target_pages[0], doc_type))

    return pages_to_process


def pdf_pages_to_images(
    pdf_path: str,
    page_numbers: Iterable[int],
    output_dir: Path,
    prefix: str,
    zoom: float = 2.0,
) -> List[Path]:
    """Render selected PDF pages to cropped PNG files for vision extraction."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rendered_paths: List[Path] = []
    for page_number in page_numbers:
        image_path = output_dir / f"{prefix}_page_{page_number + 1}.png"
        render_page_to_png(pdf_path, page_number, image_path)
        rendered_paths.append(image_path)

    return rendered_paths


def render_and_crop_page(pdf_path: str, page_num: int, crop_margins: bool = True) -> Image.Image:
    """Render a page at 150 DPI and crop surrounding white margins to reduce payload."""
    return render_page_image(pdf_path, page_num, crop_margins=crop_margins)


def render_page_to_png(pdf_path: str, page_num: int, output_path: Path) -> Path:
    """Render and save a single page as optimized PNG."""
    image = render_and_crop_page(pdf_path, page_num, crop_margins=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, "PNG", optimize=True)
    return output_path


def extract_vision_record_for_cluster(cluster: ProcessingCluster) -> Dict[str, str]:
    """Extract JSON fields from targeted page images and fold page outputs into one dict."""
    merged: Dict[str, str] = {}
    cluster_dir = Path("intermediate") / "vision_pages" / _sanitize_path_fragment(cluster.master_key)
    selected_pages = select_vision_pages(cluster, cluster.identity_cards)

    for file_path, filename, page_num, doc_type in selected_pages:
        prompt, expected_keys = _prompt_and_keys_for_doc_type(doc_type)
        page_number = page_num + 1
        image_dir = cluster_dir / _sanitize_path_fragment(filename)
        image_path = image_dir / f"{Path(filename).stem}_page_{page_number}.png"
        cache_key = (file_path, page_num)

        page_payload = vision_json_cache.get(cache_key, {})
        if not page_payload:
            page_payload = _load_saved_page_payload(cluster.master_key, filename, page_number)
            if page_payload:
                page_payload = _normalize_vision_payload(page_payload, expected_keys)
                vision_json_cache[cache_key] = page_payload
        if not page_payload:
            page_payload = _load_logged_page_payload(cluster.master_key, image_path.name, page_number)
            if page_payload:
                page_payload = _normalize_vision_payload(page_payload, expected_keys)
                vision_json_cache[cache_key] = page_payload

        if not page_payload and llm_available():
            image_paths = pdf_pages_to_images(
                file_path,
                [page_num],
                image_dir,
                prefix=Path(filename).stem,
            )
            page_payload = _extract_page_json(
                image_path=image_paths[0],
                master_key=cluster.master_key,
                doc_type=doc_type,
                page_number=page_number,
                prompt=prompt,
                expected_keys=expected_keys,
            )
            if not any(str(value or "").strip() for value in page_payload.values()):
                page_payload = (
                    _load_saved_page_payload(cluster.master_key, filename, page_number)
                    or _load_logged_page_payload(cluster.master_key, image_path.name, page_number)
                    or page_payload
                )
                if page_payload:
                    page_payload = _normalize_vision_payload(page_payload, expected_keys)
            if page_payload:
                vision_json_cache[cache_key] = page_payload

        if page_payload:
            _save_page_payload(cluster.master_key, filename, page_number, page_payload)
            for key, value in page_payload.items():
                value = str(value or "").strip()
                if not value:
                    continue
                existing = str(merged.get(key, "")).strip()
                if not existing or len(value) > len(existing):
                    merged[key] = value

    return merged


def merge_regex_llm(regex_record: CandidateRecord, llm_record: Dict[str, str]) -> CandidateRecord:
    """Merge regex extraction with vision extraction using field-aware priority rules."""
    if not llm_record:
        return regex_record

    regex_payload = regex_record.to_output_dict()
    mapped_llm = _map_vision_to_candidate_fields(llm_record)
    merged: Dict[str, str] = {}

    all_fields = set(regex_payload.keys()) | set(mapped_llm.keys())
    for field in all_fields:
        regex_value = str(regex_payload.get(field, "") or "").strip()
        llm_value = str(mapped_llm.get(field, "") or "").strip()
        merged[field] = _choose_field_value(field, regex_value, llm_value)

    merged["Document Type"] = regex_payload.get("Document Type", "")
    merged["Source Files"] = regex_payload.get("Source Files", "")
    merged["Master Key"] = regex_payload.get("Master Key", "")
    merged["sr no"] = regex_payload.get("sr no", "")

    normalized = normalize_payload_keys(merged)
    return CandidateRecord.model_validate(normalized)


def _extract_page_json(
    image_path: Path,
    master_key: str,
    doc_type: str,
    page_number: int,
    prompt: str,
    expected_keys: List[str],
) -> Dict[str, str]:
    if not llm_available():
        return {}

    model_name = os.getenv("VISION_LLM_MODEL", os.getenv("LLM_MODEL", "gpt-4.1-mini"))
    image_data = base64.b64encode(image_path.read_bytes()).decode("ascii")
    user_text = (
        f"Document type: {doc_type}. Page number: {page_number}. "
        "Extract only values present on this page image. "
        "CRITICAL: Return ALL numeric values as STRINGS to preserve leading zeros."
    )

    started = time.perf_counter()
    try:
        response = _client().chat.completions.create(
            model=model_name,
            temperature=0,
            max_completion_tokens=int(os.getenv("VISION_MAX_OUTPUT_TOKENS", "700")),
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_data}"}},
                    ],
                },
            ],
        )
        content = response.choices[0].message.content or "{}"
    except (AuthenticationError, APIError, ValueError) as error:
        register_llm_error(error, context="vision")
        log_schema_error(
            master_key=master_key,
            group_type=doc_type,
            error_message=f"Vision LLM request failed: {error}",
            raw_response="",
            log_path="logs/vision_schema_errors.jsonl",
        )
        return {}
    finally:
        log_timing("vision_extraction", time.perf_counter() - started, f"{master_key} page {page_number}")

    log_llm(
        prompt=f"VISION: {image_path.name}",
        response=content,
        metadata={
            "master_key": master_key,
            "page_number": page_number,
            "provider": provider_name(),
            "vision": True,
        },
        log_path="logs/vision_llm_logs.jsonl",
    )

    try:
        payload = json.loads(content.strip())
    except json.JSONDecodeError:
        return {}

    if not isinstance(payload, dict):
        return {}
    return _normalize_vision_payload(payload, expected_keys)


def _map_vision_to_candidate_fields(llm_record: Dict[str, str]) -> Dict[str, str]:
    field_map = {
        "na_order_no": "NA Order No.",
        "order_date": "Dated",
        "survey_number": "survey no",
        "block_number": "Block Number",
        "village": "village",
        "lease_deed_no": "Lease Deed Doc. No.",
        "lease_date": "Lease Start",
        "lease_area": "Lease Area",
    }
    mapped = {}
    for key, value in llm_record.items():
        if key.startswith("_"):  # Skip internal markers
            continue
        target = field_map.get(key)
        if not target:
            continue
        mapped[target] = str(value or "").strip()

    # Lease Area must come only from lease extraction (Annexure-I -> lease_area).
    lease_land_area = str(llm_record.get("lease_area", "") or "").strip()
    if lease_land_area:
        mapped["Lease Area"] = lease_land_area
        mapped.setdefault("Land Area", lease_land_area)

    # Handle order area separately; never map it into Lease Area.
    order_land_area = str(llm_record.get("land_area", "") or "").strip()
    if order_land_area:
        mapped.setdefault("Area in NA Order", order_land_area)
        if not lease_land_area:
            mapped.setdefault("Land Area", order_land_area)

    return mapped


def _prompt_and_keys_for_doc_type(doc_type: str) -> tuple[str, List[str]]:
    lowered = (doc_type or "").strip().lower()
    if lowered == DocumentType.NA_ORDER.value:
        return (
            NA_ORDER_FIRST_PAGE_PROMPT,
            [
                "na_order_no",
                "order_date",
                "survey_number",
                "block_number",
                "village",
                "taluka",
                "district",
                "land_area",
            ],
        )
    if lowered == DocumentType.NA_LEASE.value:
        return (
            NA_LEASE_ANNEXURE_PROMPT,
            [
                "lease_deed_no",
                "lease_date",
                "survey_number",
                "village",
                "taluka",
                "district",
                "lease_area",
            ],
        )

    return NA_ORDER_FIRST_PAGE_PROMPT, [
        "na_order_no",
        "order_date",
        "survey_number",
        "block_number",
        "village",
        "taluka",
        "district",
        "land_area",
    ]


def _normalize_vision_payload(payload: Dict[str, object], expected_keys: List[str]) -> Dict[str, str]:
    """
    Normalize vision payload while preserving leading zeros in numeric strings.
    
    CRITICAL: This function must preserve leading zeros in numeric values.
    Example: "04047" must remain "04047", not become "4047"
    """
    normalized: Dict[str, str] = {key: "" for key in expected_keys}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if key not in normalized:
            continue
        
        # Special handling for numeric fields to preserve leading zeros
        if key in {"land_area", "lease_area", "block_number"}:
            # If the LLM returned a number (e.g., 4047), we've already lost the leading zero
            # If it returned a string (e.g., "04047"), preserve it exactly
            if isinstance(raw_value, str):
                # It's already a string - preserve leading zeros
                value = _numeric_only(raw_value)
            else:
                # It's a number - we can't recover lost leading zeros, but convert to string
                value = _numeric_only(str(raw_value or ""))
        else:
            value = str(raw_value or "").strip()
        
        normalized[key] = value
    return normalized


def _numeric_only(value: str) -> str:
    """
    Extract numeric value while preserving leading zeros.
    Removes commas and extracts digits, but maintains leading zeros.
    
    Examples:
    - "04047" -> "04047" (preserves leading zero)
    - "16,792" -> "16792" (removes comma)
    - "0604 sq.m" -> "0604" (removes units, preserves leading zero)
    """
    cleaned = str(value or "").replace(",", "")
    matches = re.findall(r"\d+(?:\.\d+)?", cleaned)
    return "".join(matches)


def _choose_field_value(field: str, regex_value: str, llm_value: str) -> str:
    if regex_value and llm_value and _normalize(regex_value) == _normalize(llm_value):
        return regex_value
    if not regex_value:
        return llm_value
    if not llm_value:
        return regex_value

    if field in NUMERIC_PRIORITY_FIELDS:
        return regex_value

    if field == "Lease Deed Doc. No.":
        return llm_value if llm_value else regex_value

    if field in {"Dated", "Lease Start"}:
        regex_date = _parse_date(regex_value)
        llm_date = _parse_date(llm_value)
        if regex_date and not llm_date:
            return regex_value
        if llm_date and not regex_date:
            return llm_value
        if regex_date and llm_date:
            if field == "Lease Start":
                return llm_value
            return regex_value

    # Annexure-derived lease fields should prioritize the vision extraction.
    if field in {"Lease Area", "Lease Start"}:
        return llm_value if llm_value else regex_value

    if field in TEXT_PRIORITY_FIELDS:
        return llm_value if llm_value else regex_value

    return llm_value if len(llm_value) > len(regex_value) else regex_value


def _parse_date(value: str) -> datetime | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None

    for date_format in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(candidate, date_format)
        except ValueError:
            continue
    return None


def _normalize(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def _sanitize_path_fragment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(value or "")).strip("_") or "unknown"


def _save_page_payload(master_key: str, filename: str, page_number: int, payload: Dict[str, str]) -> None:
    output_dir = Path("intermediate") / "vision_json" / _sanitize_path_fragment(master_key)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_sanitize_path_fragment(filename)}_page_{page_number}.json"
    if not any(str(value or "").strip() for value in payload.values()):
        existing = _load_saved_page_payload(master_key, filename, page_number)
        if existing:
            return
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_saved_page_payload(master_key: str, filename: str, page_number: int) -> Dict[str, str]:
    path = (
        Path("intermediate")
        / "vision_json"
        / _sanitize_path_fragment(master_key)
        / f"{_sanitize_path_fragment(filename)}_page_{page_number}.json"
    )
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized = {str(key): str(value or "").strip() for key, value in payload.items()}
    if any(normalized.values()):
        return normalized
    return {}


def _load_logged_page_payload(master_key: str, image_name: str, page_number: int) -> Dict[str, str]:
    log_path = Path("logs") / "vision_llm_logs.jsonl"
    if not log_path.exists():
        return {}

    best_match: Dict[str, str] = {}
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}

    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        metadata = record.get("metadata", {}) if isinstance(record, dict) else {}
        if str(metadata.get("master_key", "")).strip() != str(master_key).strip():
            continue
        if int(metadata.get("page_number", 0) or 0) != int(page_number):
            continue
        if str(record.get("prompt", "")).strip() != f"VISION: {image_name}":
            continue

        raw_response = str(record.get("response", "")).strip()
        if not raw_response:
            continue
        try:
            payload = json.loads(raw_response)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        normalized = {str(key): str(value or "").strip() for key, value in payload.items()}
        if any(normalized.values()):
            best_match = normalized
            break

    return best_match
