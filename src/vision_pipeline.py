from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import fitz
from PIL import Image, ImageOps
from openai import APIError, AuthenticationError

from src.llm_handler import _client, llm_available, provider_name, register_llm_error
from src.logger import log_llm, log_schema_error
from src.parser import find_annexure_page, get_target_pages
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
- authority_details

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

authority_details
Located at the bottom of the page above signature.
Contains officer name and designation like:
Deputy Collector / District Collector / Prant Officer / Mamlatdar

EXTRACTION RULES
Extract only values visible on this page
Do not guess values
Remove commas from numbers
Land area should be numeric only
Return STRICT JSON only
If value not present, return empty string

OUTPUT FORMAT
{
    "na_order_no": "",
    "order_date": "",
    "survey_number": "",
    "block_number": "",
    "village": "",
    "taluka": "",
    "district": "",
    "land_area": "",
    "authority_details": ""
}
""".strip()

NA_LEASE_ANNEXURE_PROMPT = """
You are extracting land parcel information from an ANNEXURE-I page of a Gujarat NA Lease document.

Annexure-I usually contains "Description of Subject Land".

From this page extract:
- survey_number
- village
- taluka
- district
- land_area
- owner_name

WHERE TO FIND INFORMATION
On Annexure-I page look for fields like:
Survey No.
Old Survey No.
Village
Taluka
District
Area (Sq Meter / Hectare)
Owner Name

Area may be written as:
Area Sq. Meter
Area Hectare
Land Area

If area is in square meters, return the number only (no units).

EXTRACTION RULES
Extract only values visible on this page
Do not guess values
Remove commas from numbers
Return STRICT JSON only
If value not present, return empty string

OUTPUT FORMAT
{
    "survey_number": "",
    "village": "",
    "taluka": "",
    "district": "",
    "land_area": "",
    "owner_name": ""
}
""".strip()

NUMERIC_PRIORITY_FIELDS = {
    "survey no",
    "Block Number",
    "NA Order No.",
    "Area in NA Order",
    "Land Area",
    "Lease Deed Doc. No.",
}

TEXT_PRIORITY_FIELDS = {
    "village",
    "Owner Name",
    "Authority Details",
}


def select_vision_pages(cluster: ProcessingCluster, identity_cards: Iterable[object]) -> List[tuple[str, str, int, str]]:
    """Select fixed target pages for vision processing and skip unknown documents."""
    pages_to_process: List[tuple[str, str, int, str]] = []
    for card in identity_cards:
        doc_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
        if doc_type == DocumentType.UNKNOWN.value:
            continue

        if doc_type == DocumentType.NA_ORDER.value:
            pages_to_process.append((card.file_path, card.filename, 0, doc_type))
            continue

        if doc_type == DocumentType.NA_LEASE.value:
            annexure_page = find_annexure_page(card.file_path)
            target_page = annexure_page if annexure_page is not None else 0
            pages_to_process.append((card.file_path, card.filename, target_page, doc_type))
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
    with fitz.open(pdf_path) as document:
        if page_num < 0 or page_num >= document.page_count:
            raise ValueError(f"Invalid page number {page_num} for {pdf_path}")

        page = document[page_num]
        matrix = fitz.Matrix(150 / 72, 150 / 72)
        pix = page.get_pixmap(matrix=matrix, alpha=False)
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    if not crop_margins:
        return image

    grayscale = image.convert("L")
    inverted = ImageOps.invert(grayscale)
    content_box = inverted.getbbox()
    if content_box:
        image = image.crop(content_box)
    return image


def render_page_to_png(pdf_path: str, page_num: int, output_path: Path) -> Path:
    """Render and save a single page as optimized PNG."""
    image = render_and_crop_page(pdf_path, page_num, crop_margins=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, "PNG", optimize=True)
    return output_path


def extract_vision_record_for_cluster(cluster: ProcessingCluster) -> Dict[str, str]:
    """Extract JSON fields from targeted page images and fold page outputs into one dict."""
    # Early quota check: skip expensive rendering if provider is already unavailable.
    if not llm_available():
        return {}

    merged: Dict[str, str] = {}
    lease_land_area: str = ""  # Track land_area specifically from lease pages
    cluster_dir = Path("intermediate") / "vision_pages" / _sanitize_path_fragment(cluster.master_key)
    selected_pages = select_vision_pages(cluster, cluster.identity_cards)

    for file_path, filename, page_num, doc_type in selected_pages:
        if not llm_available():
            break

        image_dir = cluster_dir / _sanitize_path_fragment(filename)
        image_paths = pdf_pages_to_images(
            file_path,
            [page_num],
            image_dir,
            prefix=Path(filename).stem,
        )

        for image_path, page_number in zip(image_paths, [page_num]):
            if not llm_available():
                break
            prompt, expected_keys = _prompt_and_keys_for_doc_type(doc_type)
            page_payload = _extract_page_json(
                image_path=image_path,
                master_key=cluster.master_key,
                doc_type=doc_type,
                page_number=page_number + 1,
                prompt=prompt,
                expected_keys=expected_keys,
            )
            _save_page_payload(cluster.master_key, filename, page_number + 1, page_payload)
            
            # Track land_area from lease pages separately for correct Lease Area mapping
            if doc_type == DocumentType.NA_LEASE.value and "land_area" in page_payload:
                lease_land_area = str(page_payload.get("land_area", "")).strip()
            
            for key, value in page_payload.items():
                value = str(value or "").strip()
                if not value:
                    continue
                existing = str(merged.get(key, "")).strip()
                if not existing or len(value) > len(existing):
                    merged[key] = value

        if not llm_available():
            break

    # Tag lease land_area for priority in merge
    if lease_land_area:
        merged["_lease_land_area"] = lease_land_area

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
        "Extract only values present on this page image."
    )

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
        "owner_name": "Owner Name",
        "authority_details": "Authority Details",
    }
    mapped = {}
    for key, value in llm_record.items():
        if key.startswith("_"):  # Skip internal markers
            continue
        target = field_map.get(key)
        if not target:
            continue
        mapped[target] = str(value or "").strip()

    # Prioritize lease land_area if it was explicitly extracted from Annexure-I
    lease_land_area = str(llm_record.get("_lease_land_area", "") or "").strip()
    if lease_land_area:
        mapped["Lease Area"] = lease_land_area
        # Also set other area fields if not already set by order extraction
        mapped.setdefault("Land Area", lease_land_area)
    else:
        # Fallback: use generic land_area from any vision page for all area fields
        area_value = str(llm_record.get("land_area", "") or llm_record.get("area_hectare", "") or "").strip()
        if area_value:
            mapped.setdefault("Area in NA Order", area_value)
            mapped.setdefault("Land Area", area_value)
            mapped.setdefault("Lease Area", area_value)

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
                "authority_details",
            ],
        )
    if lowered == DocumentType.NA_LEASE.value:
        return (
            NA_LEASE_ANNEXURE_PROMPT,
            [
                "survey_number",
                "village",
                "taluka",
                "district",
                "land_area",
                "owner_name",
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
        "authority_details",
    ]


def _normalize_vision_payload(payload: Dict[str, object], expected_keys: List[str]) -> Dict[str, str]:
    normalized: Dict[str, str] = {key: "" for key in expected_keys}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if key not in normalized:
            continue
        value = str(raw_value or "").strip()
        if key in {"land_area", "block_number"}:
            value = _numeric_only(value)
        normalized[key] = value
    return normalized


def _numeric_only(value: str) -> str:
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

    if field in {"Dated", "Lease Start"}:
        regex_date = _parse_date(regex_value)
        llm_date = _parse_date(llm_value)
        if regex_date and not llm_date:
            return regex_value
        if llm_date and not regex_date:
            return llm_value
        if regex_date and llm_date:
            return regex_value

    # For Lease Area, prioritize vision extraction from lease pages
    if field == "Lease Area":
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
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")