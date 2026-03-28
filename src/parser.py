from __future__ import annotations

import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List

import fitz
from PIL import ImageOps

from src.perf import annexure_page_cache, log_timing, ocr_text_cache, render_page_image, vision_json_cache
from src.schema import (
    CandidateRecord,
    GroupType,
    PageText,
    ProcessingCluster,
)


DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b")
ORDER_NUMBER_RE = re.compile(r"iORA/\d+/\d+/\d+/\d+/\d+", re.IGNORECASE)
SURVEY_RE = re.compile(
    r"(?:survey|block|s\.?\s*no\.?)\s*[:\-]?\s*([0-9]+(?:[\/\-]?[A-Za-z0-9]+)*)",
    re.IGNORECASE,
)
BLOCK_RE = re.compile(
    r"(?:block(?:\s*no\.?)?|survey\s*/\s*block\s*no\.?)\s*[:\-]?\s*([0-9]+(?:[\/\-]?[A-Za-z0-9]+)*)",
    re.IGNORECASE,
)
VILLAGE_RE = re.compile(r"(?:village|moje)\s*[:\-]?\s*([A-Za-z][A-Za-z\s]+)", re.IGNORECASE)
AREA_RE = re.compile(
    r"([0-9,]+(?:\.\d+)?)\s*(sq\.?\s*m(?:trs?)?|sq\.?\s*ft|sqm|hectare|acre)",
    re.IGNORECASE,
)
AREA_AFTER_LABEL_RE = re.compile(
    r"(?:total\s*area|land\s*area|plot\s*area|area\s*(?:\(?\s*sq\.?\s*m(?:eter)?s?\s*\)?)?|area\s*hectare)\s*[:\-]?\s*([0-9,]+(?:\.\d+)?)",
    re.IGNORECASE,
)
LEASE_DEED_FILENAME_RE = re.compile(r"Lease Deed No\.[^\d]*(\d+)", re.IGNORECASE)
DIRECT_SQM_RE = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?\s*square\s*meters?", re.IGNORECASE)
OUT_OF_SQM_RE = re.compile(r"out of\s*0?(\d{4,})\s*square\s*meter", re.IGNORECASE)
PROPERTY_DETAIL_AREA_RE = re.compile(r"0-(\d{2})-(\d{2})")
LEASE_PAGE_DATE_RE = re.compile(
    r"Page\s+\d+\s+(?:of|0f)\s+\d+,\s*Date[:\-]?\s*([0-9]{1,2}\s*[-/]\s*[0-9]{1,2}\s*[-/]\s*[0-9]{4})",
    re.IGNORECASE,
)
LEASE_PAGE_YEAR_RE = re.compile(
    r"Page\s+\d+\s+(?:of|0f)\s+\d+,\s*Date[:\-]?[^\n]{0,20}?([12]\d{3})",
    re.IGNORECASE,
)
PRINTED_ON_RE = re.compile(r"Printed On.*?(\d{1,2}[/-]\d{1,2}[/-]\d{4})", re.IGNORECASE)
ORDER_AREA_NEAR_SURVEY_RE = re.compile(
    r"(?:survey|block|otot2|s\.?\s*no\.?)[^\n]{0,80}?(\d{1,3}(?:,\d{3})+)(?:\.\d+)?",
    re.IGNORECASE,
)
ANNEXURE_RE = re.compile(r"annexure\s*[-:]?\s*(?:1|i)\b", re.IGNORECASE)
ANNEXURE_PAGE_RE = re.compile(
    r"(annexure\s*[-:]?\s*(?:1|i)\b|description\s+of\s+subject\s+lands)",
    re.IGNORECASE,
)
ANNEXURE_AREA_IN_SQM_RE = re.compile(
    r"Description\s+of\s+Subject\s+Lands.*?(?:Area\s+in\s+SQM\s*\|\s*|Area\s+in\s+SQM\s*[:\-]?\s*)(0?\d{4,}(?:\.\d+)?)\b",
    re.IGNORECASE | re.DOTALL,
)
LEASE_DEED_DOC_WITH_YEAR_RE = re.compile(r"\b(\d{1,4})\s*/\s*(\d{4})\b")
LEASE_DEED_DOC_STAMP_RE = re.compile(r"\b(\d{1,4})\s*/\s*\d{1,3}\s*/\s*\d{1,3}\b")
LEASE_VISION_KEYS = {"lease_deed_no", "lease_date", "survey_number", "village", "taluka", "district", "lease_area", "land_area"}


def pdf_name(pdf_path: str) -> str:
    return Path(pdf_path).name


def extract_text(pdf_path: str, max_pages: int | None = None) -> str:
    text_parts: List[str] = []
    with fitz.open(pdf_path) as document:
        for index, page in enumerate(document):
            if max_pages is not None and index >= max_pages:
                break
            text_parts.append(page.get_text())
    return "\n".join(text_parts)


def page_count(pdf_path: str) -> int:
    with fitz.open(pdf_path) as document:
        return document.page_count


def extract_text_by_page(pdf_path: str, page_number: int) -> str:
    with fitz.open(pdf_path) as document:
        if 0 <= page_number < document.page_count:
            return document[page_number].get_text()
    return ""


def extract_native_text_by_page(pdf_path: str, page_number: int) -> str:
    """Extract native text from a single page (zero-based)."""
    return extract_text_by_page(pdf_path, page_number).strip()


def ocr_region_only(pdf_path: str, page_num: int, region: str = "header", zoom: float = 2.0) -> str:
    """
    OCR a page region for speed.

    Regions:
    - title: top 15%
    - header: top 25%
    - full: entire page
    """
    region_name = (region or "").strip().lower() or "header"
    cache_key = (pdf_path, page_num, region_name)
    if cache_key in ocr_text_cache:
        return ocr_text_cache[cache_key]

    region_ratio = {
        "title": 0.15,
        "header": 0.25,
        "full": 1.0,
    }
    ratio = region_ratio.get(region_name, 0.25)

    with fitz.open(pdf_path) as document:
        if page_num < 0 or page_num >= document.page_count:
            ocr_text_cache[cache_key] = ""
            return ""

    started = time.perf_counter()
    # Reuse the same cropped render for full-page OCR, region OCR, and vision extraction.
    image = render_page_image(pdf_path, page_num, crop_margins=True)
    if ratio < 1.0:
        width, height = image.size
        image = image.crop((0, 0, width, max(1, int(height * ratio))))

    with tempfile.TemporaryDirectory() as temp_dir:
        image_path = Path(temp_dir) / f"page_{page_num + 1}_{region_name}.png"
        if ratio < 1.0:
            grayscale = ImageOps.grayscale(image)
            image = grayscale.point(lambda value: 255 if value > 180 else 0, mode="1")
        image.save(image_path, "PNG", optimize=True)
        command = ["tesseract", str(image_path), "stdout"]
        if ratio < 1.0:
            command.extend(["--psm", "6"])
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        text = result.stdout.strip()

    ocr_text_cache[cache_key] = text
    log_timing("ocr", time.perf_counter() - started, f"{Path(pdf_path).name} page {page_num + 1} region={region_name}")
    return text


def find_annexure_page(pdf_path: str, start_page: int | None = None, region_scan: float = 0.15) -> int | None:
    """Detect an Annexure-I style page by scanning only the tail pages once."""
    if pdf_path in annexure_page_cache:
        return annexure_page_cache[pdf_path]

    started = time.perf_counter()
    with fitz.open(pdf_path) as document:
        total_pages = document.page_count
        if total_pages <= 0:
            annexure_page_cache[pdf_path] = None
            return None

        inferred_start = max(0, total_pages - 25)
        begin = inferred_start if start_page is None else max(0, min(start_page, total_pages - 1))
        scan_ratio = max(0.05, min(region_scan, 1.0))
        for page_num in range(begin, total_pages):
            page = document[page_num]
            rect = page.rect
            clip = fitz.Rect(0, 0, rect.width, rect.height * scan_ratio)
            header_text = normalize_whitespace(page.get_text(clip=clip)).lower()
            if ANNEXURE_PAGE_RE.search(header_text):
                annexure_page_cache[pdf_path] = page_num
                log_timing("find_annexure_page", time.perf_counter() - started, f"{Path(pdf_path).name} -> page {page_num + 1}")
                return page_num
            ocr_title = normalize_whitespace(ocr_region_only(pdf_path, page_num, region="title")).lower()
            if ANNEXURE_PAGE_RE.search(ocr_title):
                annexure_page_cache[pdf_path] = page_num
                log_timing("find_annexure_page", time.perf_counter() - started, f"{Path(pdf_path).name} -> page {page_num + 1}")
                return page_num
    annexure_page_cache[pdf_path] = None
    log_timing("find_annexure_page", time.perf_counter() - started, f"{Path(pdf_path).name} -> not found")
    return None


def get_target_pages(pdf_path: str, doc_type: str) -> List[int]:
    """Return minimal pages per document type to reduce processing overhead."""
    with fitz.open(pdf_path) as document:
        if document.page_count <= 0:
            return []

    lowered = (doc_type or "").strip().lower()
    if lowered == "na_order":
        return [0]
    if lowered == "na_lease":
        annexure_page = find_annexure_page(pdf_path)
        return [annexure_page] if annexure_page is not None else [0]
    return [0]


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_survey(value: str) -> str:
    normalized = normalize_whitespace(value).replace(" ", "")
    normalized = normalized.replace("-p", "/p").replace("-P", "/p")
    normalized = re.sub(r"(?<=\d)p(?=\d)", "/p", normalized, flags=re.IGNORECASE)
    return normalized


def _sanitize_path_fragment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(value or "")).strip("_") or "unknown"


def _expected_image_name(filename: str, page_number: int) -> str:
    return f"{Path(filename).stem}_page_{page_number}.png"


def _load_saved_vision_page_payload(master_key: str, filename: str, page_number: int) -> Dict[str, str]:
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
    return normalized if any(normalized.values()) else {}


def _load_logged_vision_page_payload(master_key: str, filename: str, page_number: int) -> Dict[str, str]:
    log_path = Path("logs") / "vision_llm_logs.jsonl"
    if not log_path.exists():
        return {}

    expected_prompt = f"VISION: {_expected_image_name(filename, page_number)}"
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
        if str(record.get("prompt", "")).strip() != expected_prompt:
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
            return normalized
    return {}


def load_cached_vision_page_payload(pdf_path: str, master_key: str, filename: str, page_number: int) -> Dict[str, str]:
    cache_key = (pdf_path, page_number - 1)
    if cache_key in vision_json_cache:
        return vision_json_cache[cache_key]

    payload = _load_saved_vision_page_payload(master_key, filename, page_number) or _load_logged_vision_page_payload(
        master_key, filename, page_number
    )
    if payload:
        payload = {key: value for key, value in payload.items() if key in LEASE_VISION_KEYS}
        vision_json_cache[cache_key] = payload
    return payload


def _vision_payload_to_text(payload: Dict[str, str], page_number: int) -> str:
    survey_number = str(payload.get("survey_number", "") or "").strip()
    district = str(payload.get("district", "") or "").strip()
    taluka = str(payload.get("taluka", "") or "").strip()
    village = str(payload.get("village", "") or "").strip()
    lease_area = str(payload.get("lease_area", "") or payload.get("land_area", "") or "").strip()
    lease_deed_no = str(payload.get("lease_deed_no", "") or "").strip()
    lease_date = normalize_date_string(str(payload.get("lease_date", "") or "").strip())

    parts = [
        "Annexure-I",
        "Description of Subject Lands",
        "No | District | Taluka | Village | R.S.No Old | New | Area in SQM",
        f"1 | {district} | {taluka} | {village} | {survey_number} | {survey_number} | {lease_area}",
    ]
    if lease_deed_no:
        parts.append(f"Lease Deed No.: {lease_deed_no}")
    if lease_date:
        parts.append(f"Page {page_number} of {page_number}, Date: {lease_date}")
    return "\n".join(part for part in parts if part.strip())


class HeuristicParser:
    def __init__(self) -> None:
        self._page_cache: Dict[tuple[str, tuple[int, ...]], List[PageText]] = {}

    def build_candidate_record(self, cluster: ProcessingCluster) -> CandidateRecord:
        started = time.perf_counter()
        source_files = [card.filename for card in cluster.identity_cards]
        record = CandidateRecord.empty(cluster.group_type, cluster.master_key, source_files)
        pages = self.collect_cluster_pages(cluster)
        combined_text = "\n".join(page.text for page in pages)

        if cluster.group_type == GroupType.NA:
            result = self._fill_na_record(record, cluster, combined_text)
            log_timing("parsing", time.perf_counter() - started, cluster.master_key)
            return result
        log_timing("parsing", time.perf_counter() - started, cluster.master_key)
        return record

    def collect_cluster_pages(self, cluster: ProcessingCluster) -> List[PageText]:
        pages: List[PageText] = []
        for card in cluster.identity_cards:
            document_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
            pages.extend(
                self._extract_document_pages(
                    card.file_path,
                    document_type=document_type,
                    master_key=cluster.master_key,
                    filename=card.filename,
                )
            )
        return pages

    def _extract_document_pages(
        self,
        pdf_path: str,
        document_type: str = "unknown",
        master_key: str = "",
        filename: str = "",
    ) -> List[PageText]:
        # Cache at (file + doc_type + page targets) to avoid repeated OCR work.
        relevant_pages = tuple(get_target_pages(pdf_path, document_type))
        cache_key = (f"{pdf_path}:{document_type}", relevant_pages)
        if cache_key in self._page_cache:
            return self._page_cache[cache_key]

        pages: List[PageText] = []
        for page_number in relevant_pages:
            native_text = extract_native_text_by_page(pdf_path, page_number)
            if len(native_text) > 50:
                text = native_text
                source = "native"
            else:
                if (document_type or "").lower() == "na_lease" and master_key:
                    cached_payload = load_cached_vision_page_payload(pdf_path, master_key, filename or pdf_name(pdf_path), page_number + 1)
                    if cached_payload:
                        text = _vision_payload_to_text(cached_payload, page_number + 1)
                        source = "ocr"
                        pages.append(
                            PageText(
                                file_path=pdf_path,
                                filename=pdf_name(pdf_path),
                                page_number=page_number + 1,
                                text=text,
                                source=source,
                            )
                        )
                        continue
                # Lease annexure extraction needs table/footer content, so OCR the full page when native text is absent.
                region = "full" if (document_type or "").lower() == "na_lease" else "header" if (document_type or "").lower() == "na_order" else "title"
                ocr_text = normalize_whitespace(ocr_region_only(pdf_path, page_number, region=region))
                text = ocr_text or native_text
                source = "ocr" if ocr_text else "native"

            pages.append(
                PageText(
                    file_path=pdf_path,
                    filename=pdf_name(pdf_path),
                    page_number=page_number + 1,
                    text=text,
                    source=source,
                )
            )

        self._page_cache[cache_key] = pages
        return pages

    def _fill_na_record(self, record: CandidateRecord, cluster: ProcessingCluster, text: str) -> CandidateRecord:
        first_card = cluster.identity_cards[0]
        order_text = self._cluster_text(cluster, {"na_order"})
        lease_text = self._cluster_text(cluster, {"na_lease"})
        survey_match = SURVEY_RE.search(order_text or text)
        block_match = BLOCK_RE.search(order_text or text)
        village_match = VILLAGE_RE.search(lease_text or order_text or text)
        date_match = DATE_RE.search(order_text)
        order_match = ORDER_NUMBER_RE.search(order_text or text)
        lease_start_value = self._extract_lease_start(lease_text)

        if first_card.survey_number:
            record.survey_no = first_card.survey_number
        elif survey_match:
            record.survey_no = normalize_survey(survey_match.group(1))

        if block_match:
            record.block_number = normalize_survey(block_match.group(1))

        if first_card.village:
            record.village = first_card.village
        elif village_match:
            record.village = normalize_whitespace(village_match.group(1)).title()

        if date_match:
            record.dated = date_match.group(0)
        if order_match:
            record.na_order_no = order_match.group(0).upper()
        elif first_card.order_number:
            record.na_order_no = first_card.order_number
        record.area_in_na_order = self._extract_na_order_area(order_text, lease_text)
        record.land_area = self._extract_land_area(lease_text) or record.area_in_na_order
        record.lease_area = self._extract_lease_area(lease_text)
        record.lease_start = lease_start_value
        record.lease_deed_doc_no = self._extract_lease_deed_number(cluster, lease_text, record.lease_start)

        return record

    def _cluster_text(self, cluster: ProcessingCluster, document_types: set[str]) -> str:
        parts: List[str] = []
        for card in cluster.identity_cards:
            document_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
            if document_type in document_types:
                parts.extend(
                    page.text
                    for page in self._extract_document_pages(
                        card.file_path,
                        document_type=document_type,
                        master_key=cluster.master_key,
                        filename=card.filename,
                    )
                )
        return "\n".join(parts)

    def _extract_na_order_area(self, order_text: str, lease_text: str) -> str:
        direct = self._extract_primary_sqm(order_text)
        if direct:
            return direct
        order_numeric = self._extract_order_numeric_area(order_text)
        if order_numeric:
            return order_numeric
        property_area = self._extract_property_detail_area(lease_text)
        return property_area

    def _extract_land_area(self, lease_text: str) -> str:
        direct_sqm = self._extract_primary_sqm(lease_text)
        if direct_sqm:
            return direct_sqm
        labeled_area = AREA_AFTER_LABEL_RE.search(lease_text or "")
        if labeled_area:
            return labeled_area.group(1).replace(",", "").strip()
        area_match = AREA_RE.search(lease_text)
        return normalize_whitespace(" ".join(area_match.groups())) if area_match else ""

    def _extract_lease_area(self, lease_text: str) -> str:
        annexure_area = self._extract_annexure_area_in_sqm(lease_text)
        if annexure_area:
            return annexure_area
        direct_sqm = self._extract_primary_sqm(lease_text)
        if direct_sqm:
            return direct_sqm
        labeled_area = AREA_AFTER_LABEL_RE.search(lease_text or "")
        if labeled_area:
            return labeled_area.group(1).replace(",", "").strip()
        property_area = self._extract_property_detail_area(lease_text)
        if property_area:
            return property_area
        area_match = AREA_RE.search(lease_text)
        return normalize_whitespace(" ".join(area_match.groups())) if area_match else ""

    def _extract_primary_sqm(self, text: str) -> str:
        matches: List[int] = []
        for pattern in (OUT_OF_SQM_RE, DIRECT_SQM_RE):
            for match in pattern.finditer(text or ""):
                value = match.group(1).replace(",", "")
                if value.isdigit():
                    matches.append(int(value))
        return str(max(matches)) if matches else ""

    def _extract_annexure_area_in_sqm(self, text: str) -> str:
        match = ANNEXURE_AREA_IN_SQM_RE.search(normalize_whitespace(text or ""))
        if match:
            return match.group(1).replace(",", "").strip()
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        for index, line in enumerate(lines):
            if "area in sqm" not in line.lower():
                continue
            for candidate_line in lines[index + 1:index + 4]:
                cells = [cell.strip() for cell in candidate_line.split("|") if cell.strip()]
                if not cells:
                    continue
                last_cell = cells[-1].replace(",", "").strip()
                if re.fullmatch(r"0?\d{4,}(?:\.\d+)?", last_cell):
                    return last_cell
        return ""

    def _extract_order_numeric_area(self, text: str) -> str:
        near_survey = ORDER_AREA_NEAR_SURVEY_RE.search(text or "")
        if near_survey:
            return str(int(near_survey.group(1).replace(",", "")))

        matches: List[int] = []
        for raw in re.findall(r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b", text or ""):
            candidate = int(raw.split(".", 1)[0].replace(",", ""))
            if 1000 <= candidate <= 50000:
                matches.append(candidate)
        return str(max(matches)) if matches else ""

    def _extract_property_detail_area(self, text: str) -> str:
        matches: List[int] = []
        for first, second in PROPERTY_DETAIL_AREA_RE.findall(text or ""):
            candidate = int(first) * 100 + int(second)
            if candidate >= 1000:
                matches.append(candidate)
        return str(max(matches)) if matches else ""

    def _extract_lease_start(self, lease_text: str) -> str:
        lease_start_match = PRINTED_ON_RE.search(lease_text) or LEASE_PAGE_DATE_RE.search(lease_text)
        if lease_start_match:
            return normalize_date_string(lease_start_match.group(1))
        return ""

    def _extract_lease_deed_number(self, cluster: ProcessingCluster, lease_text: str, lease_start: str) -> str:
        lease_year = lease_start.split("/")[-1] if lease_start else ""
        if not lease_year:
            footer_year_match = LEASE_PAGE_YEAR_RE.search(lease_text or "")
            if footer_year_match:
                lease_year = footer_year_match.group(1)

        doc_match = LEASE_DEED_DOC_WITH_YEAR_RE.search(lease_text or "")
        if doc_match:
            deed_number = doc_match.group(1)
            detected_year = doc_match.group(2)
            final_year = lease_year or detected_year
            return f"{deed_number}/{final_year}" if final_year else deed_number

        stamp_match = LEASE_DEED_DOC_STAMP_RE.search(lease_text or "")
        if stamp_match:
            deed_number = stamp_match.group(1)
            return f"{deed_number}/{lease_year}" if lease_year else deed_number

        for card in cluster.identity_cards:
            filename_match = LEASE_DEED_FILENAME_RE.search(card.filename)
            if filename_match:
                deed_number = filename_match.group(1)
                if lease_year:
                    return f"{deed_number}/{lease_year}"
                return deed_number
        return ""


def normalize_date_string(value: str) -> str:
    compact = normalize_whitespace(value).replace(" ", "")
    compact = compact.replace("-", "/")
    return compact
