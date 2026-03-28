from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List

import fitz

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
OWNER_RE = re.compile(
    r"(?:owner|occupant|applicant|lessee)\s*(?:name)?\s*[:\-]?\s*([A-Za-z][A-Za-z\s\.]{2,80})",
    re.IGNORECASE,
)
AUTHORITY_RE = re.compile(
    r"((?:district\s+collector|collector|competent authority|deputy collector)[A-Za-z,\s\-]{0,120})",
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
ANNEXURE_AREA_IN_SQM_RE = re.compile(
    r"Description\s+of\s+Subject\s+Lands.*?Owner\S*\s+Name.*?\|\s*(0?\d{4,}(?:\.\d+)?)\b",
    re.IGNORECASE | re.DOTALL,
)
LEASE_DEED_DOC_WITH_YEAR_RE = re.compile(r"\b(\d{1,4})\s*/\s*(\d{4})\b")
LEASE_DEED_DOC_STAMP_RE = re.compile(r"\b(\d{1,4})\s*/\s*\d{1,3}\s*/\s*\d{1,3}\b")


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
    region_ratio = {
        "title": 0.15,
        "header": 0.25,
        "full": 1.0,
    }
    ratio = region_ratio.get((region or "").strip().lower(), 0.25)

    with fitz.open(pdf_path) as document:
        if page_num < 0 or page_num >= document.page_count:
            return ""

        page = document[page_num]
        rect = page.rect
        clip = fitz.Rect(0, 0, rect.width, rect.height * ratio)
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=clip, alpha=False)

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = Path(temp_dir) / f"page_{page_num + 1}_{region}.png"
            pix.save(image_path)
            result = subprocess.run(
                ["tesseract", str(image_path), "stdout"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.stdout.strip()


def find_annexure_page(pdf_path: str, start_page: int = 15, region_scan: float = 0.25) -> int | None:
    """Detect an Annexure-I style page by scanning the top region of later pages."""
    with fitz.open(pdf_path) as document:
        total_pages = document.page_count
        if total_pages <= 0:
            return None

        begin = max(0, min(start_page, total_pages - 1))
        scan_ratio = max(0.05, min(region_scan, 1.0))
        for page_num in range(begin, total_pages):
            page = document[page_num]
            rect = page.rect
            clip = fitz.Rect(0, 0, rect.width, rect.height * scan_ratio)
            header_text = normalize_whitespace(page.get_text(clip=clip)).lower()
            if ANNEXURE_RE.search(header_text):
                return page_num
            ocr_title = normalize_whitespace(ocr_region_only(pdf_path, page_num, region="title")).lower()
            if ANNEXURE_RE.search(ocr_title):
                return page_num
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


def clean_person_name(value: str) -> str:
    candidate = normalize_whitespace(value)
    blacklist = ("hereinafter", "collectively referred", "lessor", "lessee", "agreement")
    if not candidate or any(term in candidate.lower() for term in blacklist):
        return ""
    if len(candidate.split()) < 2:
        return ""
    return candidate.title()


class HeuristicParser:
    def __init__(self) -> None:
        self._page_cache: Dict[tuple[str, tuple[int, ...]], List[PageText]] = {}

    def build_candidate_record(self, cluster: ProcessingCluster) -> CandidateRecord:
        source_files = [card.filename for card in cluster.identity_cards]
        record = CandidateRecord.empty(cluster.group_type, cluster.master_key, source_files)
        pages = self.collect_cluster_pages(cluster)
        combined_text = "\n".join(page.text for page in pages)

        if cluster.group_type == GroupType.NA:
            return self._fill_na_record(record, cluster, combined_text)
        return record

    def collect_cluster_pages(self, cluster: ProcessingCluster) -> List[PageText]:
        pages: List[PageText] = []
        for card in cluster.identity_cards:
            document_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
            pages.extend(self._extract_document_pages(card.file_path, document_type=document_type))
        return pages

    def _extract_document_pages(self, pdf_path: str, document_type: str = "unknown") -> List[PageText]:
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
        owner_match = OWNER_RE.search(lease_text)
        authority_match = AUTHORITY_RE.search(order_text or text)
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

        if owner_match:
            record.owner_name = clean_person_name(owner_match.group(1))
        if authority_match:
            record.authority_details = normalize_whitespace(authority_match.group(1))
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
                parts.extend(page.text for page in self._extract_document_pages(card.file_path, document_type=document_type))
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
