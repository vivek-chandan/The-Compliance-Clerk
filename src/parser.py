from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Sequence

import fitz

from src.ocr import ocr_selected_pages
from src.schema import (
    NA_FIELD_KEYWORDS,
    CandidateRecord,
    GroupType,
    PageText,
    ProcessingCluster,
)


DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b")
DATE_DASH_RE = re.compile(r"\b\d{1,2}-\d{1,2}-\d{4}\b")
ORDER_NUMBER_RE = re.compile(r"iORA/\d+/\d+/\d+/\d+/\d+", re.IGNORECASE)
CHALLAN_NUMBER_RE = re.compile(
    r"(?:challan|notice|application)\s*(?:number|no\.?|#)?\s*[:\-]?\s*([A-Z0-9\-]{6,})",
    re.IGNORECASE,
)
VEHICLE_NUMBER_RE = re.compile(r"\b[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}\b")
SURVEY_RE = re.compile(
    r"(?:survey|block|s\.?\s*no\.?)\s*[:\-]?\s*([0-9]+(?:[\/\-]?[A-Za-z0-9]+)*)",
    re.IGNORECASE,
)
VILLAGE_RE = re.compile(r"(?:village|moje)\s*[:\-]?\s*([A-Za-z][A-Za-z\s]+)", re.IGNORECASE)
AREA_RE = re.compile(
    r"([0-9,]+(?:\.\d+)?)\s*(sq\.?\s*m(?:trs?)?|sq\.?\s*ft|sqm|hectare|acre)",
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
LEASE_PAGE_DATE_RE = re.compile(r"Page\s+\d+\s+of\s+\d+,\s*Date[:\-]?\s*([0-9]{1,2}\s*[-/]\s*[0-9]{1,2}\s*[-/]\s*[0-9]{4})", re.IGNORECASE)
PRINTED_ON_RE = re.compile(r"Printed On.*?(\d{1,2}[/-]\d{1,2}[/-]\d{4})", re.IGNORECASE)
ORDER_AREA_NEAR_SURVEY_RE = re.compile(
    r"(?:survey|block|otot2|s\.?\s*no\.?)[^\n]{0,80}?(\d{1,3}(?:,\d{3})+)(?:\.\d+)?",
    re.IGNORECASE,
)


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
            pages.extend(self._extract_document_pages(card.file_path))
        return pages

    def select_informative_pages(
        self,
        cluster: ProcessingCluster,
        candidate_record: CandidateRecord,
        max_pages: int = 6,
    ) -> List[PageText]:
        pages = self.collect_cluster_pages(cluster)
        missing_fields = self._missing_fields(cluster.group_type, candidate_record)
        if not missing_fields:
            return self._top_scored_pages(cluster, candidate_record, pages, max_pages=3)

        selected: List[PageText] = []
        seen_pages = set()

        for field_name in missing_fields:
            best_page = self._best_page_for_field(cluster, pages, field_name)
            if not best_page:
                continue
            page_key = (best_page.filename, best_page.page_number)
            if page_key in seen_pages:
                continue
            selected.append(best_page)
            seen_pages.add(page_key)
            if len(selected) >= max_pages:
                return selected

        for page in self._top_scored_pages(cluster, candidate_record, pages, max_pages=max_pages * 2):
            page_key = (page.filename, page.page_number)
            if page_key in seen_pages:
                continue
            selected.append(page)
            seen_pages.add(page_key)
            if len(selected) >= max_pages:
                break

        return selected[:max_pages]

    def relevant_fields(self, group_type: GroupType) -> List[str]:
        return list(self._field_keywords(group_type).keys())

    def missing_fields(self, group_type: GroupType, record: CandidateRecord) -> List[str]:
        return self._missing_fields(group_type, record)

    def _top_scored_pages(
        self,
        cluster: ProcessingCluster,
        candidate_record: CandidateRecord,
        pages: List[PageText],
        max_pages: int,
    ) -> List[PageText]:
        missing_fields = self._missing_fields(cluster.group_type, candidate_record)
        confirmed_values = {
            value.lower()
            for key, value in candidate_record.filled_fields().items()
            if key not in {"Document Type", "Source Files", "Master Key"} and value
        }

        scored_pages = []
        for page in pages:
            normalized_text = page.text.lower()
            master_key_score = 0
            for card in cluster.identity_cards:
                tokens = [card.master_key, card.survey_number, card.village, card.order_number]
                for token in tokens:
                    if token and token.lower() in normalized_text:
                        master_key_score += 3

            keyword_score = 0
            for field_name in missing_fields:
                keywords = self._field_keywords(cluster.group_type).get(field_name, [])
                keyword_score += sum(2 for keyword in keywords if keyword.lower() in normalized_text)

            novelty_score = 0
            if not confirmed_values:
                novelty_score += 2
            else:
                for token in self._salient_tokens(page.text):
                    if token.lower() not in confirmed_values:
                        novelty_score += 1

            redundancy_penalty = 0
            if confirmed_values and all(value in normalized_text for value in confirmed_values if len(value) > 4):
                redundancy_penalty = 3

            score = master_key_score + keyword_score + novelty_score - redundancy_penalty
            if score > 0:
                scored_pages.append((score, len(page.text), page))

        scored_pages.sort(key=lambda item: (-item[0], -item[1], item[2].filename, item[2].page_number))
        return [item[2] for item in scored_pages[:max_pages]]

    def _best_page_for_field(
        self,
        cluster: ProcessingCluster,
        pages: List[PageText],
        field_name: str,
    ) -> PageText | None:
        filename_to_type = {
            card.filename: (card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type))
            for card in cluster.identity_cards
        }
        ranked = []
        for page in pages:
            page_text = page.text.lower()
            score = 0
            keywords = self._field_keywords(cluster.group_type).get(field_name, [])
            score += sum(3 for keyword in keywords if keyword.lower() in page_text)

            page_doc_type = filename_to_type.get(page.filename, "")
            if cluster.group_type == GroupType.NA:
                if field_name in {"Area in NA Order", "Dated", "NA Order No.", "Authority Details"} and page_doc_type == "na_order":
                    score += 4
                if field_name in {"Lease Deed Doc. No.", "Lease Area", "Lease Start", "Owner Name"} and page_doc_type == "na_lease":
                    score += 4
            if field_name in {"Area in NA Order", "Lease Area", "Land Area"}:
                if self._extract_primary_sqm(page.text) or self._extract_property_detail_area(page.text):
                    score += 5
            if field_name in {"Dated", "Lease Start"} and DATE_RE.search(page.text):
                score += 5
            if field_name == "NA Order No." and ORDER_NUMBER_RE.search(page.text):
                score += 5
            if field_name == "Lease Deed Doc. No." and LEASE_DEED_FILENAME_RE.search(page.filename):
                score += 5

            if score > 0:
                ranked.append((score, len(page.text), page))

        ranked.sort(key=lambda item: (-item[0], -item[1], item[2].filename, item[2].page_number))
        return ranked[0][2] if ranked else None

    def _extract_document_pages(self, pdf_path: str) -> List[PageText]:
        total_pages = page_count(pdf_path)
        relevant_pages = tuple(self._relevant_page_numbers(total_pages))
        cache_key = (pdf_path, relevant_pages)
        if cache_key in self._page_cache:
            return self._page_cache[cache_key]

        pages: List[PageText] = []
        with fitz.open(pdf_path) as document:
            native_texts: Dict[int, str] = {}
            ocr_candidates = []

            force_ocr = total_pages <= 4
            for page_number in relevant_pages:
                page = document[page_number]
                text = page.get_text().strip()
                native_texts[page_number] = text
                if force_ocr or len(text) < 60:
                    ocr_candidates.append(page_number)

            ocr_texts = ocr_selected_pages(pdf_path, ocr_candidates, zoom=2.0) if ocr_candidates else {}

            for page_number in relevant_pages:
                native_text = native_texts.get(page_number, "")
                ocr_text = normalize_whitespace(ocr_texts.get(page_number, ""))
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
        village_match = VILLAGE_RE.search(lease_text or order_text or text)
        owner_match = OWNER_RE.search(lease_text)
        authority_match = AUTHORITY_RE.search(order_text or text)
        date_match = DATE_RE.search(order_text)
        order_match = ORDER_NUMBER_RE.search(order_text or text)
        lease_start_match = PRINTED_ON_RE.search(lease_text) or LEASE_PAGE_DATE_RE.search(lease_text)

        if first_card.survey_number:
            record.survey_no = first_card.survey_number
        elif survey_match:
            record.survey_no = normalize_survey(survey_match.group(1))

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
        record.lease_area = self._extract_lease_area(lease_text) or record.area_in_na_order
        if lease_start_match:
            record.lease_start = normalize_date_string(lease_start_match.group(1))
        record.lease_deed_doc_no = self._extract_lease_deed_number(cluster, record.lease_start)

        return record

    def _field_keywords(self, group_type: GroupType) -> Dict[str, List[str]]:
        if group_type == GroupType.NA:
            return NA_FIELD_KEYWORDS
        return {}

    def _missing_fields(self, group_type: GroupType, record: CandidateRecord) -> List[str]:
        relevant_fields = list(self._field_keywords(group_type).keys())
        payload = record.to_output_dict()
        return [field for field in relevant_fields if not payload.get(field)]

    def _salient_tokens(self, text: str) -> Sequence[str]:
        tokens: List[str] = []
        for pattern in (ORDER_NUMBER_RE, VEHICLE_NUMBER_RE, DATE_RE, AREA_RE):
            if pattern is AREA_RE:
                tokens.extend([" ".join(match) for match in pattern.findall(text)])
            else:
                for match in pattern.findall(text):
                    tokens.append(match if isinstance(match, str) else match[0])
        return [normalize_whitespace(token) for token in tokens if normalize_whitespace(token)]

    def _relevant_page_numbers(self, total_pages: int) -> List[int]:
        candidate_pages = [0, 1, 2, 3, total_pages - 1]
        return sorted({page for page in candidate_pages if 0 <= page < total_pages})

    def _cluster_text(self, cluster: ProcessingCluster, document_types: set[str]) -> str:
        parts: List[str] = []
        for card in cluster.identity_cards:
            document_type = card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
            if document_type in document_types:
                parts.extend(page.text for page in self._extract_document_pages(card.file_path))
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
        area_match = AREA_RE.search(lease_text)
        return normalize_whitespace(" ".join(area_match.groups())) if area_match else ""

    def _extract_lease_area(self, lease_text: str) -> str:
        direct_sqm = self._extract_primary_sqm(lease_text)
        if direct_sqm:
            return direct_sqm
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

    def _extract_lease_deed_number(self, cluster: ProcessingCluster, lease_start: str) -> str:
        lease_year = ""
        if lease_start:
            lease_year = lease_start.split("/")[-1]

        for card in cluster.identity_cards:
            filename_match = LEASE_DEED_FILENAME_RE.search(card.filename)
            if filename_match:
                deed_number = filename_match.group(1)
                if lease_year:
                    if lease_year >= "2026" and len(deed_number) == 3:
                        deed_number = f"1{deed_number}"
                    return f"{deed_number}/{lease_year}"
                return deed_number
        return ""


def normalize_date_string(value: str) -> str:
    compact = normalize_whitespace(value).replace(" ", "")
    compact = compact.replace("-", "/")
    return compact
