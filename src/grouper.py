from __future__ import annotations

import re
from collections import defaultdict
from typing import Iterable, Iterator, List, Optional

from src.parser import extract_native_text_by_page, get_target_pages, ocr_region_only, pdf_name
from src.schema import DocumentType, GroupType, IdentityCard, ProcessingCluster


ORDER_NUMBER_RE = re.compile(r"iORA/\d+/\d+/\d+/\d+/\d+", re.IGNORECASE)
SURVEY_RE = re.compile(
    r"(?:survey|block|s\.?\s*no\.?)\s*[:\-]?\s*([0-9]+(?:[\/\-]?[A-Za-z0-9]+)*)",
    re.IGNORECASE,
)
VILLAGE_RE = re.compile(r"(?:village|moje)\s*[:\-]?\s*([A-Za-z][A-Za-z\s]+)", re.IGNORECASE)
LEASE_DEED_RE = re.compile(r"lease deed", re.IGNORECASE)
FINAL_ORDER_RE = re.compile(r"final order", re.IGNORECASE)


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_survey_number(value: str) -> str:
    cleaned = normalize_spaces(value).replace(" ", "")
    cleaned = cleaned.replace("-p", "/p").replace("-P", "/p")
    cleaned = re.sub(r"(?<=\d)p(?=\d)", "/p", cleaned, flags=re.IGNORECASE)
    return cleaned


def normalize_village(value: str) -> str:
    return normalize_spaces(value).title()


def normalize_key_fragment(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


class IdentityCardBuilder:
    def build(self, pdf_path: str) -> IdentityCard:
        filename = pdf_name(pdf_path)
        sample_text = self._sample_document_text(pdf_path)
        document_type = self._classify_document(filename, sample_text)

        survey_number = self._extract_survey_number(filename, sample_text)
        village = self._extract_village(filename, sample_text)
        order_number = self._extract_order_number(sample_text)

        group_type = self._group_type(document_type)
        master_key, grouping_basis = self._master_key(
            document_type=document_type,
            filename=filename,
            survey_number=survey_number,
            village=village,
            order_number=order_number,
        )

        confidence = 0.3
        if group_type == GroupType.NA and survey_number:
            confidence += 0.3
        if group_type == GroupType.NA and village:
            confidence += 0.2
        if order_number:
            confidence += 0.1

        return IdentityCard(
            file_path=pdf_path,
            filename=filename,
            document_type=document_type,
            group_type=group_type,
            master_key=master_key,
            grouping_basis=grouping_basis,
            survey_number=survey_number,
            village=village,
            order_number=order_number,
            confidence=min(confidence, 0.99),
            sample_text=sample_text[:3000],
        )

    def _sample_document_text(self, pdf_path: str) -> str:
        # Fast classification path: read only the first target page for unknown docs.
        target_pages = get_target_pages(pdf_path, DocumentType.UNKNOWN.value)
        if not target_pages:
            return ""

        page_num = target_pages[0]
        native_text = extract_native_text_by_page(pdf_path, page_num)
        if len(normalize_spaces(native_text)) >= 80:
            return native_text

        # Regional OCR fallback (top title/header) avoids full-page OCR latency.
        ocr_text = ocr_region_only(pdf_path, page_num, region="title")
        return "\n".join(part for part in [native_text, ocr_text] if normalize_spaces(part))

    def _classify_document(self, filename: str, sample_text: str) -> DocumentType:
        lowered_name = filename.lower()
        lowered_text = sample_text.lower()

        if FINAL_ORDER_RE.search(lowered_name) or ORDER_NUMBER_RE.search(sample_text):
            return DocumentType.NA_ORDER
        if LEASE_DEED_RE.search(lowered_name) or LEASE_DEED_RE.search(lowered_text):
            return DocumentType.NA_LEASE
        return DocumentType.UNKNOWN

    def _group_type(self, document_type: DocumentType) -> GroupType:
        if document_type in {DocumentType.NA_ORDER, DocumentType.NA_LEASE}:
            return GroupType.NA
        return GroupType.UNKNOWN

    def _extract_survey_number(self, filename: str, sample_text: str) -> str:
        filename_match = re.search(r"S\.?\s*No\.?\s*[-:]?\s*([0-9]+(?:[\/\-]?[A-Za-z0-9]+)*)", filename, re.IGNORECASE)
        if filename_match:
            return normalize_survey_number(filename_match.group(1))

        order_match = re.match(r"([0-9]+(?:-p\d+)?)\s+FINAL ORDER", filename, re.IGNORECASE)
        if order_match:
            return normalize_survey_number(order_match.group(1))

        text_match = SURVEY_RE.search(sample_text or "")
        if text_match:
            return normalize_survey_number(text_match.group(1))

        return ""

    def _extract_village(self, filename: str, sample_text: str) -> str:
        filename_match = re.match(r"(.+?)\s+S\.?\s*No\.?", filename, re.IGNORECASE)
        if filename_match:
            return normalize_village(filename_match.group(1))

        text_match = VILLAGE_RE.search(sample_text or "")
        if text_match:
            return normalize_village(text_match.group(1))

        return ""

    def _extract_order_number(self, sample_text: str) -> str:
        match = ORDER_NUMBER_RE.search(sample_text or "")
        return match.group(0).upper() if match else ""

    def _master_key(
        self,
        document_type: DocumentType,
        filename: str,
        survey_number: str,
        village: str,
        order_number: str,
    ) -> tuple[str, str]:
        if document_type in {DocumentType.NA_ORDER, DocumentType.NA_LEASE}:
            if survey_number and village:
                return (
                    f"na:{normalize_key_fragment(village)}:{normalize_key_fragment(survey_number)}",
                    "survey_number+village",
                )
            if survey_number:
                return f"na:survey:{normalize_key_fragment(survey_number)}", "survey_number"
            if order_number:
                return f"na:order:{normalize_key_fragment(order_number)}", "order_number"
            return f"na:file:{normalize_key_fragment(filename)}", "filename"

        return f"unknown:file:{normalize_key_fragment(filename)}", "filename"


class EntityGrouper:
    def group(self, identity_cards: Iterable[IdentityCard]) -> List[ProcessingCluster]:
        cards = list(identity_cards)
        survey_to_keys = defaultdict(set)
        for card in cards:
            if card.group_type == GroupType.NA and card.survey_number and card.village:
                survey_to_keys[normalize_key_fragment(card.survey_number)].add(card.master_key)

        grouped = defaultdict(list)
        for card in cards:
            group_key = card.master_key
            if (
                card.group_type == GroupType.NA
                and card.survey_number
                and not card.village
                and card.grouping_basis == "survey_number"
            ):
                matching_keys = sorted(survey_to_keys.get(normalize_key_fragment(card.survey_number), set()))
                if len(matching_keys) == 1:
                    group_key = matching_keys[0]

            grouped[(card.group_type, group_key)].append(card)

        clusters: List[ProcessingCluster] = []
        for (group_type, master_key), cards in grouped.items():
            cards = sorted(cards, key=lambda item: item.filename.lower())
            clusters.append(
                ProcessingCluster(
                    master_key=master_key,
                    group_type=group_type,
                    identity_cards=cards,
                )
            )

        return sorted(clusters, key=lambda cluster: (cluster.group_type, cluster.master_key))

    def group_and_persist(
        self,
        identity_cards: Iterable[IdentityCard],
        storage: Optional[object] = None,
    ) -> Iterator[ProcessingCluster]:
        """
        Group identity cards and persist clusters to disk as they're created.

        This method enables streaming cluster processing without holding all
        clusters in memory simultaneously.

        Args:
            identity_cards: Iterator or list of identity cards
            storage: StorageManager instance for persisting clusters to disk.
                     If provided, clusters are saved incrementally.

        Yields:
            ProcessingCluster objects in sorted order
        """
        from src.storage import StorageManager

        if storage is None:
            storage = StorageManager()

        cards = list(identity_cards)
        survey_to_keys = defaultdict(set)
        for card in cards:
            if card.group_type == GroupType.NA and card.survey_number and card.village:
                survey_to_keys[normalize_key_fragment(card.survey_number)].add(card.master_key)

        grouped = defaultdict(list)
        for card in cards:
            group_key = card.master_key
            if (
                card.group_type == GroupType.NA
                and card.survey_number
                and not card.village
                and card.grouping_basis == "survey_number"
            ):
                matching_keys = sorted(survey_to_keys.get(normalize_key_fragment(card.survey_number), set()))
                if len(matching_keys) == 1:
                    group_key = matching_keys[0]

            grouped[(card.group_type, group_key)].append(card)

        # Create and persist clusters in sorted order
        clusters = []
        for (group_type, master_key), cards_in_group in grouped.items():
            cards_in_group = sorted(cards_in_group, key=lambda item: item.filename.lower())
            cluster = ProcessingCluster(
                master_key=master_key,
                group_type=group_type,
                identity_cards=cards_in_group,
            )
            clusters.append(cluster)

        # Sort and persist to disk
        sorted_clusters = sorted(clusters, key=lambda cluster: (cluster.group_type, cluster.master_key))
        storage.save_clusters(sorted_clusters)

        # Yield clusters one at a time for streaming processing
        for cluster in sorted_clusters:
            yield cluster
