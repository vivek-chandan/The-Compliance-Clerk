from __future__ import annotations

from enum import Enum
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class DocumentType(str, Enum):
    NA_ORDER = "na_order"
    NA_LEASE = "na_lease"
    UNKNOWN = "unknown"


class GroupType(str, Enum):
    NA = "na"
    UNKNOWN = "unknown"


NA_EXPORT_COLUMNS = [
    "Sr.no.",
    "Village ",
    "Survey No.",
    "Block Number",
    "Area in NA Order",
    "Dated",
    "NA Order No.",
    "Lease Deed Doc. No.",
    "Lease Area ",
    "Lease Start ",
]

class IdentityCard(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    file_path: str
    filename: str
    document_type: DocumentType
    group_type: GroupType
    master_key: str
    grouping_basis: str
    survey_number: str = ""
    village: str = ""
    order_number: str = ""
    confidence: float = 0.0
    sample_text: str = ""


class PageText(BaseModel):
    file_path: str
    filename: str
    page_number: int
    text: str
    source: Literal["native", "ocr"] = "native"


class CandidateRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    sr_no: str = Field(default="", alias="sr no")
    document_type: str = Field(default="", alias="Document Type")
    source_files: str = Field(default="", alias="Source Files")
    master_key: str = Field(default="", alias="Master Key")
    owner_name: str = Field(default="", alias="Owner Name")
    authority_details: str = Field(default="", alias="Authority Details")
    village: str = Field(default="", alias="village")
    survey_no: str = Field(default="", alias="survey no")
    block_number: str = Field(default="", alias="Block Number")
    land_area: str = Field(default="", alias="Land Area")
    area_in_na_order: str = Field(default="", alias="Area in NA Order")
    dated: str = Field(default="", alias="Dated")
    na_order_no: str = Field(default="", alias="NA Order No.")
    lease_deed_doc_no: str = Field(default="", alias="Lease Deed Doc. No.")
    lease_area: str = Field(default="", alias="Lease Area")
    lease_start: str = Field(default="", alias="Lease Start")

    def to_output_dict(self) -> Dict[str, str]:
        payload = self.model_dump(by_alias=True)
        return {column: str(payload.get(column, "") or "") for column in [
            "sr no",
            "Document Type",
            "Source Files",
            "Master Key",
            "Owner Name",
            "Authority Details",
            "village",
            "survey no",
            "Block Number",
            "Land Area",
            "Area in NA Order",
            "Dated",
            "NA Order No.",
            "Lease Deed Doc. No.",
            "Lease Area",
            "Lease Start",
        ]}

    def filled_fields(self) -> Dict[str, str]:
        return {key: value for key, value in self.to_output_dict().items() if value and key != "sr no"}

    @classmethod
    def empty(cls, group_type: GroupType, master_key: str, source_files: List[str]) -> "CandidateRecord":
        return cls(
            **{
                "Document Type": group_type.value if isinstance(group_type, GroupType) else str(group_type),
                "Source Files": " | ".join(source_files),
                "Master Key": master_key,
            }
        )


class ProcessingCluster(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    master_key: str
    group_type: GroupType
    identity_cards: List[IdentityCard]


class LLMLogRecord(BaseModel):
    timestamp: str
    prompt: str
    response: str
    metadata: Dict[str, object] = Field(default_factory=dict)


KEY_ALIASES = {
    "sr no": "sr no",
    "sr_no": "sr no",
    "document type": "Document Type",
    "document_type": "Document Type",
    "source files": "Source Files",
    "source_files": "Source Files",
    "master key": "Master Key",
    "master_key": "Master Key",
    "owner name": "Owner Name",
    "owner_name": "Owner Name",
    "authority details": "Authority Details",
    "authority_details": "Authority Details",
    "village": "village",
    "survey no": "survey no",
    "survey number": "survey no",
    "survey_number": "survey no",
    "block number": "Block Number",
    "block_number": "Block Number",
    "land area": "Land Area",
    "land_area": "Land Area",
    "area in na order": "Area in NA Order",
    "area_in_na_order": "Area in NA Order",
    "dated": "Dated",
    "order date": "Dated",
    "na order no.": "NA Order No.",
    "na order no": "NA Order No.",
    "na_order_no": "NA Order No.",
    "lease deed doc. no.": "Lease Deed Doc. No.",
    "lease deed doc no": "Lease Deed Doc. No.",
    "lease_deed_doc_no": "Lease Deed Doc. No.",
    "lease area": "Lease Area",
    "lease_area": "Lease Area",
    "lease start": "Lease Start",
    "lease_start": "Lease Start",
}


def normalize_payload_keys(payload: Dict[str, object]) -> Dict[str, object]:
    normalized: Dict[str, object] = {}
    for key, value in payload.items():
        key_text = str(key).strip().lower()
        mapped = KEY_ALIASES.get(key_text, key)
        normalized[mapped] = value
    return normalized


def to_na_export_row(payload: Dict[str, object]) -> Dict[str, str]:
    return {
        "Sr.no.": str(payload.get("sr no", "") or ""),
        "Village ": str(payload.get("village", "") or ""),
        "Survey No.": str(payload.get("survey no", "") or ""),
        "Block Number": str(payload.get("Block Number", "") or ""),
        "Area in NA Order": str(payload.get("Area in NA Order", "") or ""),
        "Dated": str(payload.get("Dated", "") or ""),
        "NA Order No.": str(payload.get("NA Order No.", "") or ""),
        "Lease Deed Doc. No.": str(payload.get("Lease Deed Doc. No.", "") or ""),
        "Lease Area ": str(payload.get("Lease Area", "") or ""),
        "Lease Start ": str(payload.get("Lease Start", "") or ""),
    }


