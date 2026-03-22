from __future__ import annotations

from glob import glob
from pathlib import Path

from src.exporter import save_results
from src.grouper import EntityGrouper, IdentityCardBuilder
from src.llm_handler import audit_candidate_record, llm_available, required_api_key_env
from src.parser import HeuristicParser
from src.schema import CandidateRecord, GroupType


RAW_PDF_DIR = Path("data/raw_pdfs")
OUTPUT_DIR = Path("output")
LOG_DIR = Path("logs")
SRC_DIR = Path("src")


def initialize_workspace() -> None:
    for path in (RAW_PDF_DIR, OUTPUT_DIR, LOG_DIR, SRC_DIR):
        path.mkdir(parents=True, exist_ok=True)


def discover_pdfs() -> list[str]:
    pdf_paths = sorted(glob(str(RAW_PDF_DIR / "**" / "*.pdf"), recursive=True))
    return [path for path in pdf_paths if not Path(path).name.startswith(".")]


def assign_serial_numbers(records: list[CandidateRecord]) -> list[CandidateRecord]:
    for index, record in enumerate(records, start=1):
        record.sr_no = str(index)
    return records


def process_cluster(cluster, parser: HeuristicParser) -> CandidateRecord:
    candidate_record = parser.build_candidate_record(cluster)
    relevant_fields = parser.relevant_fields(cluster.group_type)
    missing_fields = parser.missing_fields(cluster.group_type, candidate_record)
    max_pages = max(3, min(6, len(missing_fields) + 1))
    selected_pages = parser.select_informative_pages(cluster, candidate_record, max_pages=max_pages)

    audited_record, _, _ = audit_candidate_record(
        candidate_record=candidate_record,
        pages=selected_pages,
        group_type=cluster.group_type,
        master_key=cluster.master_key,
        relevant_fields=relevant_fields,
        missing_fields=missing_fields,
    )
    audited_record.document_type = cluster.group_type.value if hasattr(cluster.group_type, "value") else str(cluster.group_type)
    audited_record.source_files = candidate_record.source_files
    audited_record.master_key = candidate_record.master_key
    return audited_record


def main() -> None:
    initialize_workspace()
    pdf_paths = discover_pdfs()
    if not pdf_paths:
        print("No PDFs found in data/raw_pdfs.")
        return

    identity_builder = IdentityCardBuilder()
    grouper = EntityGrouper()
    parser = HeuristicParser()

    identity_cards = [identity_builder.build(path) for path in pdf_paths]
    clusters = grouper.group(identity_cards)

    if not llm_available():
        print(f"{required_api_key_env()} is not set or invalid. Running with heuristic extraction only.")

    results: list[CandidateRecord] = []
    skipped_unknown: list[str] = []

    for cluster in clusters:
        if cluster.group_type == GroupType.UNKNOWN:
            skipped_unknown.extend(card.filename for card in cluster.identity_cards)
            continue
        results.append(process_cluster(cluster, parser))

    if skipped_unknown:
        skipped_list = ", ".join(sorted(skipped_unknown))
        print(f"Skipped {len(skipped_unknown)} unsupported PDF(s): {skipped_list}")

    if not results:
        print("No records extracted.")
        return

    assign_serial_numbers(results)
    save_results(results)


if __name__ == "__main__":
    main()
