from __future__ import annotations

from glob import glob
from pathlib import Path

from src.exporter import save_results
from src.grouper import EntityGrouper, IdentityCardBuilder
from src.llm_handler import llm_available, required_api_key_env
from src.parser import HeuristicParser
from src.schema import CandidateRecord
from src.storage import StorageManager
from src.streaming_processor import StreamingClusterProcessor


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


def assign_serial_numbers(records: list[CandidateRecord]) -> None:
    """Assign serial numbers to records in place."""
    for index, record in enumerate(records, start=1):
        record.sr_no = str(index)


def main() -> None:
    """
    Main processing pipeline with streaming architecture.

    Phases:
    1. Discover PDFs and build identity cards (persisted to disk)
    2. Group identity cards into clusters (persisted to disk)
    3. Process clusters one at a time (streaming from disk)
    4. Export final results to CSV/Excel (from disk-persisted JSON)
    """
    initialize_workspace()
    pdf_paths = discover_pdfs()

    if not pdf_paths:
        print("No PDFs found in data/raw_pdfs.")
        return

    # Initialize components
    storage = StorageManager()
    storage.clear_state()  # Start fresh

    identity_builder = IdentityCardBuilder()
    grouper = EntityGrouper()
    parser = HeuristicParser()
    processor = StreamingClusterProcessor(parser)

    # ==================== Phase 1: Build Identity Cards ====================
    print(f"Discovering {len(pdf_paths)} PDF(s)...")
    identity_cards = [identity_builder.build(path) for path in pdf_paths]

    # Persist identity cards to disk
    storage.save_identity_cards(identity_cards)
    print(f"Persisted {len(identity_cards)} identity card(s) to disk")

    # ==================== Phase 2: Group Documents ====================
    print("Grouping documents...")
    clusters = list(grouper.group_and_persist(identity_cards, storage))
    print(f"Created {len(clusters)} cluster(s)")

    # ==================== Phase 3: Process Clusters (Streaming) ====================
    if not llm_available():
        print(f"Note: {required_api_key_env()} is not set. Running with regex-only extraction (vision disabled).")

    print("Processing clusters...")
    # Process clusters from disk one at a time (constant memory usage)
    cluster_iterator = storage.load_clusters()
    results = list(processor.process_clusters_streaming(cluster_iterator, storage, show_progress=True))

    if not results:
        print("No records extracted.")
        return

    # ==================== Phase 4: Assign Serial Numbers & Export ====================
    print("Assigning serial numbers and exporting...")
    assign_serial_numbers(results)

    # Load all results from disk and export
    all_results = storage.load_all_results()
    save_results(all_results)

    # Print summary
    state = storage.get_state_summary()
    print(f"\nProcessing complete!")
    print(f"  Identity cards: {state['identity_cards_count']}")
    print(f"  Clusters: {state['clusters_count']}")
    print(f"  Final results: {state['results_count']}")
    print(f"  Intermediate storage: {state['storage_dir']}")


if __name__ == "__main__":
    main()
