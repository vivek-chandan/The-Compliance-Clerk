"""
Streaming cluster processor for memory-efficient document processing.

Processes clusters one at a time from disk, returning results as they complete
without accumulating all data in memory.
"""

from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator, List

from src.parser import HeuristicParser
from src.schema import CandidateRecord, GroupType, ProcessingCluster
from src.storage import StorageManager
from src.vision_pipeline import extract_vision_record_for_cluster, merge_regex_llm


def prioritize_clusters(clusters: List[ProcessingCluster]) -> List[ProcessingCluster]:
    """Prioritize clusters for faster early throughput: order -> lease -> unknown."""

    def priority(cluster: ProcessingCluster) -> int:
        doc_types = {
            card.document_type.value if hasattr(card.document_type, "value") else str(card.document_type)
            for card in cluster.identity_cards
        }
        if "na_order" in doc_types:
            return 1
        if "na_lease" in doc_types:
            return 2
        return 3

    return sorted(clusters, key=priority)


class StreamingClusterProcessor:
    """Process clusters efficiently without holding all data in memory."""

    def __init__(self, parser: HeuristicParser | None = None, max_workers: int | None = None):
        self.parser = parser or HeuristicParser()
        configured_workers = int(os.getenv("CLUSTER_MAX_WORKERS", str(max_workers or 4)))
        self.max_workers = max(1, min(configured_workers, multiprocessing.cpu_count()))

    def process_cluster(self, cluster: ProcessingCluster) -> CandidateRecord | None:
        """
        Process a single cluster and return the resulting record.

        Args:
            cluster: The cluster to process

        Returns:
            CandidateRecord if successfully processed, None if skipped (e.g., UNKNOWN type)
        """
        # Skip unknown document types
        if cluster.group_type == GroupType.UNKNOWN:
            return None

        # Build initial candidate record from heuristics
        candidate_record = self.parser.build_candidate_record(cluster)

        # Apply vision extraction and merge (no text LLM audit stage).
        candidate_record = self._apply_vision_merge(cluster, candidate_record)

        # Set document type and metadata
        candidate_record.document_type = (
            cluster.group_type.value if hasattr(cluster.group_type, "value") else str(cluster.group_type)
        )
        candidate_record.source_files = self._format_source_files(cluster)
        candidate_record.master_key = cluster.master_key

        return candidate_record

    def process_clusters_streaming(
        self,
        cluster_iterator: Iterator[ProcessingCluster],
        storage: StorageManager,
        show_progress: bool = True,
    ) -> Iterator[CandidateRecord]:
        """
        Process clusters one at a time from an iterator.

        Yields results as they complete, saving to disk incrementally.
        Memory usage stays constant regardless of total cluster count.

        Args:
            cluster_iterator: Iterator over clusters (from disk)
            storage: StorageManager for persisting results
            show_progress: Whether to show progress bar

        Yields:
            CandidateRecord for each successfully processed cluster
        """
        parallel_enabled = os.getenv("PARALLEL_CLUSTERS", "true").strip().lower() in {"1", "true", "yes", "on"}
        if parallel_enabled and self.max_workers > 1:
            clusters = prioritize_clusters(list(cluster_iterator))
            yield from self._process_clusters_parallel(clusters, storage, show_progress)
            return

        from tqdm import tqdm

        clusters = tqdm(cluster_iterator, desc="Processing clusters") if show_progress else cluster_iterator
        skipped_count = 0
        for cluster in clusters:
            result = self.process_cluster(cluster)
            if result is None:
                skipped_count += 1
                continue

            storage.save_result(result)
            yield result

        if show_progress and skipped_count > 0:
            print(f"Skipped {skipped_count} unsupported/unknown clusters")

    def _process_clusters_parallel(
        self,
        clusters: List[ProcessingCluster],
        storage: StorageManager,
        show_progress: bool,
    ) -> Iterator[CandidateRecord]:
        """Process clusters concurrently and stream-write results as futures complete."""
        from tqdm import tqdm

        skipped_count = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_cluster, cluster) for cluster in clusters]
            iterator = as_completed(futures)
            if show_progress:
                iterator = tqdm(iterator, total=len(futures), desc="Processing clusters")

            for future in iterator:
                result = future.result()
                if result is None:
                    skipped_count += 1
                    continue

                storage.save_result(result)
                yield result

        if show_progress and skipped_count > 0:
            print(f"Skipped {skipped_count} unsupported/unknown clusters")

    def _apply_vision_merge(
        self,
        cluster: ProcessingCluster,
        candidate_record: CandidateRecord,
    ) -> CandidateRecord:
        """Apply vision extraction and merge with regex-based candidate record."""
        use_vision = os.getenv("VISION_LLM_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
        if use_vision:
            vision_payload = extract_vision_record_for_cluster(cluster)
            candidate_record = merge_regex_llm(candidate_record, vision_payload)
        return candidate_record

    @staticmethod
    def _format_source_files(cluster: ProcessingCluster) -> str:
        """Format source filenames from identity cards."""
        filenames = [card.filename for card in cluster.identity_cards]
        return "; ".join(sorted(filenames))
