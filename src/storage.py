"""
Disk-based storage layer for efficient memory management.

Provides streaming I/O capabilities for identity cards, clusters, and results,
avoiding the need to keep all data in memory simultaneously.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from src.schema import (
    CandidateRecord,
    IdentityCard,
    ProcessingCluster,
)


class StorageManager:
    """Manages disk-based persistence of intermediate processing state."""

    def __init__(self, intermediate_dir: str = "intermediate"):
        self.intermediate_dir = Path(intermediate_dir)
        self.intermediate_dir.mkdir(parents=True, exist_ok=True)

        self.identity_cards_path = self.intermediate_dir / "identity_cards.jsonl"
        self.clusters_path = self.intermediate_dir / "clusters.jsonl"
        self.results_path = self.intermediate_dir / "results.jsonl"

    def clear_state(self) -> None:
        """Clear all intermediate files to start fresh."""
        for path in [self.identity_cards_path, self.clusters_path, self.results_path]:
            if path.exists():
                path.unlink()

    # ==================== Identity Cards I/O ====================

    def save_identity_card(self, card: IdentityCard) -> None:
        """Append a single identity card to disk (streaming write)."""
        with open(self.identity_cards_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(card.model_dump()) + "\n")

    def save_identity_cards(self, cards: Iterable[IdentityCard]) -> None:
        """Stream identity cards to disk."""
        with open(self.identity_cards_path, "w", encoding="utf-8") as f:
            for card in cards:
                f.write(json.dumps(card.model_dump()) + "\n")

    def load_identity_cards(self) -> Iterator[IdentityCard]:
        """Load identity cards from disk one at a time (memory-efficient)."""
        if not self.identity_cards_path.exists():
            return

        with open(self.identity_cards_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                yield IdentityCard(**data)

    def load_all_identity_cards(self) -> list[IdentityCard]:
        """Load all identity cards into memory (use when needed for grouping logic)."""
        return list(self.load_identity_cards())

    # ==================== Clusters I/O ====================

    def save_cluster(self, cluster: ProcessingCluster) -> None:
        """Append a single cluster to disk (streaming write)."""
        with open(self.clusters_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(cluster.model_dump()) + "\n")

    def save_clusters(self, clusters: Iterable[ProcessingCluster]) -> None:
        """Stream clusters to disk."""
        with open(self.clusters_path, "w", encoding="utf-8") as f:
            for cluster in clusters:
                f.write(json.dumps(cluster.model_dump()) + "\n")

    def load_clusters(self) -> Iterator[ProcessingCluster]:
        """Load clusters from disk one at a time (enables streaming processing)."""
        if not self.clusters_path.exists():
            return

        with open(self.clusters_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                yield ProcessingCluster(**data)

    def cluster_count(self) -> int:
        """Get total number of clusters without loading them all."""
        if not self.clusters_path.exists():
            return 0
        with open(self.clusters_path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())

    # ==================== Results I/O ====================

    def save_result(self, record: CandidateRecord) -> None:
        """Append a single result to disk (streaming write)."""
        with open(self.results_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record.to_output_dict()) + "\n")

    def save_results(self, records: Iterable[CandidateRecord]) -> None:
        """Stream results to disk."""
        with open(self.results_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record.to_output_dict()) + "\n")

    def load_results(self) -> Iterator[Dict[str, Any]]:
        """Load results from disk one at a time."""
        if not self.results_path.exists():
            return

        with open(self.results_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def load_all_results(self) -> list[Dict[str, Any]]:
        """Load all results into memory."""
        return list(self.load_results())

    def result_count(self) -> int:
        """Get total number of results without loading them all."""
        if not self.results_path.exists():
            return 0
        with open(self.results_path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())

    # ==================== State Tracking ====================

    def has_identity_cards(self) -> bool:
        """Check if identity cards have been persisted."""
        return self.identity_cards_path.exists() and self.identity_cards_path.stat().st_size > 0

    def has_clusters(self) -> bool:
        """Check if clusters have been persisted."""
        return self.clusters_path.exists() and self.clusters_path.stat().st_size > 0

    def has_results(self) -> bool:
        """Check if any results have been persisted."""
        return self.results_path.exists() and self.results_path.stat().st_size > 0

    def get_state_summary(self) -> Dict[str, Any]:
        """Get summary of persisted state."""
        return {
            "identity_cards_count": self._count_lines(self.identity_cards_path),
            "clusters_count": self.cluster_count(),
            "results_count": self.result_count(),
            "storage_dir": str(self.intermediate_dir),
        }

    @staticmethod
    def _count_lines(path: Path) -> int:
        """Count non-empty lines in a file."""
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
