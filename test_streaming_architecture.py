"""
Unit tests for the streaming architecture.

Verifies that storage, clustering, and processing work correctly
without requiring actual PDF files.
"""

import json
import tempfile
from pathlib import Path

from src.schema import (
    DocumentType,
    GroupType,
    IdentityCard,
    ProcessingCluster,
    CandidateRecord,
)
from src.storage import StorageManager


def test_storage_manager_identity_cards():
    """Test that identity cards are persisted and loaded correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)

        # Create test identity cards
        cards = [
            IdentityCard(
                file_path="/test/pdf1.pdf",
                filename="pdf1.pdf",
                document_type=DocumentType.NA_ORDER,
                group_type=GroupType.NA,
                master_key="na:survey:001",
                grouping_basis="survey_number",
                survey_number="001",
                village="TestVillage",
                confidence=0.9,
            ),
            IdentityCard(
                file_path="/test/pdf2.pdf",
                filename="pdf2.pdf",
                document_type=DocumentType.NA_LEASE,
                group_type=GroupType.NA,
                master_key="na:survey:001",
                grouping_basis="survey_number",
                survey_number="001",
                village="TestVillage",
                confidence=0.85,
            ),
        ]

        # Save cards
        storage.save_identity_cards(cards)

        # Load cards one at a time
        loaded_cards = list(storage.load_identity_cards())
        assert len(loaded_cards) == 2
        assert loaded_cards[0].filename == "pdf1.pdf"
        assert loaded_cards[1].filename == "pdf2.pdf"
        print("✓ Identity card persistence works")


def test_storage_manager_clusters():
    """Test that clusters are persisted and loaded correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)

        # Create test clusters
        card1 = IdentityCard(
            file_path="/test/pdf1.pdf",
            filename="pdf1.pdf",
            document_type=DocumentType.NA_ORDER,
            group_type=GroupType.NA,
            master_key="na:survey:001",
            grouping_basis="survey_number",
            survey_number="001",
            village="TestVillage",
            confidence=0.9,
        )

        clusters = [
            ProcessingCluster(
                master_key="na:survey:001",
                group_type=GroupType.NA,
                identity_cards=[card1],
            )
        ]

        # Save clusters
        storage.save_clusters(clusters)

        # Load clusters
        loaded_clusters = list(storage.load_clusters())
        assert len(loaded_clusters) == 1
        assert loaded_clusters[0].master_key == "na:survey:001"
        assert len(loaded_clusters[0].identity_cards) == 1
        print("✓ Cluster persistence works")


def test_storage_manager_results():
    """Test that results are persisted and loaded correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)

        # Create test records
        record1 = CandidateRecord(
            sr_no="1",
            document_type="na",
            source_files="pdf1.pdf",
            master_key="na:survey:001",
            owner_name="Test Owner",
            village="TestVillage",
            survey_no="001",
        )

        records = [record1]

        # Save records
        storage.save_results(records)

        # Load all results
        results = list(storage.load_results())
        assert len(results) == 1
        assert results[0]["Document Type"] == "na"
        print("✓ Result persistence works")


def test_cluster_count():
    """Test that cluster count can be retrieved without loading all clusters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)

        # Create 5 test clusters
        card = IdentityCard(
            file_path="/test/pdf.pdf",
            filename="pdf.pdf",
            document_type=DocumentType.NA_ORDER,
            group_type=GroupType.NA,
            master_key="test",
            grouping_basis="survey_number",
            confidence=0.9,
        )

        clusters = [
            ProcessingCluster(
                master_key=f"test_{i}",
                group_type=GroupType.NA,
                identity_cards=[card],
            )
            for i in range(5)
        ]

        storage.save_clusters(clusters)
        count = storage.cluster_count()
        assert count == 5
        print("✓ Cluster counting works")


def test_state_summary():
    """Test that state summary provides correct information."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)

        # Add some data
        card = IdentityCard(
            file_path="/test/pdf.pdf",
            filename="pdf.pdf",
            document_type=DocumentType.NA_ORDER,
            group_type=GroupType.NA,
            master_key="test",
            grouping_basis="survey_number",
            confidence=0.9,
        )

        storage.save_identity_cards([card])
        storage.save_clusters(
            [ProcessingCluster(master_key="test", group_type=GroupType.NA, identity_cards=[card])]
        )
        storage.save_results(
            [CandidateRecord(sr_no="1", document_type="na", master_key="test")]
        )

        summary = storage.get_state_summary()
        assert summary["identity_cards_count"] == 1
        assert summary["clusters_count"] == 1
        assert summary["results_count"] == 1
        print("✓ State summary works")


if __name__ == "__main__":
    test_storage_manager_identity_cards()
    test_storage_manager_clusters()
    test_storage_manager_results()
    test_cluster_count()
    test_state_summary()
    print("\n✅ All streaming architecture tests passed!")
