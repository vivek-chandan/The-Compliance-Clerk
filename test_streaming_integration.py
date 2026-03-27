"""
Integration test for the streaming pipeline architecture.

Tests that all components work together without requiring actual PDFs.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.grouper import EntityGrouper, IdentityCardBuilder
from src.parser import HeuristicParser
from src.schema import (
    DocumentType,
    GroupType,
    IdentityCard,
    ProcessingCluster,
    CandidateRecord,
)
from src.storage import StorageManager
from src.streaming_processor import StreamingClusterProcessor


def test_full_streaming_pipeline():
    """Test the complete streaming pipeline from identity card to result."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Step 1: Create mock identity cards
        print("Step 1: Creating identity cards...")
        cards = [
            IdentityCard(
                file_path="/test/na_order_001.pdf",
                filename="na_order_001.pdf",
                document_type=DocumentType.NA_ORDER,
                group_type=GroupType.NA,
                master_key="na:survey:001",
                grouping_basis="survey_number",
                survey_number="001",
                village="Testage",
                confidence=0.95,
                sample_text="Sample text for order 001",
            ),
            IdentityCard(
                file_path="/test/na_lease_001.pdf",
                filename="na_lease_001.pdf",
                document_type=DocumentType.NA_LEASE,
                group_type=GroupType.NA,
                master_key="na:survey:001",
                grouping_basis="survey_number",
                survey_number="001",
                village="Testage",
                confidence=0.90,
                sample_text="Sample text for lease 001",
            ),
            IdentityCard(
                file_path="/test/unknown_001.pdf",
                filename="unknown_001.pdf",
                document_type=DocumentType.UNKNOWN,
                group_type=GroupType.UNKNOWN,
                master_key="unknown:file:unknown_001",
                grouping_basis="filename",
                confidence=0.0,
            ),
        ]
        
        # Step 2: Persist identity cards
        print("Step 2: Persisting identity cards to disk...")
        storage.save_identity_cards(cards)
        assert storage.has_identity_cards()
        assert len(list(storage.load_identity_cards())) == 3
        print("  ✓ Identity cards persisted")
        
        # Step 3: Group and persist clusters
        print("Step 3: Grouping documents and persisting clusters...")
        grouper = EntityGrouper()
        created_clusters = list(grouper.group_and_persist(cards, storage))
        assert storage.has_clusters()
        persisted_clusters = list(storage.load_clusters())
        
        # Should have 2 clusters: 1 for NA (survey 001), 1 for UNKNOWN
        assert len(persisted_clusters) == 2
        print(f"  ✓ Created {len(persisted_clusters)} clusters")
        
        # Verify cluster composition
        na_cluster = [c for c in persisted_clusters if c.group_type == GroupType.NA][0]
        unknown_cluster = [c for c in persisted_clusters if c.group_type == GroupType.UNKNOWN][0]
        
        assert len(na_cluster.identity_cards) == 2  # Order + Lease
        assert len(unknown_cluster.identity_cards) == 1
        print(f"  ✓ NA cluster has {len(na_cluster.identity_cards)} documents")
        print(f"  ✓ Unknown cluster has {len(unknown_cluster.identity_cards)} documents")
        
        # Step 4: Process clusters (with mocked parser)
        print("Step 4: Processing clusters from disk...")
        parser = HeuristicParser()
        processor = StreamingClusterProcessor(parser)
        
        # Mock the parser methods to avoid accessing real files
        def mock_build_record(cluster):
            record = CandidateRecord(
                sr_no="",
                document_type="na",
                source_files="; ".join(c.filename for c in cluster.identity_cards),
                master_key=cluster.master_key,
            )
            return record
        
        # Patch only for this test
        with patch.object(parser, 'build_candidate_record', side_effect=mock_build_record):
            with patch('src.streaming_processor.extract_vision_record_for_cluster', return_value={}):
                processed_count = 0
                for result in processor.process_clusters_streaming(
                    storage.load_clusters(),
                    storage,
                    show_progress=False,
                ):
                    processed_count += 1
                    assert result is not None
                    print(f"  ✓ Processed cluster: {result.master_key}")
        
        # Should be 1 result (NA cluster), 1 skipped (UNKNOWN)
        assert processed_count == 1
        print(f"  ✓ Processed {processed_count} cluster(s)")
        
        # Step 5: Verify results persistence
        print("Step 5: Verifying results were persisted...")
        assert storage.has_results()
        persisted_results = list(storage.load_results())
        assert len(persisted_results) == 1
        result = persisted_results[0]
        assert result["Document Type"] == "na"
        assert result["Master Key"] == "na:survey:001"
        print(f"  ✓ Result persisted: {result['Master Key']}")
        
        # Step 6: Check state summary
        print("Step 6: Verifying state summary...")
        state = storage.get_state_summary()
        assert state["identity_cards_count"] == 3
        assert state["clusters_count"] == 2
        assert state["results_count"] == 1
        print(f"  Summary: {state}")
        
        print("\n✅ Full streaming pipeline test passed!")


if __name__ == "__main__":
    test_full_streaming_pipeline()
