# Developer Guide: Streaming Architecture

A technical deep-dive for developers who want to understand, extend, or debug the streaming architecture.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    The-Compliance-Clerk                     │
│                   Streaming Architecture                    │
└─────────────────────────────────────────────────────────────┘

                         main.py
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
    Phase 1             Phase 2             Phase 3
    ┌─────────────┐  ┌──────────────┐  ┌────────────────┐
    │  PDF Files  │  │   Grouping   │  │   Processing   │
    └──────┬──────┘  └──────┬───────┘  └────────┬───────┘
           │                │                   │
    ┌──────▼──────────┐     │            ┌──────▼──────┐
    │ Identity Cards  │     │            │   Clusters  │
    │   (in memory,   │     │            │ (one at a   │
    │   then disk)    │     │            │  time RAM)  │
    └────────┬────────┘     │            └──────┬──────┘
             │              │                   │
             └──────┬───────┴───────────────────┘
                    │
          intermediate/ [JSONL]
          ├─ identity_cards.jsonl  ← StorageManager writes
          ├─ clusters.jsonl        ← StorageManager writes
          └─ results.jsonl         ← StreamingClusterProcessor writes

    Phase 4: Export
    ┌──────────────────────┐
    │   results.jsonl      │
    └──────┬───────────────┘
           │
    ┌──────▼──────────┐
    │  save_results() │
    └──────┬──────────┘
           │
    ┌──────▼──────────────┐
    │ output/
    │ ├─ na_results.xlsx
    │ └─ na_results.csv
    └─────────────────────┘
```

## Component Breakdown

### 1. StorageManager (src/storage.py)

**Purpose**: Handle all disk I/O using JSONL format

**Key Design Decisions**:
- JSONL format: One JSON object per line
  - Pro: Streaming-friendly, human-readable, append-efficient
  - Con: Not as compact as binary formats
- No caching: Always read from disk for true streaming
- State tracking without loading: Count lines in file

**Internal Implementation**:
```python
class StorageManager:
    def __init__(self, intermediate_dir: str):
        self.intermediate_dir = Path(intermediate_dir)
        # Create subdirectory if needed
        self.intermediate_dir.mkdir(parents=True, exist_ok=True)
        
        # Define paths for all intermediate files
        self.identity_cards_path = self.intermediate_dir / "identity_cards.jsonl"
        self.clusters_path = self.intermediate_dir / "clusters.jsonl"
        self.results_path = self.intermediate_dir / "results.jsonl"
```

**Streaming Patterns**:

```python
# Writing (append mode for streaming)
def save_identity_card(self, card: IdentityCard) -> None:
    with open(self.identity_cards_path, "a") as f:  # append mode
        f.write(json.dumps(card.model_dump()) + "\n")

# Reading (generator for memory efficiency)
def load_identity_cards(self) -> Iterator[IdentityCard]:
    with open(self.identity_cards_path, "r") as f:
        for line in f:
            if line.strip():
                yield IdentityCard(**json.loads(line))

# Counting without loading
def cluster_count(self) -> int:
    with open(self.clusters_path, "r") as f:
        return sum(1 for line in f if line.strip())
```

### 2. StreamingClusterProcessor (src/streaming_processor.py)

**Purpose**: Process one cluster at a time, saving results to disk

**Design Pattern**: Generator/Iterator pattern

```python
def process_clusters_streaming(
    self,
    cluster_iterator: Iterator[ProcessingCluster],
    storage: StorageManager,
) -> Iterator[CandidateRecord]:
    """
    Takes an iterator of clusters,
    processes each, yields result,
    saves to disk.
    
    Memory: Only one cluster in RAM at a time
    """
```

**Key Methods**:

1. `process_cluster(cluster)` - Single cluster processing
   - Builds candidate record from heuristics
   - Applies LLM auditing if available
   - Returns CandidateRecord

2. `process_clusters_streaming(iterator, storage)` - Batch streaming
   - Wraps cluster iterator
   - Calls process_cluster for each
   - Incremental disk save
   - Progress tracking

### 3. EntityGrouper Enhancement (src/grouper.py)

**New Method**: `group_and_persist()`

**Difference from `group()`**:
```python
# Old: Returns list (batch)
clusters: List[ProcessingCluster] = grouper.group(cards)

# New: Persists to disk, yields as created (streaming)
for cluster in grouper.group_and_persist(cards, storage):
    # Cluster already saved to storage
    print(f"Grouped: {cluster.master_key}")
```

**Implementation Logic**:
1. Create grouping logic (same as `group()`)
2. Build clusters in sorted order
3. **Save all clusters to disk**
4. **Yield each cluster one-by-one**

### 4. Updated main.py

**4-Phase Pipeline**:

```python
def main():
    # Phase 1: Discover & Build Identity Cards
    pdf_paths = discover_pdfs()
    identity_cards = [IdentityCardBuilder().build(p) for p in pdf_paths]
    storage.save_identity_cards(identity_cards)
    
    # Phase 2: Group into Clusters
    clusters = list(grouper.group_and_persist(identity_cards, storage))
    
    # Phase 3: Process Clusters (Streaming)
    processor.process_clusters_streaming(
        storage.load_clusters(),  # From disk
        storage                    # Save results to disk
    )
    
    # Phase 4: Export Results
    save_results(storage.load_all_results())  # From disk
```

**Memory Profile**:
- After Phase 1: identity_cards freed (on disk)
- After Phase 2: clusters freed (on disk)
- Phase 3: Only one cluster in RAM at a time
- After Phase 3: results on disk
- Phase 4: Load and export one record at a time

## Data Flow Example

### Example: Processing 3 documents

```
Input PDFs:
├─ order_001.pdf    → document_type: NA_ORDER
├─ lease_001.pdf    → document_type: NA_LEASE
└─ unknown.pdf      → document_type: UNKNOWN

↓ Phase 1: Identity Cards
identity_cards.jsonl:
├─ {"filename": "order_001.pdf", "survey_no": "001", "group_type": "NA", ...}
├─ {"filename": "lease_001.pdf", "survey_no": "001", "group_type": "NA", ...}
└─ {"filename": "unknown.pdf", "group_type": "UNKNOWN", ...}

↓ Phase 2: Grouping & Persistence
clusters.jsonl:
├─ {"master_key": "na:survey:001", "identity_cards": [order, lease]}
└─ {"master_key": "unknown:file:...", "identity_cards": [unknown]}

↓ Phase 3: Streaming Processing
results.jsonl:
└─ {"Document Type": "na", "Master Key": "na:survey:001", ...}
   (unknown cluster skipped)

↓ Phase 4: Export
output/na_results.csv:
sr_no, Village, Survey No., ...
1,    Test,   001,    ...
```

## Extension Points

### 1. Add Custom Storage Backend

```python
# Extend StorageManager to use S3, GCS, etc.

class S3StorageManager(StorageManager):
    def __init__(self, bucket_name: str, prefix: str):
        self.bucket = boto3.client('s3').Bucket(bucket_name)
        self.prefix = prefix
    
    def save_identity_card(self, card: IdentityCard) -> None:
        # Write to S3 instead of local disk
        self.bucket.put_object(
            Key=f"{self.prefix}/identity_cards.jsonl",
            Body=json.dumps(card.model_dump()) + "\n"
        )
```

### 2. Add Parallel Processing

```python
# Process clusters in parallel

from concurrent.futures import ThreadPoolExecutor, as_completed

def process_clusters_parallel(storage: StorageManager, num_workers: int = 4):
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for cluster in storage.load_clusters():
            future = executor.submit(processor.process_cluster, cluster)
            futures.append((cluster.master_key, future))
        
        for master_key, future in futures:
            result = future.result()
            storage.save_result(result)
```

### 3. Add Checkpoint/Resume

```python
# Resume from exact checkpoint

def resume_processing(storage: StorageManager):
    total_clusters = storage.cluster_count()
    processed_clusters = storage.result_count()
    
    remaining = total_clusters - processed_clusters
    print(f"Resuming: {processed_clusters}/{total_clusters} done")
    
    # Load only unprocessed clusters
    # (This requires cluster ordering to be preserved)
```

## Testing Strategy

### Unit Tests (test_streaming_architecture.py)

Each component tested in isolation:
- StorageManager persistence
- Cluster counting
- State tracking

**Why this approach**:
- Fast execution
- No external dependencies
- Clear component boundaries

### Integration Tests (test_streaming_integration.py)

Full pipeline tested with mocked file I/O:
- Identity card creation
- Grouping
- Streaming processing
- Result persistence

**Why this approach**:
- Tests real data flow
- Verifies component interactions
- Mocking avoids PDF dependency

### End-to-End Testing (manual)

With real PDFs:
```bash
python main.py
# Monitor:
ls -lh intermediate/
wc -l intermediate/clusters.jsonl
tail -1 intermediate/results.jsonl | jq .
```

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Save identity card | O(1) | Append to file |
| Load identity cards | O(n) | Stream from disk |
| Group clusters | O(n log n) | Sort by master_key |
| Process cluster | O(m log m) | m = pages in cluster |
| Count files | O(n) | Count lines in file |
| Export results | O(k) | k = results |

### Space Complexity

| Phase | Memory | Storage |
|-------|--------|---------|
| Phase 1 | O(n) | O(n) JSONL |
| Phase 2 | O(1) | + O(c) clusters |
| Phase 3 | O(m) | + O(r) results |
| Phase 4 | O(1) | Final CSV/XLSX |

n = documents, c = clusters, m = cluster size, r = results

## Debugging Guide

### Problem: Process hanging after "Discovering X PDFs"

**Likely cause**: Building identity cards is slow (OCR)
```bash
# Check if process is running
ps aux | grep python

# Check disk I/O
iotop

# Check memory (should stay ~5 MB during identity card phase)
free -h
```

### Problem: Results file empty

**Debug**:
```bash
# Check intermediate state
wc -l intermediate/*.jsonl

# If clusters empty
head -1 intermediate/identity_cards.jsonl | jq '.group_type'

# If results empty but clusters exist
tail -1 intermediate/clusters.jsonl | jq .master_key
```

### Problem: "Disk full" error

**Check**:
```bash
du -sh intermediate/
df -h
# JSONL is text, can be large for 10k+ documents
# Consider archiving old intermediate/ directories
```

## Code Style & Conventions

### Imports
```python
# Local imports go last
from __future__ import annotations  # For forward references
import json                          # stdlib
from pathlib import Path             # stdlib

from src.schema import CandidateRecord  # Local
from src.storage import StorageManager  # Local
```

### Type Hints
```python
# All functions have type hints
def process_cluster(self, cluster: ProcessingCluster) -> CandidateRecord | None:
    # Use | for union types (Python 3.10+)
    # Return None for skipped clusters
    pass

# Iterables use Iterator when streaming
def load_clusters(self) -> Iterator[ProcessingCluster]:
    pass

# Use Any carefully and document why
def load_results(self) -> Iterator[Dict[str, Any]]:
    # Returns raw dicts from JSON (Any fields possible)
    pass
```

### Error Handling
```python
# Streaming functions should be resilient
def load_identity_cards(self) -> Iterator[IdentityCard]:
    if not self.identity_cards_path.exists():
        return  # Graceful exit
    
    with open(self.identity_cards_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            # JSON parse errors bubble up for debugging
            yield IdentityCard(**json.loads(line))
```

## Future Research & Optimization

### 1. Memory-Mapped I/O
Instead of `json.loads()`, use memory-mapped files for faster access

### 2. Incremental Hashing
Add content hashing to detect duplicate documents

### 3. Cluster Prioritization
Sort clusters by likelihood of success and process high-confidence ones first

### 4. Streaming LLM Calls
Batch LLM requests across clusters for API efficiency

---

**Last Updated**: March 27, 2026  
**Maintainers**: The-Compliance-Clerk Development Team
