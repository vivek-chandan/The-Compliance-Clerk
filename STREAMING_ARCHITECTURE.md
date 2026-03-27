# Streaming Architecture Implementation Guide

## Overview

The Compliance Clerk has been refactored to use a **disk-based, streaming architecture** that significantly reduces memory usage and enables processing of very large PDF batches without OOM crashes.

### Previous Architecture (Batch Processing)
```
All PDFs → Build ALL identity cards (RAM) 
        → Create ALL clusters (RAM)
        → Process ALL clusters (RAM)
        → Accumulate ALL results (RAM)
        → Export to disk
```
**Memory Problem:** All data across all documents held in RAM simultaneously.

### New Architecture (Streaming Processing)
```
All PDFs → Build identity cards → Save to disk (JSONL)
                                 ↓
                            Group documents → Save clusters to disk (JSONL)
                                 ↓
                   Load clusters one-by-one from disk
                                 ↓
                         Process cluster → Save result to disk (JSONL)
                                 ↓
                         Final export from disk
```
**Memory Benefit:** Only ONE cluster processed at a time. Constant memory usage regardless of batch size.

---

## New Components

### 1. StorageManager (`src/storage.py`)

Handles all persistent I/O for disk-based data management.

**Key Features:**
- **Identity Card I/O:** Stream identity cards to/from disk (JSONL format)
- **Cluster I/O:** Persist clusters for disk-based grouping
- **Result I/O:** Save processing results incrementally
- **State Tracking:** Query counts and status without loading everything

**Usage:**
```python
from src.storage import StorageManager

storage = StorageManager()

# Save identity cards (streaming write)
storage.save_identity_cards(identity_cards)

# Load clusters one at a time (memory efficient)
for cluster in storage.load_clusters():
    process(cluster)

# Check state without loading everything
count = storage.cluster_count()  # O(n) but doesn't hold in RAM
summary = storage.get_state_summary()
```

### 2. StreamingClusterProcessor (`src/streaming_processor.py`)

Processes clusters one at a time with automatic result persistence.

**Key Features:**
- Process individual clusters: `process_cluster(cluster)`
- Stream processing mode: `process_clusters_streaming(iterator, storage)`
- Automatic LLM auditing if available
- Results saved to disk incrementally

**Usage:**
```python
from src.streaming_processor import StreamingClusterProcessor
from src.storage import StorageManager

storage = StorageManager()
processor = StreamingClusterProcessor()

# Process clusters from disk, streaming results to disk
for result in processor.process_clusters_streaming(
    storage.load_clusters(),
    storage,
    show_progress=True
):
    # Results also automatically saved to disk
    print(f"Processed: {result.master_key}")
```

### 3. Enhanced EntityGrouper (`src/grouper.py`)

Extended with disk-based grouping capability.

**New Method: `group_and_persist()`**
```python
grouper = EntityGrouper()

# Groups documents AND saves clusters to disk
for cluster in grouper.group_and_persist(identity_cards, storage):
    # Yields clusters as they're created
    # Clusters also persisted to disk
    print(f"Cluster: {cluster.master_key}")
```

---

## Processing Pipeline (Updated main.py)

```python
# Phase 1: Build Identity Cards
identity_cards = [identity_builder.build(path) for path in pdf_paths]
storage.save_identity_cards(identity_cards)  # Persist before grouping

# Phase 2: Group & Persist Clusters
clusters = list(grouper.group_and_persist(identity_cards, storage))

# Phase 3: Process Streaming (Low Memory!)
cluster_iterator = storage.load_clusters()  # Load from disk
results = processor.process_clusters_streaming(cluster_iterator, storage)

# Phase 4: Export from Disk
all_results = storage.load_all_results()
save_results(all_results)
```

---

## Intermediate Storage Format

All intermediate data is stored as **JSONL** (JSON Lines - one JSON object per line) in the `intermediate/` directory:

```
intermediate/
├── identity_cards.jsonl    # One card per line
├── clusters.jsonl          # One cluster per line
└── results.jsonl           # One result per line
```

**JSONL Benefits:**
- ✅ Human-readable (can inspect with `cat` or `tail`)
- ✅ Append-friendly (streaming writes)
- ✅ One record per line (easy to parse incrementally)
- ✅ JSON format (language-neutral)

**Example:**
```bash
# View first identity card
head -1 intermediate/identity_cards.jsonl

# Count clusters
wc -l intermediate/clusters.jsonl

# Extract specific field
jq '.master_key' intermediate/clusters.jsonl | sort | uniq -c
```

---

## Memory Usage Comparison

### Batch Processing (Old)
```
For 1000 PDFs with 500 pages each:
- Identity cards: 1000 objects × ~5KB = 5 MB
- Page texts cached: 1000 × 500 pages × 2KB = 1 GB  ← HUGE!
- Clusters: 100 clusters × 10 cards × 5KB = 5 MB
- Results accumulation: 100 results × 1KB = 100 KB
TOTAL: ~1-2 GB peak memory
```

### Streaming Processing (New)
```
For same 1000 PDFs:
- Current cluster in RAM: 1 cluster × 10 cards × 5KB = 50 KB
- Current result in RAM: 1 result × 1KB = 1 KB
- Parser cache: ~10-50 MB (configurable)
TOTAL: ~50-100 MB peak memory (20x reduction!)
```

---

## Configuration

### Clear Previous Runs
```python
storage = StorageManager()
storage.clear_state()  # Removes intermediate/identity_cards.jsonl, etc.
```

### Custom Storage Location
```python
storage = StorageManager(intermediate_dir="custom/path")
```

### Resume from Checkpoint
The intermediate files allow you to resume:

```python
# If grouper fails after identity card phase:
if not storage.has_clusters():
    clusters = list(grouper.group_and_persist(
        storage.load_identity_cards(), 
        storage
    ))

# If processor fails mid-way:
processed_so_far = storage.result_count()
print(f"Already processed {processed_so_far} of {storage.cluster_count()} clusters")
```

---

## Benefits of Streaming Architecture

| Aspect | Batch (Old) | Streaming (New) |
|--------|-----------|-------------------|
| **Memory Usage** | O(n) - all data | O(1) - constant small buffer |
| **Max File Size** | Crashes on large batches | Unlimited |
| **Resume Capability** | None - restart from 0 | Yes - check intermediate/ |
| **Debugging** | Black box processing | Inspect intermediate/ files |
| **Intermediate Data** | Lost on crash | Persisted to disk |
| **Horizontal Scale** | Difficult | Easier (cluster = unit of work) |

---

## Testing

Unit tests for the streaming architecture:

```bash
python test_streaming_architecture.py
```

Tests cover:
- Identity card persistence and loading
- Cluster persistence and loading  
- Result persistence and loading
- State tracking and summaries

---

## Troubleshooting

### "Intermediate directory not cleaning up"
- Manually: `rm -rf intermediate/`
- Code: `StorageManager().clear_state()`

### "Want to inspect intermediate data"
```bash
# Pretty-print first cluster
head -1 intermediate/clusters.jsonl | jq .

# Count results by document type
jq '.["Document Type"]' intermediate/results.jsonl | sort | uniq -c
```

### "Processing seems stuck"
```bash
# Check progress
wc -l intermediate/results.jsonl  # Results processed so far
wc -l intermediate/clusters.jsonl  # Total clusters
```

---

## Future Enhancements

1. **Parallel Processing**: Process multiple clusters simultaneously (thread pool)
2. **Checkpointing**: Save/resume from exact cluster
3. **Incremental Exports**: Export results file-by-file instead of batching
4. **Cloud Storage**: Support S3/GCS for intermediate files
5. **Progress Tracking**: Detailed metrics per cluster

---

## Legacy Interface

The old batch API still works for backward compatibility:

```python
clusters = grouper.group(identity_cards)  # Still returns List[ProcessingCluster]
```

Use `group_and_persist()` for new code to benefit from streaming.
