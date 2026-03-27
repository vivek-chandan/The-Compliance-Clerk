# Quick Reference: Streaming Architecture

## File Structure

```
src/
├── storage.py                    # NEW: Disk I/O layer
├── streaming_processor.py        # NEW: Process clusters one-by-one
├── grouper.py                    # UPDATED: Added group_and_persist()
├── exporter.py                   # UPDATED: Handle dict inputs
└── main.py                       # UPDATED: Uses streaming pipeline

intermediate/                      # NEW: Persistent intermediate data
├── identity_cards.jsonl
├── clusters.jsonl
└── results.jsonl
```

## Main Processing Flow

```python
# 1. Discover and build identity cards
pdfs = discover_pdfs()
cards = [IdentityCardBuilder().build(p) for p in pdfs]
storage.save_identity_cards(cards)

# 2. Group into clusters (persisted to disk)
clusters = grouper.group_and_persist(cards, storage)

# 3. Process one cluster at a time
processor.process_clusters_streaming(
    storage.load_clusters(),  # Loads from disk
    storage,
    show_progress=True
)

# 4. Export results
save_results(storage.load_all_results())
```

## Key APIs

### StorageManager
```python
storage = StorageManager(intermediate_dir="intermediate")

# Identity cards
storage.save_identity_cards(cards)
for card in storage.load_identity_cards():  # Streaming
    pass

# Clusters  
storage.save_clusters(clusters)
for cluster in storage.load_clusters():  # Streaming
    pass

# Results
storage.save_result(record)  # Append single
for result in storage.load_results():  # Streaming
    pass

# State
storage.cluster_count()
storage.get_state_summary()
storage.clear_state()
```

### StreamingClusterProcessor
```python
processor = StreamingClusterProcessor()

# Single cluster
result = processor.process_cluster(cluster)

# Streaming
for result in processor.process_clusters_streaming(
    iterator,
    storage,
    show_progress=True
):
    pass
```

### EntityGrouper (new method)
```python
grouper = EntityGrouper()

# Old: Returns list (batch)
clusters = grouper.group(cards)

# New: Persists to disk, yields as created (streaming)
for cluster in grouper.group_and_persist(cards, storage):
    pass
```

## Memory Footprint

| Operation | Duration | Memory |
|-----------|----------|--------|
| Build 100 identity cards | ~1 sec/card | ~5KB per card |
| Group into clusters | ~instant | Minimal (grouping logic) |
| Process 1 cluster | ~2-5 sec (with OCR+LLM) | ~50KB |
| Total for 100 PDFs | ~5-10 mins | **Constant ~50MB** |

## Streaming vs Batch Comparison

### Batch (Old main.py)
```python
identity_cards = [...]  # ALL in RAM
clusters = grouper.group(identity_cards)  # ALL in RAM
for cluster in clusters:
    results.append(process_cluster(cluster))
save_results(results)  # ALL in RAM until export
```

### Streaming (New main.py)
```python
identity_cards = [...]
storage.save_identity_cards(identity_cards)  # Persisted

clusters = list(grouper.group_and_persist(cards, storage))
for result in processor.process_clusters_streaming(
    storage.load_clusters(),  # One at a time from disk
    storage
):
    pass
storage.load_all_results()  # Load from disk for export
```

## Debugging Intermediate Files

```bash
# View first identity card
head -1 intermediate/identity_cards.jsonl | jq .

# Count clusters by type
jq '.group_type' intermediate/clusters.jsonl | sort | uniq -c

# List all master keys
jq '.master_key' intermediate/clusters.jsonl

# Sample results
tail -5 intermediate/results.jsonl | jq .

# Check file sizes
du -h intermediate/
```

## Resuming from Checkpoint

```python
storage = StorageManager()

# Skip to cluster processing if already grouped
if storage.has_clusters():
    print("Clusters already grouped, resuming processing...")
    processor.process_clusters_streaming(
        storage.load_clusters(),
        storage
    )
else:
    # Full pipeline
    clusters = grouper.group_and_persist(cards, storage)
```

## Performance Tips

1. **Parallel Processing**: Each cluster is independent - can process in thread pool
2. **Incremental Export**: Don't wait for all results - export per-batch
3. **Monitor Progress**: Check `storage.result_count()` / `storage.cluster_count()`
4. **Clear Old Runs**: `StorageManager().clear_state()` before new batch

## Migration from Batch to Streaming

**Old code:**
```python
clusters = grouper.group(identity_cards)
results = [process_cluster(c) for c in clusters]
save_results(results)
```

**New code (drop-in replacement):**
```python
storage = StorageManager()
storage.save_identity_cards(identity_cards)

clusters = grouper.group_and_persist(identity_cards, storage)
results = list(processor.process_clusters_streaming(
    storage.load_clusters(), storage
))
storage.load_all_results()  # Load from disk
```

Both work, but new version uses disk for scalability.
