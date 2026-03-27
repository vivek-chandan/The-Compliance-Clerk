# The Compliance Clerk

The Compliance Clerk extracts structured NA land-compliance data from PDF sets (NA orders + lease deeds) using a streaming pipeline.

It combines:
- heuristic extraction (regex/text rules),
- targeted OCR fallback,
- optional vision-based LLM extraction,
- cluster-level merging,
- disk-backed intermediate state for low memory usage.

## What It Does

- Discovers PDFs recursively under `data/raw_pdfs/`
- Builds lightweight identity cards for each document
- Groups related files into clusters (for example, order + lease for the same survey/village)
- Processes clusters and creates normalized candidate records
- Optionally enriches records with vision LLM extraction
- Exports NA records to CSV and Excel

## Current Architecture (Streaming + Disk Backed)

The runtime in `main.py` uses a 4-phase streaming flow:

1. Discover PDFs and build identity cards
2. Group identity cards into processing clusters
3. Process clusters and persist results incrementally
4. Export final NA records

Intermediate artifacts are written to JSONL files in `intermediate/`:
- `identity_cards.jsonl`
- `clusters.jsonl`
- `results.jsonl`

This keeps memory usage low compared to all-in-memory batch processing.

## Requirements

- Python 3.10+
- Tesseract OCR installed and available on PATH

Install Tesseract:
- Ubuntu/Debian: `sudo apt-get install tesseract-ocr`
- macOS: `brew install tesseract`
- Windows: install from official Tesseract builds and add to PATH

Python packages are listed in `requirements.txt`.

## Setup

```bash
git clone https://github.com/vivek-chandan/The-Compliance-Clerk.git
cd The-Compliance-Clerk

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## LLM Configuration (Optional)

Create a `.env` file in the project root if you want vision extraction through an LLM provider.

OpenAI example:

```env
LLM_PROVIDER=openai
OPENAI_API_KEY=your_key_here
```

OpenRouter example:

```env
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=your_key_here
# Optional override:
# OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
```

If no valid provider key is found, the pipeline automatically runs in regex/OCR-only mode.

## Runtime Controls

Environment variables:

- `VISION_LLM_ENABLED` (default: `true`)
  - `true`: run vision page extraction and merge into record
  - `false`: skip vision stage entirely
- `PARALLEL_CLUSTERS` (default: `true`)
  - `true`: process clusters concurrently with thread pool
  - `false`: process clusters sequentially
- `CLUSTER_MAX_WORKERS` (default: `4`, capped by CPU count)
  - controls worker count when parallel processing is enabled

## Usage

1. Put PDFs in `data/raw_pdfs/` (recursive folders are supported).
2. Run:

```bash
python main.py
```

3. Check outputs:
- `output/results.xlsx`

Note:
- Export includes records where `Document Type` is `na`.
- Unknown clusters are skipped.
- The pipeline currently calls `storage.clear_state()` at run start, so `intermediate/` is reset each run.

## Output and Logs

### Output Files

- `output/results.xlsx`

### Intermediate State

- `intermediate/identity_cards.jsonl`
- `intermediate/clusters.jsonl`
- `intermediate/results.jsonl`
- `intermediate/vision_pages/` (rendered page images for vision extraction)
- `intermediate/vision_json/` (per-page extracted JSON payloads)

### Logs

- `logs/llm_runtime_events.jsonl` (provider disable / runtime LLM events)
- `logs/schema_errors.jsonl`
- `logs/vision_schema_errors.jsonl`

## Project Layout

```text
The-Compliance-Clerk/
├── main.py
├── requirements.txt
├── data/
│   └── raw_pdfs/
├── intermediate/
├── logs/
├── output/
├── src/
│   ├── exporter.py
│   ├── grouper.py
│   ├── llm_handler.py
│   ├── logger.py
│   ├── ocr.py
│   ├── parser.py
│   ├── schema.py
│   ├── storage.py
│   ├── streaming_processor.py
│   ├── validator.py
│   └── vision_pipeline.py
└── README.md
```

