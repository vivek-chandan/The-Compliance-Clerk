from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Union

import pandas as pd

from src.schema import (
    CandidateRecord,
    NA_EXPORT_COLUMNS,
    to_na_export_row,
)


def save_results(
    records: Iterable[Union[CandidateRecord, Dict[str, Any]]],
    excel_path: str = "output/results.xlsx",
    #csv_path: str = "output/results.csv",
) -> None:
    """
    Save results to Excel and CSV formats.

    Args:
        records: Iterable of CandidateRecord objects or dictionaries
        excel_path: Output path for Excel file
    
    """
    # Convert all records to dictionaries
    rows = [
        record.to_output_dict() if isinstance(record, CandidateRecord) else record
        for record in records
    ]

    if not rows:
        print("No data to save.")
        return

    excel_target = Path(excel_path)
   
    excel_target.parent.mkdir(parents=True, exist_ok=True)
 

    rows = _dedupe_rows_by_master_key(rows)

    # Filter for NA records only
    na_rows = [
        to_na_export_row(row)
        for row in rows
        if str(row.get("Document Type", "")).lower() == "na"
    ]

    if not na_rows:
        print("No NA records to save.")
        return

    # Create DataFrame and export
    dataframe = pd.DataFrame(na_rows).reindex(columns=NA_EXPORT_COLUMNS)
    dataframe.to_excel(excel_target, index=False)
    

    print(f"Saved {len(dataframe)} records to {excel_target}")
 


def _dedupe_rows_by_master_key(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    best_rows: Dict[str, Dict[str, Any]] = {}
    passthrough: list[Dict[str, Any]] = []

    for row in rows:
        master_key = str(row.get("Master Key", "") or "").strip()
        if not master_key:
            passthrough.append(row)
            continue
        existing = best_rows.get(master_key)
        if existing is None or _row_score(row) > _row_score(existing):
            best_rows[master_key] = row

    return list(best_rows.values()) + passthrough


def _row_score(row: Dict[str, Any]) -> tuple[int, int]:
    values = [
        str(value or "").strip()
        for key, value in row.items()
        if key not in {"sr no", "Document Type", "Source Files", "Master Key"}
    ]
    filled_count = sum(1 for value in values if value)
    total_length = sum(len(value) for value in values)
    return filled_count, total_length
