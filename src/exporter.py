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
   # csv_path: str = "output/results.csv",
) -> None:
    """
    Save results to Excel and CSV formats.

    Args:
        records: Iterable of CandidateRecord objects or dictionaries
        excel_path: Output path for Excel file
        csv_path: Output path for CSV file
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
    csv_target = Path(csv_path)
    excel_target.parent.mkdir(parents=True, exist_ok=True)
    csv_target.parent.mkdir(parents=True, exist_ok=True)

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
  #  dataframe.to_csv(csv_target, index=False)

    print(f"Saved {len(dataframe)} records to {excel_target}")
  #  print(f"Saved {len(dataframe)} records to {csv_target}")
