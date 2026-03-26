from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from src.schema import (
    CandidateRecord,
    NA_EXPORT_COLUMNS,
    to_na_export_row,
)


def save_results(
    records: Iterable[CandidateRecord],
    excel_path: str = "output/na_results.xlsx",
    csv_path: str = "output/na_results.csv",
) -> None:
    rows = [record.to_output_dict() if isinstance(record, CandidateRecord) else record for record in records]
    if not rows:
        print("No data to save.")
        return

    excel_target = Path(excel_path)
    csv_target = Path(csv_path)
    excel_target.parent.mkdir(parents=True, exist_ok=True)
    csv_target.parent.mkdir(parents=True, exist_ok=True)

    na_rows = [to_na_export_row(row) for row in rows if str(row.get("Document Type", "")).lower() == "na"]
    if not na_rows:
        print("No NA records to save.")
        return

    dataframe = pd.DataFrame(na_rows).reindex(columns=NA_EXPORT_COLUMNS)
    dataframe.to_excel(excel_target, index=False)
    dataframe.to_csv(csv_target, index=False)

    print(f"Saved {len(dataframe)} records to {excel_target}")
    print(f"Saved {len(dataframe)} records to {csv_target}")
