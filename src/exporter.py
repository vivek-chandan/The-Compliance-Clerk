from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from src.schema import (
    CandidateRecord,
    ECHALLAN_EXPORT_COLUMNS,
    NA_EXPORT_COLUMNS,
    OUTPUT_COLUMNS,
    to_echallan_export_row,
    to_na_export_row,
)


def save_results(
    records: Iterable[CandidateRecord],
    excel_path: str = "output/results.xlsx",
    csv_path: str = "output/results.csv",
) -> None:
    rows = [record.to_output_dict() if isinstance(record, CandidateRecord) else record for record in records]
    if not rows:
        print("No data to save.")
        return

    excel_target = Path(excel_path)
    csv_target = Path(csv_path)
    excel_target.parent.mkdir(parents=True, exist_ok=True)
    csv_target.parent.mkdir(parents=True, exist_ok=True)

    dataframe = pd.DataFrame(rows).reindex(columns=OUTPUT_COLUMNS)
    dataframe.to_excel(excel_target, index=False)
    dataframe.to_csv(csv_target, index=False)

    print(f"Saved {len(dataframe)} records to {excel_target}")
    print(f"Saved {len(dataframe)} records to {csv_target}")

    _save_split_results(rows, excel_target.parent)


def _save_split_results(rows: list[dict], output_dir: Path) -> None:
    na_rows = [to_na_export_row(row) for row in rows if str(row.get("Document Type", "")).lower() == "na"]
    echallan_rows = [
        to_echallan_export_row(row) for row in rows if str(row.get("Document Type", "")).lower() == "echallan"
    ]

    if na_rows:
        na_df = pd.DataFrame(na_rows).reindex(columns=NA_EXPORT_COLUMNS)
        na_excel = output_dir / "na_results.xlsx"
        na_csv = output_dir / "na_results.csv"
        na_df.to_excel(na_excel, index=False)
        na_df.to_csv(na_csv, index=False)
        print(f"Saved {len(na_df)} NA records to {na_excel}")
        print(f"Saved {len(na_df)} NA records to {na_csv}")

    if echallan_rows:
        echallan_df = pd.DataFrame(echallan_rows).reindex(columns=ECHALLAN_EXPORT_COLUMNS)
        echallan_excel = output_dir / "echallan_results.xlsx"
        echallan_csv = output_dir / "echallan_results.csv"
        echallan_df.to_excel(echallan_excel, index=False)
        echallan_df.to_csv(echallan_csv, index=False)
        print(f"Saved {len(echallan_df)} eChallan records to {echallan_excel}")
        print(f"Saved {len(echallan_df)} eChallan records to {echallan_csv}")
