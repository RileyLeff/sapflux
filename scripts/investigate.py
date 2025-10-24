# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars",
# ]
# ///

from pathlib import Path

import polars as pl


def main() -> None:
    output_dir = Path("integration_tests/rileydata/output")
    parquet_files = list(output_dir.glob("*.parquet"))
    most_recent = max(parquet_files, key=lambda p: p.stat().st_mtime)

    print(f"Reading: {most_recent.name}\n")

    df = pl.read_parquet(most_recent)

    print("Column names:")
    print(df.columns)
    print(f"\nTotal columns: {len(df.columns)}")
    print(f"Total rows: {len(df)}\n")

    print("Sample data for each column (first 5 rows):")
    print("=" * 80)

    for col in df.columns:
        print(f"\n{col}:")
        print(df[col].head(5))


if __name__ == "__main__":
    main()
