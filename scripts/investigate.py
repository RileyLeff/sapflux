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

    df.glimpse()


if __name__ == "__main__":
    main()
