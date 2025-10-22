#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=15", "tabulate>=0.9"]
# ///
"""Quick parquet profiler to highlight compression opportunities."""

from __future__ import annotations

import argparse
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
from tabulate import tabulate


@dataclass
class ColumnSummary:
    name: str
    physical_type: str
    logical_type: str | None
    has_dictionary: bool
    encodings: set[str]
    compressed_size: int = 0
    uncompressed_size: int = 0
    null_count: int | None = None
    min_value: object | None = None
    max_value: object | None = None
    row_count: int = 0

    def update_from_chunk(self, chunk) -> None:
        self.compressed_size += chunk.total_compressed_size
        self.uncompressed_size += chunk.total_uncompressed_size
        stats = chunk.statistics
        if stats is not None:
            current_nulls = self.null_count or 0
            self.null_count = current_nulls + (stats.null_count or 0)
            if self.min_value is None:
                self.min_value = stats.min
            elif stats.has_min_max and stats.min is not None and stats.min < self.min_value:
                self.min_value = stats.min
            if self.max_value is None:
                self.max_value = stats.max
            elif stats.has_min_max and stats.max is not None and stats.max > self.max_value:
                self.max_value = stats.max
        self.has_dictionary = self.has_dictionary or chunk.dictionary_page_offset is not None
        self.encodings.update(str(enc) for enc in chunk.encodings)

    @property
    def compression_ratio(self) -> float | None:
        if self.uncompressed_size == 0:
            return None
        return self.compressed_size / self.uncompressed_size

    @property
    def share(self) -> float:
        return float(self.compressed_size)


def human_bytes(size: int) -> str:
    if size == 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    power = min(int(math.log(size, 1024)), len(units) - 1)
    value = size / (1024**power)
    return f"{value:.1f} {units[power]}"


def collect_metadata(path: Path):
    pf = pq.ParquetFile(path)
    meta = pf.metadata
    summaries: dict[str, ColumnSummary] = {}

    row_groups = []

    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        rg_compressed = 0
        rg_uncompressed = 0

        for col_idx in range(meta.num_columns):
            chunk = rg.column(col_idx)
            name = chunk.path_in_schema
            logical = getattr(chunk, "logical_type", None)

            if name not in summaries:
                summaries[name] = ColumnSummary(
                    name=name,
                    physical_type=str(chunk.physical_type),
                    logical_type=str(logical) if logical else None,
                    has_dictionary=chunk.dictionary_page_offset is not None,
                    encodings=set(),
                )
            summaries[name].row_count += rg.num_rows
            summaries[name].update_from_chunk(chunk)
            rg_compressed += chunk.total_compressed_size
            rg_uncompressed += chunk.total_uncompressed_size

        row_groups.append((rg.num_rows, rg_compressed, rg_uncompressed))

    return meta, summaries, row_groups


def print_report(path: Path) -> None:
    meta, summaries, row_groups = collect_metadata(path)

    total_compressed = sum(size for _, size, _ in row_groups)
    total_uncompressed = sum(
        summary.uncompressed_size for summary in summaries.values()
    )

    print(f"File: {path}")
    print(
        f"Rows: {meta.num_rows:,} | Columns: {meta.num_columns} | Row groups: {meta.num_row_groups}"
    )
    if row_groups:
        avg_rows = sum(rows for rows, _, _ in row_groups) / len(row_groups)
        avg_bytes = sum(size for _, size, _ in row_groups) / len(row_groups)
        print(
            "Avg rows/row group: "
            f"{avg_rows:,.0f} | Avg compressed size/row group: {human_bytes(int(avg_bytes))}"
        )

    if total_uncompressed:
        file_size = path.stat().st_size
        ratio = file_size / total_uncompressed
        print(
            f"File size: {human_bytes(file_size)} | "
            f"Row-group compressed total: {human_bytes(total_compressed)} | "
            f"Compression ratio: {ratio:.3f}"
        )

    rows = []
    for summary in sorted(
        summaries.values(), key=lambda s: s.compressed_size, reverse=True
    ):
        null_rate = (
            (summary.null_count or 0) / summary.row_count
            if summary.row_count and summary.null_count is not None
            else None
        )
        share = summary.compressed_size / total_compressed if total_compressed else 0

        rows.append(
            [
                summary.name,
                summary.physical_type,
                summary.logical_type or "-",
                "Y" if summary.has_dictionary else "N",
                ", ".join(sorted(summary.encodings)) or "-",
                human_bytes(summary.compressed_size),
                (f"{summary.compression_ratio:.3f}" if summary.compression_ratio else "-"),
                f"{share*100:.1f}%",
                (f"{null_rate:.1%}" if null_rate is not None else "-"),
            ]
        )

    print()
    print(
        tabulate(
            rows,
            headers=[
                "column",
                "physical",
                "logical",
                "dict",
                "encodings",
                "compressed",
                "ratio",
                "share",
                "null%",
            ],
            tablefmt="github",
        )
    )

    heavy_hitters = [
        summary
        for summary in sorted(
            summaries.values(), key=lambda s: s.compressed_size, reverse=True
        )
        if total_compressed and summary.compressed_size / total_compressed >= 0.05
    ]

    if heavy_hitters:
        print("\nHeavy columns (>=5% of file):")
        for summary in heavy_hitters:
            print(
                f"  - {summary.name}: min={summary.min_value}, max={summary.max_value}, "
                f"nulls={summary.null_count}, rows={summary.row_count}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="Parquet file to profile")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print_report(args.path)


if __name__ == "__main__":
    main()
