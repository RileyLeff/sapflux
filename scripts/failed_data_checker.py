#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["rich>=13", "orjson>=3"]
# ///

"""Summarise sapflux transaction receipt outcomes by file category.

Usage:
    uv run scripts/failed_data_checker.py path/to/receipt.json

When no path is provided the script looks for the most recently modified
`integration_tests/**/output/full_smoke_*.json` file.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import orjson
from rich.console import Console
from rich.table import Table


console = Console()


@dataclass
class FileRecord:
    path: str
    status: str
    parser_attempts: List[dict]

    @property
    def lower_path(self) -> str:
        return self.path.lower()

    def has_utf8_error(self) -> bool:
        return any(
            attempt.get("parser") == "utf8"
            and "not valid utf-8" in attempt.get("message", "").lower()
            for attempt in self.parser_attempts
        )


class ReceiptAnalyzer:
    def __init__(self, files: Iterable[FileRecord]):
        self.files = list(files)

    def classify(self) -> dict[str, List[FileRecord]]:
        categories: dict[str, List[FileRecord]] = {
            "accepted": [],
            "duplicates": [],
            "public": [],
            "status": [],
            "datatableinfo": [],
            "nonutf8": [],
            "other": [],
        }

        for record in self.files:
            if record.status.lower() == "parsed":
                categories["accepted"].append(record)
                continue
            if record.status.lower() == "duplicate":
                categories["duplicates"].append(record)
                continue

            lowered = record.lower_path
            if "public" in lowered:
                categories["public"].append(record)
                continue
            if "status" in lowered:
                categories["status"].append(record)
                continue
            if "datatableinfo" in lowered:
                categories["datatableinfo"].append(record)
                continue
            if record.has_utf8_error():
                categories["nonutf8"].append(record)
                continue

            categories["other"].append(record)

        return categories


def load_receipt(path: Path) -> dict:
    content = path.read_bytes()
    try:
        return orjson.loads(content)
    except orjson.JSONDecodeError:
        # fallback to stdlib for friendlier error messages
        return json.loads(content)


def infer_latest_receipt() -> Optional[Path]:
    patterns = ["integration_tests", "**", "output", "full_smoke_*.json"]
    root = Path.cwd()
    candidates = list(root.joinpath(patterns[0]).glob(os.path.join(*patterns[1:])))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "receipt",
        nargs="?",
        type=Path,
        help="Path to transaction receipt JSON (defaults to latest full_smoke JSON)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of example paths to show per category (default: 10)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    receipt_path = args.receipt or infer_latest_receipt()
    if receipt_path is None:
        console.print("[red]No receipt path provided and no full_smoke_*.json found.[/]")
        raise SystemExit(1)
    if not receipt_path.exists():
        console.print(f"[red]Receipt not found: {receipt_path}[/]")
        raise SystemExit(1)

    payload = load_receipt(receipt_path)
    file_entries = payload.get("receipt", {}).get("files", [])
    records = [
        FileRecord(
            path=entry.get("path", "<unknown>"),
            status=entry.get("status", "Unknown"),
            parser_attempts=entry.get("parser_attempts", []),
        )
        for entry in file_entries
    ]

    analyzer = ReceiptAnalyzer(records)
    categories = analyzer.classify()

    table = Table(title=f"Transaction receipt summary: {receipt_path}")
    table.add_column("Category", justify="left")
    table.add_column("Count", justify="right")
    table.add_column("Example paths", justify="left")

    for name, files in categories.items():
        examples = ",\n".join(record.path for record in files[: args.top]) or "-"
        table.add_row(name, str(len(files)), examples)

    console.print(table)


if __name__ == "__main__":
    main()
