# /// script
# dependencies = ["tomli-w"]
# ///
"""Scan raw Sapflux data files to ensure RECORD increments by 1 between adjacent rows."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import tomli_w

DISALLOWED_SUBSTRINGS = ("datatableinfo", "public", "status")
HEADER_LINES_EXPECTED = 4


@dataclass(slots=True)
class RowSnapshot:
    line_number: int
    record: Optional[int]
    timestamp: Optional[str]
    fields: List[str]


@dataclass(slots=True)
class InvalidRecord:
    line_number: int
    raw_value: str
    reason: str
    fields: List[str]


@dataclass(slots=True)
class Anomaly:
    file_path: Path
    difference: int
    row_above: RowSnapshot
    row_below: RowSnapshot


@dataclass(slots=True)
class FileResult:
    path: Path
    total_rows: int
    valid_record_rows: int
    invalid_records: List[InvalidRecord]
    anomalies: List[Anomaly]
    errors: List[str]


def iter_data_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        name_lower = path.name.lower()
        if any(bad in name_lower for bad in DISALLOWED_SUBSTRINGS):
            continue
        yield path


def find_column_index(header: List[str], candidates: Iterable[str]) -> Optional[int]:
    normalized = [col.strip().upper() for col in header]
    candidate_set = {cand.strip().upper() for cand in candidates}
    for idx, column in enumerate(normalized):
        if column in candidate_set:
            return idx
    return None


def parse_record_value(value: str) -> Optional[int]:
    cleaned = value.strip()
    if not cleaned:
        return None
    upper = cleaned.upper()
    if upper in {"NAN", "NULL", "NA", "INF", "-INF"}:
        return None
    try:
        if any(ch in cleaned for ch in (".", "e", "E")):
            numeric = float(cleaned)
            if not numeric.is_integer():
                return None
            return int(numeric)
        return int(cleaned)
    except ValueError:
        return None


def analyze_file(path: Path) -> FileResult:
    total_rows = 0
    valid_record_rows = 0
    invalid_records: List[InvalidRecord] = []
    anomalies: List[Anomaly] = []
    errors: List[str] = []

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            headers: List[List[str]] = []
            for _ in range(HEADER_LINES_EXPECTED):
                try:
                    headers.append(next(reader))
                except StopIteration:
                    errors.append("file shorter than expected header length")
                    return FileResult(path, total_rows, valid_record_rows, invalid_records, anomalies, errors)

            header_row = headers[1] if len(headers) > 1 else []
            record_idx = find_column_index(header_row, ("RECORD", "RECNO", "RECORDNO", "RECNUM"))
            if record_idx is None:
                errors.append("missing RECORD column in header row")
                return FileResult(path, total_rows, valid_record_rows, invalid_records, anomalies, errors)

            timestamp_idx = find_column_index(header_row, ("TIMESTAMP", "TIME", "DATETIME"))

            prev_snapshot: Optional[RowSnapshot] = None
            header_line_count = len(headers)

            for data_idx, row in enumerate(reader, start=1):
                cleaned = [field.strip() for field in row]
                if not cleaned or all(field == "" for field in cleaned):
                    continue

                total_rows += 1
                line_number = header_line_count + data_idx
                record_value = None
                raw_record = ""
                if record_idx < len(cleaned):
                    raw_record = cleaned[record_idx]
                    record_value = parse_record_value(raw_record)
                else:
                    raw_record = "<missing>"

                timestamp_value = None
                if timestamp_idx is not None and timestamp_idx < len(cleaned):
                    candidate = cleaned[timestamp_idx]
                    timestamp_value = candidate if candidate else None

                snapshot = RowSnapshot(
                    line_number=line_number,
                    record=record_value,
                    timestamp=timestamp_value,
                    fields=cleaned,
                )

                if record_value is None:
                    reason = "unable to parse record value"
                    if raw_record == "<missing>":
                        reason = "record column missing"
                    invalid_records.append(InvalidRecord(line_number, raw_record, reason, cleaned))
                    prev_snapshot = None
                    continue

                valid_record_rows += 1

                if prev_snapshot is not None and prev_snapshot.record is not None:
                    diff = record_value - prev_snapshot.record
                    if diff != 1:
                        anomalies.append(Anomaly(path, diff, prev_snapshot, snapshot))

                prev_snapshot = snapshot

    except Exception as exc:  # pragma: no cover - safeguard for unexpected errors
        errors.append(f"exception while reading file: {exc}")

    return FileResult(path, total_rows, valid_record_rows, invalid_records, anomalies, errors)


def row_snapshot_to_dict(snapshot: RowSnapshot) -> dict:
    return {
        "line_number": snapshot.line_number,
        "record": snapshot.record,
        "timestamp": snapshot.timestamp,
        "fields": snapshot.fields,
    }


def invalid_record_to_dict(invalid: InvalidRecord) -> dict:
    return {
        "line_number": invalid.line_number,
        "raw_value": invalid.raw_value,
        "reason": invalid.reason,
        "fields": invalid.fields,
    }


def anomaly_to_dict(anomaly: Anomaly, repo_root: Path) -> dict:
    relative = anomaly.file_path.relative_to(repo_root)
    return {
        "file": str(relative),
        "difference": anomaly.difference,
        "row_above": row_snapshot_to_dict(anomaly.row_above),
        "row_below": row_snapshot_to_dict(anomaly.row_below),
    }


def file_result_to_dict(result: FileResult, repo_root: Path) -> dict:
    relative = result.path.relative_to(repo_root)
    data = {
        "file": str(relative),
        "total_rows": result.total_rows,
        "valid_record_rows": result.valid_record_rows,
        "invalid_record_count": len(result.invalid_records),
        "anomaly_count": len(result.anomalies),
    }
    if result.invalid_records:
        data["invalid_records"] = [invalid_record_to_dict(inv) for inv in result.invalid_records]
    if result.errors:
        data["errors"] = result.errors
    return data


def main() -> None:
    script_path = Path(__file__).resolve()
    repo_root = script_path.parents[1]
    rawdata_dir = repo_root / "rawdata"
    if not rawdata_dir.exists():
        raise SystemExit(f"rawdata directory not found at {rawdata_dir}")

    output_path = repo_root / "record_number_outputs.toml"

    file_results: List[FileResult] = []
    anomalies: List[Anomaly] = []

    for file_path in sorted(iter_data_files(rawdata_dir)):
        result = analyze_file(file_path)
        file_results.append(result)
        anomalies.extend(result.anomalies)

    summary = {
        "files_scanned": len(file_results),
        "files_with_anomalies": sum(1 for res in file_results if res.anomalies),
        "total_rows": sum(res.total_rows for res in file_results),
        "valid_record_rows": sum(res.valid_record_rows for res in file_results),
        "invalid_record_rows": sum(len(res.invalid_records) for res in file_results),
        "anomaly_count": len(anomalies),
        "files_with_errors": sum(1 for res in file_results if res.errors),
    }

    output_doc = {
        "summary": summary,
        "files": [file_result_to_dict(res, repo_root) for res in file_results],
        "anomalies": [anomaly_to_dict(anom, repo_root) for anom in anomalies],
    }

    output_text = tomli_w.dumps(output_doc)
    output_path.write_text(output_text, encoding="utf-8")
    print(
        f"Processed {summary['files_scanned']} files; "
        f"found {summary['anomaly_count']} anomalies. Output -> {output_path}"
    )


if __name__ == "__main__":
    main()
