#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pandas",
#   "rich",
#   "tomli-w"
# ]
# ///

"""
Performs a comprehensive data quality audit by strictly cross-referencing
logger IDs from the filename, TOA5 header, and data column.

This updated version can distinguish between Legacy Single-Sensor formats and
the new CR300 Multi-Sensor format, applying the correct validation logic for each.

Generates:
1. `fixes.toml`: For automatically correctable data issues.
2. `quality_report.toml`: A comprehensive, human-readable audit log of all
   discovered discrepancies, warnings, and errors, with a configurable output path.
"""

import hashlib
import pandas as pd
import tomli_w
import argparse
import re
from pathlib import Path
from rich.console import Console
from rich.table import Table
import csv

console = Console()

FILENAME_ID_REGEX = re.compile(r'(?:CR200|CR300)Series_(\d+)')
HEADER_ID_REGEX = re.compile(r'Series_(\d+)')

# --- NEW: Schema Identification Logic ---

def identify_schema(file_path: Path) -> str | None:
    """
    Identifies the file schema by inspecting the header rows.
    Mirrors the logic from the Rust SchemaValidator trait.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            # Check TOA5 Header (Row 1)
            toa5_header = next(reader)
            if len(toa5_header) < 2 or toa5_header[0] != "TOA5":
                return "UNKNOWN" # Not a valid file we can process

            # Check Column Header (Row 2) for Multi-Sensor format
            column_headers = next(reader)
            
            # Define the expected preamble for the CR300 Multi-Sensor format
            preamble = ["TIMESTAMP", "RECORD", "Batt_volt", "PTemp_C"]
            fields_per_sensor = 20 # As per README
            
            is_multi_sensor = (
                "CR300" in toa5_header[1] and
                len(column_headers) > len(preamble) and
                column_headers[:len(preamble)] == preamble and
                (len(column_headers) - len(preamble)) % fields_per_sensor == 0
            )

            if is_multi_sensor:
                return "CR300MultiSensor"
            else:
                # If it's not the new format, assume it's the old one.
                # More specific legacy checks could be added here if needed.
                return "LegacySingleSensor"

    except (StopIteration, csv.Error):
        return "UNKNOWN" # File is too short or malformed
    return "UNKNOWN"


def get_id_from_header(file_path: Path) -> int | None:
    """Reads the first line of the file to get the logger ID from the TOA5 header."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        try:
            first_row = next(reader)
            if len(first_row) > 1:
                match = HEADER_ID_REGEX.search(first_row[1])
                if match:
                    return int(match.group(1))
        except (StopIteration, ValueError, csv.Error):
            return None
    return None

def find_id_column(file_path: Path) -> tuple[int | None, str | None]:
    """Finds the index and name of the logger ID column from the header row."""
    id_col_names = ['id', 'logger_id', 'loggerid']
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        try:
            next(reader)  # Skip TOA5
            headers = next(reader)
            for i, header in enumerate(headers):
                if header.strip().strip('"').lower() in id_col_names:
                    return i, header
        except (StopIteration, csv.Error):
            return None, None
    return None, None

def find_issues(data_dir: Path, exclude_patterns: list):
    """Scans the data directory and returns categorized lists for fixes, warnings, and errors."""
    fixes, warnings, errors = [], [], []

    console.print(f"[bold cyan]Scanning directory:[/] {data_dir.resolve()}")
    all_files = list(data_dir.rglob("*"))
    
    files_to_scan = all_files
    if exclude_patterns:
        files_to_scan = [f for f in all_files if not any(p.lower() in f.name.lower() for p in exclude_patterns)]
        excluded_count = len(all_files) - len(files_to_scan)
        if excluded_count > 0:
            console.print(f"Found {len(all_files)} total files. [yellow]Excluded {excluded_count}[/] based on patterns: {exclude_patterns}")

    console.print(f"Analyzing {len(files_to_scan)} files.")

    for i, file_path in enumerate(files_to_scan):
        console.print(f"  -> Processing file {i+1}/{len(files_to_scan)}: {file_path.name}", end="\r")
        
        try:
            content = file_path.read_bytes()
            file_hash = hashlib.sha256(content).hexdigest()

            # --- MODIFIED: Main validation logic branch ---
            schema_type = identify_schema(file_path)

            id_from_filename = int(re.search(FILENAME_ID_REGEX, file_path.name).group(1)) if re.search(FILENAME_ID_REGEX, file_path.name) else None
            id_from_header = get_id_from_header(file_path)

            if id_from_filename is not None and id_from_header is not None and id_from_filename != id_from_header:
                warnings.append({
                    "path": str(file_path.resolve()), "hash": file_hash,
                    "category": "METADATA_MISMATCH",
                    "reason": f"Filename ID ({id_from_filename}) conflicts with Header ID ({id_from_header})."
                })
                continue # Skip further checks on this file

            authoritative_id = id_from_header or id_from_filename
            if authoritative_id is None:
                warnings.append({"path": str(file_path.resolve()), "hash": file_hash, "category": "MISSING_METADATA", "reason": "Could not determine an authoritative ID from filename or header."})
                continue

            # --- Logic branch for LEGACY format ---
            if schema_type == "LegacySingleSensor":
                id_col_index, _ = find_id_column(file_path)
                
                # This logic is only relevant for the legacy format which has an ID column
                if id_col_index is not None:
                    df = pd.read_csv(file_path, header=None, skiprows=4, on_bad_lines='warn', engine ="python")
                    id_col_values = pd.to_numeric(df.iloc[:, id_col_index], errors='coerce').dropna().astype(int).unique()

                    if len(id_col_values) == 0:
                         fixes.append({"hash": file_hash, "action": "SET_LOGGER_ID", "value": authoritative_id, "description": f"Authoritative ID is {authoritative_id}, but data column exists and is empty/non-numeric.", "path": str(file_path.resolve())})
                    elif len(id_col_values) == 1:
                        if id_col_values[0] != authoritative_id:
                            fixes.append({"hash": file_hash, "action": "SET_LOGGER_ID", "value": authoritative_id, "description": f"Authoritative ID is {authoritative_id}, but data column contains {id_col_values[0]}.", "path": str(file_path.resolve())})
                    else:
                        warnings.append({"path": str(file_path.resolve()), "hash": file_hash, "category": "DATA_AMBIGUITY", "reason": f"Data column contains multiple IDs: {list(id_col_values)}."})
                else:
                    # If it's a legacy file, we expect an ID column.
                    warnings.append({"path": str(file_path.resolve()), "hash": file_hash, "category": "MISSING_DATA_COLUMN", "reason": f"File appears to be Legacy format but no 'id' column was found in the headers."})

            # --- Logic branch for MULTI-SENSOR format ---
            elif schema_type == "CR300MultiSensor":
                # For this format, our "validation" is simply confirming the schema was identified
                # and the header/filename IDs were consistent. There is no `id` column in the data
                # to check, so no fixes are generated. This is correct.
                # A success here is simply passing through without generating warnings/errors.
                pass
            
            else:
                warnings.append({"path": str(file_path.resolve()), "hash": file_hash, "category": "UNKNOWN_SCHEMA", "reason": "File did not match any known valid schema (Legacy or Multi-Sensor)."})


        except Exception as e:
            error_reason = str(e).strip()
            category = "PARSE_ERROR"
            if "Buffer overflow" in error_reason: category = "FILE_CORRUPTION"
            
            try:
                if 'content' not in locals(): content = file_path.read_bytes()
                file_hash = hashlib.sha256(content).hexdigest()
            except Exception: file_hash = "N/A - Could not read file"
            errors.append({"path": str(file_path.resolve()), "hash": file_hash, "category": category, "reason": error_reason})

    return fixes, warnings, errors

def print_results_tables(fixes: list, warnings: list, errors: list):
    """Prints all discovered issues to the console in formatted tables."""
    console.print("\n--- [bold yellow]Automatic Fixes Identified[/] ---")
    if fixes:
        fix_table = Table()
        fix_table.add_column("File Path", style="cyan")
        fix_table.add_column("Action", style="magenta")
        fix_table.add_column("Reason")
        for fix in fixes: fix_table.add_row(Path(fix["path"]).name, fix["action"], fix["description"])
        console.print(fix_table)
    else: console.print("[green]None found.[/]")
        
    console.print("\n--- [bold magenta]Warnings (Manual Inspection Required)[/] ---")
    if warnings:
        warn_table = Table()
        warn_table.add_column("File Path", style="cyan")
        warn_table.add_column("Category", style="yellow")
        warn_table.add_column("Reason")
        for warn in warnings: warn_table.add_row(Path(warn["path"]).name, warn["category"], warn["reason"])
        console.print(warn_table)
    else: console.print("[green]None found.[/]")

    console.print("\n--- [bold red]Errors (File Cannot Be Processed)[/] ---")
    if errors:
        err_table = Table()
        err_table.add_column("File Path", style="cyan")
        err_table.add_column("Category", style="red")
        err_table.add_column("Details")
        for err in errors: err_table.add_row(Path(err["path"]).name, err["category"], err["reason"])
        console.print(err_table)
    else: console.print("[green]None found.[/]")

def main():
    parser = argparse.ArgumentParser(description="Find data files needing manual fixes and quality issues.")
    parser.add_argument("data_dir", type=Path, help="Path to the raw data directory.")
    parser.add_argument("--dry-run", action="store_true", help="Print found issues to console without writing any files.")
    parser.add_argument("--exclude", nargs='*', default=['status', 'datatableinfo', 'public'], help="Case-insensitive patterns to exclude files.")
    parser.add_argument(
        "--report-file",
        type=Path,
        default=Path("quality_report.toml"),
        help="Path to write the human-readable quality report TOML file."
    )
    
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        console.print(f"[bold red]Error:[/] Directory not found at {args.data_dir}")
        return

    fixes, warnings, errors = find_issues(args.data_dir, args.exclude)

    console.print("\n\n[bold underline]Data Quality Scan Complete[/]")

    if args.dry_run:
        print_results_tables(fixes, warnings, errors)
        return

    if fixes:
        fixes_for_toml = [{k: v for k, v in f.items() if k != 'path'} for f in fixes]
        output_fixes_path = Path("initial_metadata/fixes.toml")
        output_fixes_path.parent.mkdir(exist_ok=True)
        with open(output_fixes_path, "wb") as f:
            tomli_w.dump({"fix": fixes_for_toml}, f)
        console.print(f"✅ [bold green]Wrote {len(fixes_for_toml)} automatic fix(es) to {output_fixes_path}.[/]")
    else:
        console.print("✅ [bold green]No automatic fixes needed.[/]")

    if warnings or errors:
        report_data = {'metadata_warnings': warnings, 'parse_errors': errors}
        report_path = args.report_file 
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "wb") as f:
            tomli_w.dump(report_data, f)
        console.print(f"⚠️ [bold yellow]Wrote a quality report with {len(warnings)} warning(s) and {len(errors)} error(s) to {report_path} for manual review.[/]")
    else:
        console.print("✅ [bold green]No warnings or errors found.[/]")

if __name__ == "__main__":
    main()