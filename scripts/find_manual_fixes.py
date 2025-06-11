#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pandas",
#   "rich",
#   "tomli-w"
# ]
# ///

"""
Scans a directory of raw sap flux data files to identify specific, known data
issues that require manual correction.

It calculates the SHA-256 hash of each problematic file and generates a
`fixes.toml` file that can be used by the Rust data pipeline to apply
targeted corrections.
"""

import hashlib
import pandas as pd
import tomli_w
import argparse
from pathlib import Path
from rich.console import Console

console = Console()

def find_fixes(data_dir: Path):
    """
    Scans the data directory and returns a list of dictionaries, where each
    dictionary represents a fix to be applied.
    """
    fixes_to_apply = []
    
    console.print(f"[bold cyan]Scanning directory:[/] {data_dir.resolve()}")

    # Use rglob to recursively find all .dat files
    files_to_scan = list(data_dir.rglob("*.dat"))
    console.print(f"Found {len(files_to_scan)} files to analyze.")

    for i, file_path in enumerate(files_to_scan):
        console.print(f"  -> Processing file {i+1}/{len(files_to_scan)}: {file_path.name}", end="\r")
        
        try:
            # Read the file content for hashing
            content = file_path.read_bytes()
            file_hash = hashlib.sha256(content).hexdigest()

            # Read the file with pandas for analysis
            df = pd.read_csv(
                file_path,
                header=None,
                skiprows=4,
                on_bad_lines='warn',
                # Give columns temporary integer names
                names=range(100)
            )

            # --- Define your fix conditions here ---

            # Condition 1: File related to logger 601 with NaN in the logger_id column
            # Assumes logger_id is in the 4th column (index 3)
            id_col = df.iloc[:, 3]
            if "601" in file_path.name and id_col.isnull().any():
                fix = {
                    "hash": file_hash,
                    "action": "SET_LOGGER_ID",
                    "value": 601,
                    "description": f"File '{file_path.name}' from logger 601 had a null/NaN logger_id."
                }
                fixes_to_apply.append(fix)
                console.print(f"\n[bold yellow]FOUND FIX:[/] Logger 601 NaN ID in {file_path.name}")

            # Condition 2: File related to logger 501 reporting ID as 1
            if "501" in file_path.name and (id_col == 1).any():
                fix = {
                    "hash": file_hash,
                    "action": "SET_LOGGER_ID",
                    "value": 501,
                    "description": f"File '{file_path.name}' from logger 501 incorrectly reported ID as 1."
                }
                fixes_to_apply.append(fix)
                console.print(f"\n[bold yellow]FOUND FIX:[/] Logger 501 wrong ID in {file_path.name}")
                
        except Exception as e:
            console.print(f"\n[bold red]ERROR:[/] Could not process {file_path.name}. Reason: {e}")

    return fixes_to_apply


def main():
    parser = argparse.ArgumentParser(description="Find data files needing manual fixes.")
    parser.add_argument("data_dir", type=Path, help="Path to the raw data directory.")
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        console.print(f"[bold red]Error:[/] Directory not found at {args.data_dir}")
        return

    fixes = find_fixes(args.data_dir)

    if not fixes:
        console.print("\n[bold green]✅ No files requiring manual fixes were found.[/]")
        return
    
    output_path = Path("initial_metadata/fixes.toml")
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, "wb") as f:
        tomli_w.dump({"fix": fixes}, f)

    console.print(f"\n[bold green]✅ Success! Wrote {len(fixes)} fix(es) to {output_path}.[/]")
    console.print("You can now run 'cargo run --release --bin sapflux-cli seed' to load them into the database.")


if __name__ == "__main__":
    main()