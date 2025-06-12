#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# ///

"""
Simulates the file discovery and filtering logic of the Rust `ingest` command.
It recursively finds all files in a directory and then separates them into two lists
based on a name-based filter.

This is a diagnostic tool to identify exactly which files would be processed
versus which would be filtered out by the ingest command.

Outputs:
  - files_to_be_processed.txt
  - filtered_out_files.txt
"""

import argparse
import sys
from pathlib import Path

def generate_lists(directory: Path, output_processed: Path, output_filtered: Path):
    """
    Finds all files and sorts them into two lists based on a keyword filter.
    """
    keywords = ["public", "status", "datatableinfo"]
    
    print(f"🔍 Scanning directory: {directory.resolve()}", file=sys.stderr)
    print(f"   Filtering for filenames containing: {keywords}\n", file=sys.stderr)

    # Use rglob("*") to find ALL files, just like the Rust ingest command.
    all_paths = [p for p in directory.rglob("*") if p.is_file()]

    files_to_process = []
    files_filtered_out = []

    for file_path in all_paths:
        filename_lower = file_path.name.lower()
        if any(keyword in filename_lower for keyword in keywords):
            files_filtered_out.append(file_path)
        else:
            files_to_process.append(file_path)

    # Write the lists to their respective output files
    with open(output_processed, 'w') as f:
        for path in files_to_process:
            f.write(f"{path}\n")

    with open(output_filtered, 'w') as f:
        for path in files_filtered_out:
            f.write(f"{path}\n")

    print(f"✅ Wrote {len(files_to_process)} paths to '{output_processed}'")
    print(f"✅ Wrote {len(files_filtered_out)} paths to '{output_filtered}'")


def main():
    parser = argparse.ArgumentParser(description="Simulate the ingest command's file filtering.")
    parser.add_argument("data_dir", type=Path, help="The path to the raw data directory.")
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        print(f"Error: Directory not found at '{args.data_dir}'", file=sys.stderr)
        sys.exit(1)

    output_processed_path = Path("files_to_be_processed.txt")
    output_filtered_path = Path("filtered_out_files.txt")
    
    generate_lists(args.data_dir, output_processed_path, output_filtered_path)

if __name__ == "__main__":
    main()