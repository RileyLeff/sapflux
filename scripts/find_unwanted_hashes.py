#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# ///

"""
Recursively scans a directory for files that contain "public" or "status"
in their name (case-insensitive) and calculates the SHA-256 hash for each.

This script is PEP 723 compliant and can be run with `uv run`.

Usage:
  uv run find_unwanted_hashes.py /path/to/your/data > unwanted_hashes.txt
"""

import argparse
import hashlib
import sys
from pathlib import Path

def find_and_hash_unwanted_files(directory: Path):
    """
    Finds and hashes files with specified keywords in their names.

    Args:
        directory: The root directory to start scanning from.
    """
    keywords = ["public", "status", "datatableinfo"]
    found_count = 0

    print(f"🔍 Scanning directory: {directory.resolve()}", file=sys.stderr)
    print(f"   Filtering for filenames containing: {keywords}\n", file=sys.stderr)

    # Use rglob to search recursively for all files
    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue

        filename_lower = file_path.name.lower()
        if any(keyword in filename_lower for keyword in keywords):
            try:
                # Read file in binary mode for hashing
                content = file_path.read_bytes()
                file_hash = hashlib.sha256(content).hexdigest()
                
                # Print the hash and the file path, separated by a space
                print(f"{file_hash} {file_path}")
                
                found_count += 1
            except IOError as e:
                print(f"Error reading {file_path}: {e}", file=sys.stderr)

    print(f"\n✅ Found and hashed {found_count} matching files.", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description="Find and hash files with 'public', 'status', or 'datatableinfo' in their names."
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="The path to the raw data directory to scan.",
    )
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        print(f"Error: Directory not found at '{args.data_dir}'", file=sys.stderr)
        sys.exit(1)

    find_and_hash_unwanted_files(args.data_dir)

if __name__ == "__main__":
    main()