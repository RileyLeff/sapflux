#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "psycopg2-binary",
#   "python-dotenv"
# ]
# ///

"""
Reads a list of hashes from a file and checks for their existence in the
sapflux database's `raw_files` table.

This script is PEP 723 compliant and can be run with `uv run`. `uv` will
handle the installation of the dependencies listed above.

Usage:
  uv run check_hashes_in_db.py unwanted_hashes.txt
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

def read_hashes_from_file(file_path: Path) -> list[str]:
    """Reads hashes from the first column of a text file."""
    if not file_path.exists():
        print(f"❌ Error: Input file not found at '{file_path}'", file=sys.stderr)
        sys.exit(1)

    hashes = []
    with open(file_path, 'r') as f:
        for line in f:
            # Assumes the hash is the first thing on the line
            parts = line.strip().split()
            if parts:
                hashes.append(parts[0])
    return hashes

def check_hashes(db_url: str, hashes_to_check: list[str]):
    """Connects to the DB and checks for the existence of the given hashes."""
    if not hashes_to_check:
        print("No hashes to check. Exiting.", file=sys.stderr)
        return 0

    conn = None
    found_hashes = []
    try:
        print(" -> Connecting to the database...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        print("✅ Connection successful.\n")

        # This SQL query checks if any of the hashes in the provided list
        # exist in the file_hash column. It's very efficient.
        # The `%s` placeholder will be safely filled with the list of hashes.
        query = "SELECT file_hash FROM raw_files WHERE file_hash = ANY(%s);"

        # psycopg2 needs the argument as a tuple, even if there's only one.
        cur.execute(query, (hashes_to_check,))
        
        results = cur.fetchall()
        found_hashes = [row[0] for row in results]

    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}", file=sys.stderr)
        print("   Is the Docker container running? Is DATABASE_URL correct in your .env file?", file=sys.stderr)
        return 1
    finally:
        if conn:
            conn.close()

    print("--- Test Complete ---")
    if found_hashes:
        print(f"⚠️  TEST FAILED: {len(found_hashes)} unwanted file(s) were found in the database:")
        for h in found_hashes:
            print(f"  - {h}")
        return 1  # Exit with an error code to indicate failure
    else:
        print("✅ TEST PASSED: None of the unwanted file hashes were found in the database.")
        return 0

def main():
    parser = argparse.ArgumentParser(
        description="Check for hashes in the sapflux database."
    )
    parser.add_argument(
        "hashes_file",
        type=Path,
        help="Path to the text file containing hashes to check (one per line).",
    )
    args = parser.parse_args()

    # Load the .env file from the current directory
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ Error: DATABASE_URL not found. Please ensure it is set in your .env file.", file=sys.stderr)
        sys.exit(1)

    hashes = read_hashes_from_file(args.hashes_file)
    exit_code = check_hashes(db_url, hashes)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()