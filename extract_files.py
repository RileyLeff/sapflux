#!/usr/bin/env python3
# /// script
# dependencies = [
#   "python-dotenv",
#   "psycopg2-binary"
# ]
# ///

"""
A command-line utility to extract raw data files from the sapflux PostgreSQL
database by providing one or more abbreviated SHA-256 hashes.

This script connects to the database specified in the .env file, finds the
full hash corresponding to each abbreviated hash, retrieves the raw file
content (BYTEA), and saves it to a local file.
"""

import os
import sys
import argparse
import psycopg2
from dotenv import load_dotenv

def extract_files(db_url, abbreviated_hashes):
    """Connects to the DB and extracts files for the given hashes."""
    conn = None
    try:
        print(" -> Connecting to the database...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        print("✅ Connection successful.\n")

        for abbr_hash in abbreviated_hashes:
            print(f"🔍 Searching for hash starting with: {abbr_hash}...")

            # Use a parameterized query to prevent SQL injection.
            # The LIKE operator with '%' is perfect for finding the full hash.
            query = "SELECT file_hash, file_content FROM raw_files WHERE file_hash LIKE %s;"
            
            # psycopg2 needs the argument as a tuple, even if there's only one.
            cur.execute(query, (f"{abbr_hash}%",))
            
            result = cur.fetchone()

            if result:
                full_hash, file_content = result
                # The filename will be descriptive, e.g., "extracted_7e2fb6ab.dat"
                output_filename = f"extracted_{abbr_hash}.dat"
                
                print(f"   -> Found full hash: {full_hash[:16]}...")
                
                # IMPORTANT: Open the file in binary write mode ('wb').
                with open(output_filename, 'wb') as f:
                    # psycopg2 returns BYTEA as a memoryview/bytes object, which is exactly what we need.
                    # No hex decoding is necessary when using a library.
                    f.write(file_content)
                
                print(f"   -> ✅ File saved to: '{output_filename}'")
            else:
                print(f"   -> ⚠️  Warning: No file found for hash starting with '{abbr_hash}'.")
            
            print("-" * 20)

    except psycopg2.OperationalError as e:
        print(f"\n❌ Database connection error: {e}", file=sys.stderr)
        print("   Is the Docker container running? Is DATABASE_URL correct in your .env file?", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}", file=sys.stderr)
        return 1
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
    return 0

def main():
    """Parses command-line arguments and runs the extraction process."""
    parser = argparse.ArgumentParser(
        description="Extract raw data files from the sapflux database.",
        epilog="Example: uv run extract_files.py 7e2fb6ab 518c5771"
    )
    parser.add_argument(
        "abbreviated_hashes",
        nargs='+',
        help="One or more abbreviated SHA-256 hashes to extract."
    )
    args = parser.parse_args()

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("❌ Error: DATABASE_URL not found. Please ensure it is set in your .env file.", file=sys.stderr)
        sys.exit(1)

    sys.exit(extract_files(db_url, args.abbreviated_hashes))


if __name__ == "__main__":
    main()