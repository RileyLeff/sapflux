#!/usr/bin/env python3
# /// script
# dependencies = [
#   "polars",
#   "pyarrow"
# ]
# ///

"""
A command-line utility to inspect a Parquet file and display its
high-level statistics using the Polars library.

Provides information on shape, schema, null counts, and descriptive
statistics for numeric columns.
"""

import sys
import argparse
import polars as pl

def inspect_parquet_file(file_path: str) -> int:
    """
    Reads a Parquet file and prints a statistical summary.

    Args:
        file_path: The path to the Parquet file to inspect.

    Returns:
        0 on success, 1 on failure.
    """
    try:
        print(f"--- Inspecting Parquet File: '{file_path}' ---")
        
        # Read the Parquet file into a Polars DataFrame
        df = pl.read_parquet(file_path)

        # 1. Shape of the DataFrame
        print(f"\n📊 Shape: {df.shape[0]} rows, {df.shape[1]} columns")

        # 2. Schema (Column names and data types)
        print("\n📜 Schema:")
        # The schema object prints nicely by default
        print(df.schema)

        # 3. Head (first 5 rows)
        print("\nHEAD (First 5 Rows):")
        print(df.head())
        
        # 4. Tail (last 5 rows)
        print("\nTAIL (Last 5 Rows):")
        print(df.tail())

        # 5. Null Value Counts
        print("\n🗑️ Null Value Counts:")
        null_counts = df.null_count()
        # Only print the table if there are actually nulls to report
        if null_counts.sum(axis=1)[0] > 0:
            print(null_counts)
        else:
            print("   -> No null values found in any column. ✨")

        # 6. Descriptive Statistics for all numeric columns
        print("\n🔢 Descriptive Statistics (for numeric columns):")
        # .describe() returns a DataFrame, which is easy to read
        print(df.describe())

    except FileNotFoundError:
        print(f"\n❌ Error: File not found at '{file_path}'", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\n❌ An unexpected error occurred while processing the file: {e}", file=sys.stderr)
        return 1
        
    print("\n--- Inspection Complete ---")
    return 0

def main():
    """Parses command-line arguments and runs the inspection."""
    parser = argparse.ArgumentParser(
        description="Inspect a Parquet file and display high-level statistics.",
        epilog="Example: uv run inspect_parquet.py output.parquet"
    )
    parser.add_argument(
        "file_path",
        help="The path to the .parquet file to inspect."
    )
    args = parser.parse_args()

    sys.exit(inspect_parquet_file(args.file_path))


if __name__ == "__main__":
    main()