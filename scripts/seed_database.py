# /// script
# dependencies = [
#   "toml",
#   "psycopg2-binary",
#   "python-dotenv",
# ]
# ///

"""
This script seeds the PostgreSQL database with initial metadata from the
`initial_metadata/` directory.

It is designed to be idempotent: it clears the target tables before
inserting new data, so it can be run safely multiple times to reset
the database to its initial state.

To run:
1. Make sure the `DATABASE_URL` is set in a `.env` file in the project root.
2. From the project root, execute: `uv run scripts/seed_database.py`
"""
import os
import toml
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from pathlib import Path

# --- Configuration ---
# Define the base path relative to the script's location
# This makes the script runnable from any directory.
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
METADATA_DIR = PROJECT_ROOT / "initial_metadata"


def seed_dst_transitions(conn):
    """Seeds the dst_transitions table from a TOML file."""
    print("--- Seeding dst_transitions ---")
    
    toml_path = METADATA_DIR / "dst_transitions.toml"
    if not toml_path.exists():
        print(f"ERROR: {toml_path} not found.")
        return

    with open(toml_path, "r") as f:
        data = toml.load(f)

    transitions = data.get("transitions", [])
    if not transitions:
        print("No transitions found in TOML file.")
        return

    with conn.cursor() as cursor:
        # Clear the table to ensure idempotency
        cursor.execute("TRUNCATE TABLE dst_transitions RESTART IDENTITY CASCADE;")
        print(f"Truncated dst_transitions table.")

        # Prepare data for efficient insertion
        values_to_insert = [
            (t['action'], t['ts_local']) for t in transitions
        ]

        # Use execute_values for a fast bulk insert
        extras.execute_values(
            cursor,
            "INSERT INTO dst_transitions (transition_action, ts_local) VALUES %s",
            values_to_insert
        )
        
        print(f"Successfully inserted {len(values_to_insert)} DST transitions.")


def main():
    """Main function to connect to the DB and run all seeders."""
    
    # Load environment variables from a .env file in the project root
    dotenv_path = PROJECT_ROOT / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
        print(f"Loaded environment variables from {dotenv_path}")
    else:
        print("Warning: .env file not found. Relying on shell environment.")

    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set.")
            
        with psycopg2.connect(db_url) as conn:
            print("Successfully connected to the database.")
            
            # --- Run all seeder functions here ---
            seed_dst_transitions(conn)
            # In the future, you would add:
            # seed_deployments(conn)
            # seed_sensors(conn)
            
            # We explicitly commit the transaction here.
            conn.commit()
            print("\nDatabase seeding complete.")

    except Exception as e:
        print(f"\nAN ERROR OCCURRED: {e}")
        # If there's an error, the 'with' block will handle closing the connection,
        # and no commit will happen, ensuring atomicity.

if __name__ == "__main__":
    main()