#!/usr/bin/env python3
# PEP 723 Compliant
# /// script
# dependencies = [
#   "python-dotenv",
#   "psycopg2-binary",
#   "toml"
# ]
# ///

import os
import toml
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
DB_URL = os.getenv("DATABASE_URL")
PROJECTS_FILE = "initial_metadata/projects.toml"
SENSORS_FILE = "initial_metadata/sensors.toml"
PARAMETERS_FILE = "initial_metadata/parameters.toml"

def seed_projects(cur):
    """Seeds the projects table from the TOML file."""
    print("Seeding projects...")
    with open(PROJECTS_FILE, 'r') as f:
        data = toml.load(f)

    project_list = data.get('project', [])
    if not project_list:
        print("  -> No projects found to seed.")
        return

    cur.execute("TRUNCATE TABLE projects RESTART IDENTITY CASCADE;")
    for project in project_list:
        cur.execute(
            "INSERT INTO projects (name, description) VALUES (%s, %s)",
            (project['name'], project.get('description'))
        )
    # --- CORRECTED COUNTING ---
    print(f"  -> Seeded {len(project_list)} projects.")

def seed_sensors(cur):
    """Seeds the sensors table from the TOML file."""
    print("Seeding sensors...")
    with open(SENSORS_FILE, 'r') as f:
        data = toml.load(f)
    
    sensor_list = data.get('sensor', [])
    if not sensor_list:
        print("  -> No sensors found to seed.")
        return

    cur.execute("TRUNCATE TABLE sensors RESTART IDENTITY CASCADE;")
    for sensor in sensor_list:
        cur.execute(
            """
            INSERT INTO sensors (sensor_id, downstream_probe_distance_cm, upstream_probe_distance_cm, thermistor_depth_1_mm, thermistor_depth_2_mm)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                sensor['id'],
                sensor['downstream_probe_distance_cm'],
                sensor['upstream_probe_distance_cm'],
                sensor['thermistor_depth_1_mm'],
                sensor['thermistor_depth_2_mm']
            )
        )
    # --- CORRECTED COUNTING ---
    print(f"  -> Seeded {len(sensor_list)} sensors.")

def seed_parameters(cur):
    """Seeds the parameters table from the TOML file."""
    print("Seeding parameters...")
    with open(PARAMETERS_FILE, 'r') as f:
        data = toml.load(f)

    # --- THIS IS THE MAIN FIX ---
    # We now correctly look inside the [parameters] table.
    parameters_dict = data.get('parameters', {})
    if not parameters_dict:
        print("  -> No parameters found to seed.")
        return

    cur.execute("TRUNCATE TABLE parameters RESTART IDENTITY CASCADE;")
    for name, params in parameters_dict.items():
        cur.execute(
            "INSERT INTO parameters (name, value, unit, description) VALUES (%s, %s, %s, %s)",
            (name, params['value'], params.get('unit'), params.get('description'))
        )
    # --- CORRECTED COUNTING ---
    print(f"  -> Seeded {len(parameters_dict)} parameters.")

def main():
    """Main function to connect to DB and run all seeder functions."""
    conn = None
    try:
        print(f"Connecting to database...")
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        seed_projects(cur)
        seed_sensors(cur)
        seed_parameters(cur)
        
        conn.commit()
        print("\n✅ Database seeding completed successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while seeding database: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()