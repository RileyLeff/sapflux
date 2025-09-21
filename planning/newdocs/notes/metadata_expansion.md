# Dynamic Metadata Column Expansion
Principle: The processing pipeline must dynamically handle user-defined metadata provided in the installation_metadata JSONB field of the deployments table.
Explanation: This feature allows data managers to add rich, custom metadata over time without requiring code changes. The metadata_enricher component of a pipeline is responsible for inspecting the installation_metadata of all relevant deployments for a given run, creating new columns for all unique keys found, and populating them. Deployments that do not have a value for a specific key will receive a null value in that column.
Example:
Consider two deployments in a single processing run:
Deployment A has installation_metadata: {"probe_azimuth": 180, "bark_thickness_mm": 10}
Deployment B has installation_metadata: {"probe_azimuth": 270, "notes": "Shaded location"}
The metadata_enricher will add three new columns to the final output DataFrame: probe_azimuth, bark_thickness_mm, and notes.
Data rows from Deployment A will have values 180, 10, and null.
Data rows from Deployment B will have values 270, null, and "Shaded location".