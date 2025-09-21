All timestamps are stored as TIMESTAMPTZ and are handled as UTC within the Rust application. One of the main goals of the processing pipeline (primarily the timestamp fixing step) is to get the timestamps out of an unknown, ambiguous time zone (either of EDT or EST) and into UTC.

Geospatial data uses GEOMETRY and requires PostGIS.
High-precision measurements should use NUMERIC to avoid floating-point inaccuracies.
