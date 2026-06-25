# Warehouse
Scripts for querying the DuckDB layer on top of the Iceberg tables that the `mcap_ingest` pipeline
writes to MinIO.  DuckDB is dockerized, and its image bakes in the `iceberg`, `httpfs`,
and `avro` extensions at build time so you can query offline once everything's built.

## Usage

To enter DuckDB in interactive query mode, either run `just query` or its associated command
from the `justfile` at the repository root.

The init script (mounted into the DuckDB container) sets you up with `lake.mcap` and `memory.curated`
in your search_path, so you don't need to reference db/schema names in queries
(see `lake_init.sql` for more detail).  You still can, but it's unnecessary:

```sql
SHOW ALL TABLES;
SELECT * FROM curated.flights;
DESCRIBE mcap.sensor_baro;
SELECT
    mcap_id,
    ROUND(MAX(-z), 1)                                AS max_alt_m,
    MIN(log_time)                                    AS started,
    format_duration(MAX(log_time) - MIN(log_time))   AS duration
  FROM vehicle_local_position
  GROUP BY mcap_id;
```
