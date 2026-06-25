# Warehouse
Scripts for querying the DuckDB layer on top of the Iceberg tables that the `mcap_ingest` pipeline
writes to MinIO.  DuckDB is dockerized, and its image bakes in the `iceberg`, `httpfs`,
and `avro` extensions at build time so you can query offline once everything's built.

## Usage

To enter DuckDB in interactive query mode, either run `just query` or its associated command
from the `justfile` at the repository root.

The init script sets you up with `lake.mcap` as your schema, so you don't need to
reference db/schema names in queries (see `lake_init.sql` for more detail):

```sql
SHOW ALL TABLES;
DESCRIBE sensor_baro;
SELECT mcap_id,
    ROUND(MAX(-z), 1)             AS max_alt_m,
    MIN(log_time)                 AS started,
    MAX(log_time) - MIN(log_time) AS duration
  FROM vehicle_local_position
  GROUP BY mcap_id
  LIMIT 5;
```

Notes:
- `lake_init.sql` is mounted into the container, so you can tweak it without a full rebuild
