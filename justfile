# Core commands; run `just --list` for the full set.
default:
    @echo "mcap-lake — common commands (run 'just --list' for all):"
    @echo "  just run        full pipeline: download logs if needed -> convert -> ingest -> query"
    @echo "  just batch      convert + ingest only (no download/query)"
    @echo "  just query      interactive DuckDB on top of the lake"
    @echo "  just wipe-all   tear all docker infra down"

# Runs the batch pipeline (ulg -> mcap conversion + mcap -> iceberg ingest).
batch:
    docker compose --profile batch up -d --build minio-init iceberg-rest
    docker compose --profile batch up --build --no-deps ulog-converter
    docker compose --profile batch up --build --no-deps rust-ingest
# Infra + bucket-init come up detached first.  Then the two jobs run attached
# so `up` streams their logs live Each `up` returns when its job exits.  Infra
# is left up so `query`/`run` can read the lake immediately; `just down` to stop it.

# Stops the stack, keeping all volumes/data.
down:
    docker compose down

# Removes all docker infra: containers, networks, and named volumes (i.e. all data).
wipe-all:
    docker compose --profile "*" down -v --remove-orphans
# --profile "*" + --remove-orphans so the batch/query profile containers get torn down
# too; otherwise they can linger as orphans, holding the network open so it (and their
# volumes) can't be pruned.
# Images are left alone (see `uninstall` to also drop them). Good before rm'ing the repo, or
# to rerun fully clean. The mcap bucket is wiped too, forcing a re-conversion on the next run.

# Full uninstall, for getting the project off your machine fully: everything `wipe-all` does, plus
# ALL images (built ones AND the pulled minio/iceberg base images). Next run re-pulls and
# rebuilds from scratch. Host files (the data/raw log cache, etc.) are left untouched.
uninstall:
    docker compose --profile "*" down -v --remove-orphans --rmi all

# Starts up the Iceberg catalog
infra-up:
    docker compose up -d iceberg-rest

# Resets the lake (catalog + warehouse) for a clean mcap re-ingest, no ulog -> mcap re-conversion.
reset-lake: infra-up
    docker compose exec iceberg-rest sh -c 'rm -f /data/iceberg_rest.db*'
    docker compose restart iceberg-rest
    docker compose exec minio sh -c 'mc alias set root http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc rm --recursive --force root/warehouse'

# Starts up the iceberg catalog and drops you into an interactive DuckDB session 
query: infra-up
    docker compose --profile query run --rm duckdb

# Runs unit tests for ulog conversion and mcap ingest
test-ulog:
    uv run --directory ulog_conversion --group dev python -m pytest

# Runs unit tests for mcap ingest
test-ingest:
    cargo test --manifest-path mcap_ingest/Cargo.toml

test: test-ulog test-ingest

# Download logs into data/raw. To respect DroneCode's servers, it will only download up to 25 logs.
download-logs:
    docker compose --profile download run --build --rm ulog-download

# Runs the pipeline end-to-end.  Downloads .ulgs if necessary, converts to mcap,
# ingests mcap into iceberg, starts an interactive query session 
run: download-logs batch query
