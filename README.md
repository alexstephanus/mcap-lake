# mcap-lake

This project takes a batch of PX4 drone autopilot ULog files (`.ulg`),
converts them to MCAP, and ingests those mcaps into an Iceberg
data warehouse for easy querying across multiple flights.

```sql
SELECT mcap_id,
  ROUND(MAX(-z), 1)             AS max_altitude_m,
  MIN(log_time)                 AS started,
  MAX(log_time) - MIN(log_time) AS duration
FROM vehicle_local_position
GROUP BY mcap_id
LIMIT 5;
┌─────────────────────────────────────────────────┬────────────────┬───────────────────────────────┬─────────────────┐
│                     mcap_id                     │ max_altitude_m │            started            │    duration     │
│                     varchar                     │     double     │   timestamp with time zone    │    interval     │
├─────────────────────────────────────────────────┼────────────────┼───────────────────────────────┼─────────────────┤
│ 2026-06-21-2e3f64ca-55ed-4384-8e79-06f693477f22 │          129.5 │ 2026-06-21 00:00:06.34+00     │ 00:00:17.702    │
│ 2026-06-21-063200f1-c4c0-4269-9c72-b003af3c8f26 │           18.2 │ 2026-06-21 00:30:00.768969+00 │ 00:03:57.808173 │
│ 2026-06-21-ed1ff772-0d73-4b54-b8a6-f8874270053e │            0.2 │ 2026-06-21 00:00:01.121119+00 │ 00:12:22.996964 │
│ 2026-06-21-3272e4ca-7596-4642-b011-de84bb1feee3 │            9.7 │ 2026-06-21 00:00:29.559184+00 │ 00:00:25.41535  │
│ 2026-06-21-32e7232b-be06-4b19-bea1-47f0abe927e8 │            3.2 │ 2026-06-21 00:06:21.019497+00 │ 00:00:35.896565 │
└─────────────────────────────────────────────────┴────────────────┴───────────────────────────────┴─────────────────┘

```

## Requirements

[Docker](https://docs.docker.com/get-started/get-docker/) is required
to coordinate all the local infrastructure.
Additionally, this project uses [just](https://github.com/casey/just) as a command runner.
If you don't want to install `just` (even though it's awesome), the recipes are
plain shell commands and you can copy-paste them from the `justfile` straight into your terminal.

## Running the pipeline

To run the full pipeline end-to-end, use:
```sh
just run
```

This runs the following commands in sequence:

1. `just download-logs` downloads 25 ULogs from the publicly-available PX4 flight database
2. `just batch` runs the `.ulg` -> `.mcap` conversion and the `.mcap` -> Iceberg ingestion
3. `just query` drops you into an interactive DuckDB session pointed at the ingested mcap data

## Architecture
The `.ulg` -> `.mcap` conversion step is written in Python due to the convenience that `pyulog` offers for interacting with ULog files.
The MCAP ingestion into Iceberg is written in Rust, which is a non-standard
choice for a pipeline like this*, but I like the language and wrote this solo.
DuckDB is a locally-hostable DB that provides
a convenient SQL interface to query against an Iceberg catalog.
MinIO is a locally-hosted, S3-compatible object storage service,
which makes the hypothetical "get this running on the cloud" step more straightforward.

All of the infrastructure is coordinated via `docker compose`.
Additionally, once you've downloaded ulog files and built all the docker images, 
no further network connection is required to run the end-to-end pipeline and explore the data.

\* In particular, the Rust Iceberg client doesn't have the same feature support as Java, Python, or Go.

## Decisions
**Why convert from ULog to MCAP?**

`.ulg` is (as far as I know) only used for PX4 flight logs, whereas MCAP is used much
more widely in robotics.  Additionally, the `mcap` CLI supports conversion from other
file formats off-the-shelf.  For instance, if there's some other source of `.bag`
files we start to care about, rather than now needing to write a `.bag` -> Iceberg
pipeline, it's easy to convert from `.bag` to `.mcap` and then feed those converted files
into the existing pipeline.

**Why Iceberg?**

Iceberg is definitely overkill for this demo pipeline's "ingest 25 ulogs" pattern,
but it is a great choice for robotics telemetry data in general
(plus, I wanted to familiarize myself with Iceberg).

Iceberg's primary downsides are:
1. It doesn't handle frequently-changing data in a great way, but given flight logs don't
   change once written this is effectively a non-issue. 
2. Operational complexity.  You need to bring your own (at a minimum) object storage, a catalog service,
   and a query engine.  If you want to do any sort of batch operations on data in the warehouse
   (e.g. spark jobs or dbt transforms to generate curated datasets) you also need some external compute cluster.
   If you're ingesting data frequently, you'll probably want to sort out periodic compaction
   for better query performance.  And if you're ever deleting data it needs to happen in both
   the catalog and the object storage layer to avoid orphaned files.
   A more managed data warehouse bundles all of that into one platform
   and can make life easier, with the downsides of higher cost and more tooling lock-in.
   With the amount of telemetry data that a fleet of robots can generate, the increased tooling
   flexibility and lower overall cost can certainly justify the increased operational complexity,
   although it's still a trade-off you should make with eyes wide open.

Most of these operational problems simply don't emerge for a demo pipeline.


## Structure, Roadmap, & Limitations
Each section has its own, more-specific README:
```
mcap-lake/
├── data/                 # Download script and raw ulogs
├── mcap_ingest/          # Ingestion of mcap files into iceberg
├── ulog_conversion/      # Conversion of ulog files into mcaps
└── warehouse/            # Init scripts and dockerfile for DuckDB
```

Roadmap:
1. The project only runs in batch mode.  I'd like to add a
worker mode, where rather than simply running a batch ingest and stopping,
the conversion & ingestion steps stick around as long-running processes and use
object notifications to convert and ingest new ulogs as soon as they land
2. I'd like to add an option to the mcap ingest to drop files
into foxglove as they're being processed, maybe on some sort of programmable trigger.
3. This is all single-threaded, so only one file is ever being worked on at once.  The pipeline obviously parallelizes across different files, so an option to fan out work to multiple cores would be great.


Non-Goals -- It's important to establish what you _don't_ care about!:
1. This project, as it's not a long-running warehouse, doesn't do much in the way of iceberg maintenance.
No migrations, compaction, monitoring query patterns and tweaking partitions, data lifecycle management, scheduled jobs to create canned datasets, etc.  This is not anticipated to change.
2. It doesn't do (and there are no plans to do) any sort of streaming reads during ingestion.
Pyulog eagerly loads the entire `.ulg` into memory, so unless I want to submit a large patch to that
library to support streaming reads this is locked in.
Flight logs are typically under 100MB and right now the pipeline only handles a single file at once,
so this doesn't cause any problems on a normal PC.
3. The ingest step only handles proto-encoded MCAP files.  This is not expected to change.
4. Security & credential management.  Since this all runs locally, everything uses admin/password as creds.

## Uninstall
To remove everything, run `just uninstall` to tear all the docker infrastructure down and then delete this repository.  `just wipe-all` tears down containers, networks, and volumes to run the pipeline from scratch, but it does not touch images or the downloaded ULog files in `data/raw`.

## License
This project is licensed under the MIT license, with the exception of `data/download_logs.py` (BSD-3), and the ulog data itself (CC-BY 4.0), both provided by PX4 under those licenses.  See `data/LICENSE.md` for more details.