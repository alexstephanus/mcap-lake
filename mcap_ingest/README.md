# MCAP Ingest

This package contains all the logic required to read in MCAP files from an S3 bucket,
write out all the data messages into topic-specific `.parquet` files, and commit those
parquet files to Iceberg.

- Core logic (object storage interaction, Arrow/parquet writes, Iceberg commits) is contained in the
`ingest-core` crate.
- Batch processing is handled in the `ingest-batch` crate.

## Limitations

1. Because this is a demo pipeline, graceful handling of failures is not a priority.  Because it's running all-locally, transient errors aren't really expected to show up, and any error that does pop up probably indicates a bug or wrong assumption.  `ingest-core` makes heavy use of `.expect()` for this reason.
2. We don't have a great solution for fields which change their type across firmware versions (e.g. a boolean field being changed to an Int64).  For now, if there's a new field type that emerges for a particular field we simply create a new, type-postfixed field for that new type (e.g. `ram_usage__as_i64`).  In general, this is something that should be flagged and handled on a case-by-case basis.
