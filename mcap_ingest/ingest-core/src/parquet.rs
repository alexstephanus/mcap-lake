//! Parquet write settings and small path helpers. The Iceberg writer does the actual
//! writing (see `iceberg_commit`); this module just supplies the `WriterProperties` it's
//! built with. (An earlier standalone object_store-based writer lived here but was
//! dropped once Iceberg owned the write path.)

use arrow::datatypes::Schema;
use chrono::NaiveDate;
use object_store::path::Path;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;

/// WriterProperties for an iceberg parquet data file: zstd compression and a
/// declared sort order on publish_time (we sort rows by publish_time before writing).
pub fn sorted_parquet_props(schema: &Schema) -> WriterProperties {
    let publish_time_idx = schema
        .index_of("publish_time")
        .expect("schema missing publish_time column");
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("Invalid zstd level"),
        ))
        .set_sorting_columns(Some(vec![SortingColumn {
            column_idx: publish_time_idx as i32,
            descending: false,
            nulls_first: false,
        }]))
        .build()
}

/// Derive the mcap_id (used as a column and in data-file names) from an object path:
/// the filename with the `.mcap` extension stripped.
pub fn mcap_id_from_path(path: &Path) -> String {
    path.filename()
        .expect("mcap path has no filename")
        .trim_end_matches(".mcap")
        .to_string()
}

/// Parse the flight's log date from the `YYYY-MM-DD` prefix of an object filename
/// (`{log_date}-{log_id}.mcap`, set by the downloader). This day-precision date is the
/// anchor for the table's absolute timestamps and the partition key — see `arrow_encode`.
pub fn log_date_from_path(path: &Path) -> NaiveDate {
    let filename = path.filename().expect("mcap path has no filename");
    let prefix = filename
        .get(..10)
        .unwrap_or_else(|| panic!("mcap filename '{filename}' is too short for a YYYY-MM-DD prefix"));
    NaiveDate::parse_from_str(prefix, "%Y-%m-%d")
        .unwrap_or_else(|e| panic!("mcap filename '{filename}' has no valid date prefix: {e}"))
}
