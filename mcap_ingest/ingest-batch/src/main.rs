//! Batch ingest: read every MCAP in the `mcap` bucket and commit it to Iceberg.

use std::env;
use std::time::Instant;

use ingest_core::iceberg_commit::commit_mcap;
use ingest_core::mcap_reader::{decode_mcap, load_mcap_bytes};
use ingest_core::parquet::{log_date_from_path, mcap_id_from_path};
use ingest_core::storage::{StorageConfig, list_all};
use ingest_core::topics::group_by_base_topic;

const BYTES_PER_MB: f64 = (1024 * 1024) as f64;

fn mbps(mb: f64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        mb / seconds
    } else {
        0.0
    }
}

#[derive(Default)]
struct IngestStats {
    n_mcaps: usize,
    mcap_bytes: u64,
    parquet_bytes: u64,
    read_seconds: f64,
    transform_seconds: f64,
    write_seconds: f64,
}

impl IngestStats {
    fn record(&mut self, mcap_bytes: u64, parquet_bytes: u64, read: f64, transform: f64, write: f64) {
        self.n_mcaps += 1;
        self.mcap_bytes += mcap_bytes;
        self.parquet_bytes += parquet_bytes;
        self.read_seconds += read;
        self.transform_seconds += transform;
        self.write_seconds += write;
    }

    fn log_summary(&self) {
        if self.n_mcaps == 0 {
            println!("ingestion finished: no mcaps to ingest");
            return;
        }
        let mcap_mb = self.mcap_bytes as f64 / BYTES_PER_MB;
        let parquet_mb = self.parquet_bytes as f64 / BYTES_PER_MB;
        let ratio = self.parquet_bytes as f64 / self.mcap_bytes as f64;
        let total = self.read_seconds + self.transform_seconds + self.write_seconds;
        println!("ingestion finished: {} mcap(s) ingested", self.n_mcaps);
        println!(
            "transform: {:.1} MB/s per core ({mcap_mb:.1} MB mcap in {:.1}s)",
            mbps(mcap_mb, self.transform_seconds),
            self.transform_seconds,
        );
        println!(
            "  i/o: read {:.1} MB/s, write {:.1} MB/s | {ratio:.2}x size, wall total {total:.1}s",
            mbps(mcap_mb, self.read_seconds),
            mbps(parquet_mb, self.write_seconds),
        );
    }
}

#[tokio::main]
async fn main() {
    let catalog_uri =
        env::var("ICEBERG_CATALOG_URI").unwrap_or_else(|_| "http://localhost:8181".to_string());
    let warehouse =
        env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "s3://warehouse".to_string());
    let namespace = env::var("ICEBERG_NAMESPACE").unwrap_or_else(|_| "mcap".to_string());

    let storage = StorageConfig::from_env();
    let mcap_store = storage.build_store("mcap");
    let catalog = storage.build_catalog(&catalog_uri, &warehouse).await;

    let objects = list_all(mcap_store.as_ref(), None).await;
    let mcaps: Vec<_> = objects
        .into_iter()
        .filter(|o| o.location.filename().is_some_and(|f| f.ends_with(".mcap")))
        .collect();
    println!("found {} mcap objects in bucket", mcaps.len());

    let mut stats = IngestStats::default();
    for obj in &mcaps {
        let mcap_id = mcap_id_from_path(&obj.location);
        let log_date = log_date_from_path(&obj.location);
        println!("ingesting {} ({} bytes)", obj.location, obj.size);

        let read_start = Instant::now();
        let mcap_bytes = load_mcap_bytes(mcap_store.as_ref(), &obj.location).await;
        let read_seconds = read_start.elapsed().as_secs_f64();

        let decode_start = Instant::now();
        let messages = decode_mcap(&mcap_bytes);
        let groups = group_by_base_topic(messages);
        let n_topics = groups.len();
        let decode_seconds = decode_start.elapsed().as_secs_f64();

        let commit = commit_mcap(&catalog, &namespace, &mcap_id, log_date, groups).await;
        let transform_seconds = decode_seconds + commit.transform_seconds;
        let write_seconds = commit.write_seconds;
        let total = read_seconds + transform_seconds + write_seconds;

        let mcap_mb = obj.size as f64 / BYTES_PER_MB;
        let parquet_mb = commit.parquet_bytes as f64 / BYTES_PER_MB;
        let ratio = commit.parquet_bytes as f64 / obj.size as f64;
        println!(
            "  ingested mcap_id={mcap_id}: {mcap_mb:.1} MB mcap -> {parquet_mb:.1} MB parquet, {ratio:.2}x | \
             transform {:.1} MB/s | read {:.1}, write {:.1} MB/s | total {total:.1}s ({n_topics} topics)",
            mbps(mcap_mb, transform_seconds),
            mbps(mcap_mb, read_seconds),
            mbps(parquet_mb, write_seconds),
        );
        stats.record(
            obj.size,
            commit.parquet_bytes,
            read_seconds,
            transform_seconds,
            write_seconds,
        );
    }

    stats.log_summary();
}
