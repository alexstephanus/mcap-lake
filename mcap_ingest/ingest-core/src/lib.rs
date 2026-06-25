//! Core logic that handles the flow of data:
//! 1. MCAP Read
//! 2. Proto decode
//! 3. Arrow encode
//! 4. Parquet write
//! 5. Iceberg commit


pub mod arrow_encode;
pub mod arrow_schema;
pub mod iceberg_commit;
pub mod mcap_reader;
pub mod parquet;
pub mod storage;
pub mod topics;
