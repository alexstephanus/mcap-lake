use ingest_core::storage::{StorageConfig, list_all};

#[tokio::test]
#[ignore]
async fn lists_mcap_bucket() {
    let store = StorageConfig::from_env().build_store("mcap");
    let objects = list_all(store.as_ref(), None).await;
    println!("found {} objects in mcap bucket", objects.len());
}
