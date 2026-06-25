use std::collections::{BTreeMap, HashSet};

use ingest_core::mcap_reader::{decode_mcap, load_mcap_bytes};
use prost_reflect::ReflectMessage;
use ingest_core::storage::{StorageConfig, list_all};

#[tokio::test]
#[ignore]
async fn decodes_first_mcap_in_bucket() {
    let store = StorageConfig::from_env().build_store("mcap");
    let objects = list_all(store.as_ref(), None).await;
    assert!(
        !objects.is_empty(),
        "no MCAPs in mcap bucket — populate it first",
    );

    let first = &objects[0];
    println!("decoding {} ({} bytes)", first.location, first.size);

    let bytes = load_mcap_bytes(store.as_ref(), &first.location).await;
    let messages: Vec<_> = decode_mcap(&bytes).collect();

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for m in &messages {
        *counts.entry(m.topic.as_str()).or_insert(0) += 1;
    }

    println!(
        "decoded {} messages across {} topics",
        messages.len(),
        counts.len(),
    );
    for (topic, count) in &counts {
        println!("  {topic}: {count}");
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for m in &messages {
        if seen.insert(m.topic.as_str()) {
            let fields: Vec<String> = m
                .message
                .descriptor()
                .fields()
                .map(|f| f.name().to_string())
                .collect();
            println!("  {} fields: {:?}", m.topic, fields);
        }
    }
}
