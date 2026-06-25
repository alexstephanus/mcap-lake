//! Decode protobuf-encoded MCAP files into `DynamicMessage`s.

use std::collections::HashMap;

use bytes::Bytes;
use object_store::{ObjectStore, path::Path};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};


pub struct DecodedMessage {
    pub topic: String,
    pub log_time: u64,
    pub publish_time: u64,
    pub sequence: u32,
    pub message: DynamicMessage,
}

/// Load a whole MCAP object into memory. Fine for these files (tens to ~100 MB); a
/// streaming reader would be needed for much larger ones.
pub async fn load_mcap_bytes(store: &dyn ObjectStore, path: &Path) -> Bytes {
    store
        .get(path)
        .await
        .expect("failed to get mcap object")
        .bytes()
        .await
        .expect("failed to read mcap body")
}


pub fn register_file_descriptor_set(pool: &mut DescriptorPool, bytes: &[u8]) {
    pool.decode_file_descriptor_set(bytes)
        .expect("failed to decode FileDescriptorSet from mcap schema record");
}

pub fn lookup_message(pool: &DescriptorPool, full_name: &str) -> MessageDescriptor {
    pool.get_message_by_name(full_name)
        .unwrap_or_else(|| panic!("message type {full_name} not found in descriptor pool"))
}


pub fn decode_mcap(bytes: &[u8]) -> impl Iterator<Item = DecodedMessage> + '_ {
    let mut pool = DescriptorPool::new();

    // Cache of topic -> message descriptor, so we only register each schema once.
    let mut topic_descriptors: HashMap<String, MessageDescriptor> = HashMap::new();

    let stream = mcap::MessageStream::new(bytes).expect("failed to open mcap stream");
    stream.map(move |record| {
        let msg = record.expect("failed to read mcap message");
        let channel = msg.channel.as_ref();
        let schema = channel
            .schema
            .as_ref()
            .expect("channel has no attached schema");
        assert_eq!(
            schema.encoding, "protobuf",
            "expected protobuf-encoded schema, got encoding={}",
            schema.encoding,
        );

        if !topic_descriptors.contains_key(&channel.topic) {
            // Multiple channels (e.g. sensor_baro_0 and sensor_baro_1) share a single proto
            // type, so we need to check if the schema is already registered from another instance
            let descriptor = match pool.get_message_by_name(&schema.name) {
                Some(descriptor) => descriptor,
                None => {
                    register_file_descriptor_set(&mut pool, schema.data.as_ref());
                    lookup_message(&pool, &schema.name)
                }
            };
            topic_descriptors.insert(channel.topic.clone(), descriptor);
        }
        let descriptor = topic_descriptors.get(&channel.topic).unwrap();
        let dynamic_msg = DynamicMessage::decode(descriptor.clone(), msg.data.as_ref())
            .expect("failed to decode protobuf message");

        DecodedMessage {
            topic: channel.topic.clone(),
            log_time: msg.log_time,
            publish_time: msg.publish_time,
            sequence: msg.sequence,
            message: dynamic_msg,
        }
    })
}
