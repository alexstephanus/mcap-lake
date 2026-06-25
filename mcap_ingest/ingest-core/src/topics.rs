//! Group decoded messages into per-base-topic batches.
//!
//! PX4 multi-instance topics are named `<base>_<multi_id>` (e.g. `sensor_baro_0`,
//! `sensor_baro_1`). In the lake they share one schema and belong in one table — so we strip
//! the instance suffix into a `multi_id` column and group every instance under `<base>`.

use std::collections::HashMap;

use prost_reflect::{ MessageDescriptor, ReflectMessage };

use crate::mcap_reader::DecodedMessage;

/// All rows for one base topic, plus the shared proto descriptor used to build columns.
pub struct TopicGroup {
    pub base_topic: String,
    pub descriptor: MessageDescriptor,
    pub rows: Vec<TopicRow>,
}

pub struct TopicRow {
    pub log_time: u64,
    pub publish_time: u64,
    pub sequence: u32, 
    pub multi_id: u8,
    pub message: prost_reflect::DynamicMessage,
}

/// The return is (<topic name>, <multi_id>).
/// If it doesn't have one (topics that don't have multi_ids),
/// we just assign a multi_id of 0
pub fn parse_topic_name(topic: &str) -> (String, u8) {
    match topic.rsplit_once('_') {
        Some((base, suffix)) => match suffix.parse::<u8>() {
            Ok(multi_id) => (base.to_string(), multi_id),
            Err(_) => (topic.to_string(), 0),
        },
        None => (topic.to_string(), 0),
    }
}

pub fn group_by_base_topic(
    messages: impl IntoIterator<Item = DecodedMessage>,
) -> HashMap<String, TopicGroup> {
    let mut groups: HashMap<String, TopicGroup> = HashMap::new();

    for m in messages {
        let (base, multi_id) = parse_topic_name(&m.topic);
        let descriptor = m.message.descriptor();

        let row = TopicRow {
            log_time: m.log_time,
            publish_time: m.publish_time,
            sequence: m.sequence,
            multi_id,
            message: m.message,
        };

        match groups.get_mut(&base) {
            Some(group) => {
                assert_eq!(
                    group.descriptor.full_name(),
                    descriptor.full_name(),
                    "topic base {} has mismatched proto descriptors across instances",
                    base,
                );
                group.rows.push(row);
            }
            None => {
                groups.insert(
                    base.clone(),
                    TopicGroup {
                        base_topic: base,
                        descriptor,
                        rows: vec![row],
                    },
                );
            }
        }
    }
    groups
}
