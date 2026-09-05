use kafka_common::uuid::Uuid;
use kafka_server_common::{OffsetAndEpoch, TopicIdPartition};
use rstest::rstest;

// --- OffsetAndEpoch tests ---

#[test]
fn test_offset_and_epoch_new() {
    let oae = OffsetAndEpoch::new(100, 5);
    assert_eq!(oae.offset(), 100);
    assert_eq!(oae.epoch(), 5);
}

#[test]
fn test_offset_and_epoch_default() {
    let oae = OffsetAndEpoch::default();
    assert_eq!(oae.offset(), 0);
    assert_eq!(oae.epoch(), 0);
}

#[rstest]
#[case(100, 5, 100, 5, std::cmp::Ordering::Equal)]
#[case(100, 5, 200, 5, std::cmp::Ordering::Less)]
#[case(200, 5, 100, 5, std::cmp::Ordering::Greater)]
#[case(100, 5, 100, 6, std::cmp::Ordering::Less)]
#[case(100, 6, 100, 5, std::cmp::Ordering::Greater)]
#[case(0, 0, 0, 0, std::cmp::Ordering::Equal)]
#[case(-1, 0, 0, 0, std::cmp::Ordering::Less)]
fn test_offset_and_epoch_ordering(
    #[case] off1: i64,
    #[case] ep1: i32,
    #[case] off2: i64,
    #[case] ep2: i32,
    #[case] expected: std::cmp::Ordering,
) {
    let a = OffsetAndEpoch::new(off1, ep1);
    let b = OffsetAndEpoch::new(off2, ep2);
    assert_eq!(a.cmp(&b), expected);
}

// --- TopicIdPartition tests ---

#[test]
fn test_topic_id_partition_new() {
    let uuid = Uuid::new(1, 2);
    let tip = TopicIdPartition::new(uuid, "test-topic".to_string(), 3);
    assert_eq!(tip.topic_id(), uuid);
    assert_eq!(tip.topic(), "test-topic");
    assert_eq!(tip.partition_id(), 3);
}

#[test]
fn test_topic_id_partition_new_without_name() {
    let uuid = Uuid::new(1, 2);
    let tip = TopicIdPartition::new_without_name(uuid, 3);
    assert_eq!(tip.topic_id(), uuid);
    assert_eq!(tip.topic(), "");
    assert_eq!(tip.partition_id(), 3);
}

#[test]
fn test_topic_id_partition_display() {
    let uuid = Uuid::new(0x123456789ABCDEF0, -1);
    let tip = TopicIdPartition::new_without_name(uuid, 0);
    let display = format!("{}", tip);
    // Java's UUID.toString() returns the base64url encoding, followed by ":0"
    assert!(display.ends_with(":0"));
}

#[test]
fn test_topic_id_partition_default() {
    let tip = TopicIdPartition::default();
    assert_eq!(tip.topic_id(), Uuid::new(0, 0));
    assert_eq!(tip.partition_id(), -1);
}

#[test]
fn test_topic_id_partition_equality() {
    let uuid = Uuid::new(1, 2);
    let a = TopicIdPartition::new(uuid, "topic".to_string(), 0);
    let b = TopicIdPartition::new(uuid, "topic".to_string(), 0);
    assert_eq!(a, b);

    let c = TopicIdPartition::new(uuid, "other".to_string(), 0);
    assert_ne!(a, c);
}
