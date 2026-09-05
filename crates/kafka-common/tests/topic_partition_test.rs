use kafka_common::TopicPartition;
use rstest::rstest;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[test]
fn test_topic_partition_new() {
    let tp = TopicPartition::new("test-topic".to_string(), 3);
    assert_eq!(tp.topic(), "test-topic");
    assert_eq!(tp.partition(), 3);
}

#[test]
fn test_topic_partition_display() {
    let tp = TopicPartition::new("foo".to_string(), 0);
    assert_eq!(format!("{}", tp), "foo-0");
}

#[test]
fn test_topic_partition_default() {
    let tp = TopicPartition::default();
    assert_eq!(tp.topic(), "");
    assert_eq!(tp.partition(), -1);
}

#[rstest]
#[case("topic", 0, "topic", 0, true)]
#[case("topic", 0, "topic", 1, false)]
#[case("topic", 0, "other", 0, false)]
#[case("topic", -1, "topic", -1, true)]
#[case("", 0, "", 0, true)]
fn test_equality(
    #[case] topic1: &str,
    #[case] part1: i32,
    #[case] topic2: &str,
    #[case] part2: i32,
    #[case] expected: bool,
) {
    let a = TopicPartition::new(topic1.to_string(), part1);
    let b = TopicPartition::new(topic2.to_string(), part2);
    assert_eq!(a == b, expected);
}

#[rstest]
#[case("topic-a", 0, "topic-b", 0, false)]
#[case("topic-a", 1, "topic-a", 0, false)]
#[case("topic-a", 0, "topic-a", 0, true)]
fn test_hash_consistency(
    #[case] topic1: &str,
    #[case] part1: i32,
    #[case] topic2: &str,
    #[case] part2: i32,
    #[case] expected_eq: bool,
) {
    let a = TopicPartition::new(topic1.to_string(), part1);
    let b = TopicPartition::new(topic2.to_string(), part2);

    let mut hasher_a = DefaultHasher::new();
    let mut hasher_b = DefaultHasher::new();
    a.hash(&mut hasher_a);
    b.hash(&mut hasher_b);

    assert_eq!(hasher_a.finish() == hasher_b.finish(), expected_eq);
}

#[test]
fn test_hash_cached() {
    let tp = TopicPartition::new("test".to_string(), 5);
    let mut hasher1 = DefaultHasher::new();
    let mut hasher2 = DefaultHasher::new();
    tp.hash(&mut hasher1);
    tp.hash(&mut hasher2);
    assert_eq!(hasher1.finish(), hasher2.finish());
}
