use kafka_common::Uuid;
use kafka_server_common::{GroupTopicPartitionData, PartitionData, PartitionDataBuilder, TopicData};
use rstest::rstest;

// --- PartitionData tests ---

#[test]
fn test_partition_data_new() {
    let batches = vec![];
    let pd = PartitionData::new(1, 2, 100, 3, 0, "error".to_string(), 4, batches);
    assert_eq!(pd.partition(), 1);
    assert_eq!(pd.state_epoch(), 2);
    assert_eq!(pd.start_offset(), 100);
    assert_eq!(pd.delivery_complete_count(), 3);
    assert_eq!(pd.error_code(), 0);
    assert_eq!(pd.error_message(), "error");
    assert_eq!(pd.leader_epoch(), 4);
}

#[test]
fn test_partition_data_builder() {
    let pd = PartitionData::builder()
        .partition(5)
        .state_epoch(7)
        .start_offset(1000)
        .delivery_complete_count(2)
        .error_code(42)
        .error_message("test error".to_string())
        .leader_epoch(9)
        .state_batches(vec![])
        .build();
    assert_eq!(pd.partition(), 5);
    assert_eq!(pd.state_epoch(), 7);
    assert_eq!(pd.start_offset(), 1000);
    assert_eq!(pd.delivery_complete_count(), 2);
    assert_eq!(pd.error_code(), 42);
    assert_eq!(pd.error_message(), "test error");
    assert_eq!(pd.leader_epoch(), 9);
}

#[test]
fn test_partition_data_equality() {
    let a = PartitionData::new(1, 2, 100, 3, 0, "err".to_string(), 4, vec![]);
    let b = PartitionData::new(1, 2, 100, 3, 0, "err".to_string(), 4, vec![]);
    assert_eq!(a, b);

    let c = PartitionData::new(1, 2, 100, 3, 0, "err".to_string(), 5, vec![]);
    assert_ne!(a, c);
}

#[rstest]
#[case(0, "no error")]
#[case(1, "mock error")]
#[case(42, "some error")]
#[case(-1, "internal error")]
fn test_partition_data_error_message(#[case] code: i16, #[case] msg: &str) {
    let pd = PartitionData::builder()
        .error_code(code)
        .error_message(msg.to_string())
        .build();
    assert_eq!(pd.error_code(), code);
    assert_eq!(pd.error_message(), msg);
}

// --- TopicData tests ---

#[test]
fn test_topic_data_new() {
    let td: TopicData<i32> = TopicData::new(Uuid::new(1, 2), vec![10, 20, 30]);
    assert_eq!(td.topic_id(), Uuid::new(1, 2));
    assert_eq!(td.partitions().len(), 3);
}

#[test]
fn test_topic_data_empty() {
    let td: TopicData<i32> = TopicData::new(Uuid::new(0, 0), vec![]);
    assert!(td.partitions().is_empty());
}

#[test]
fn test_topic_data_default() {
    let td: TopicData<i32> = TopicData::default();
    assert_eq!(td.topic_id(), Uuid::new(0, 0));
    assert!(td.partitions().is_empty());
}

#[test]
fn test_topic_data_equality() {
    let td1: TopicData<i32> = TopicData::new(Uuid::new(1, 2), vec![1, 2, 3]);
    let td2: TopicData<i32> = TopicData::new(Uuid::new(1, 2), vec![1, 2, 3]);
    assert_eq!(td1, td2);

    let td3: TopicData<i32> = TopicData::new(Uuid::new(3, 4), vec![1, 2, 3]);
    assert_ne!(td1, td3);
}

// --- GroupTopicPartitionData tests ---

#[test]
fn test_group_topic_partition_data_new() {
    let topics = vec![
        TopicData::new(Uuid::new(1, 2), vec![]),
        TopicData::new(Uuid::new(3, 4), vec![]),
    ];
    let gtp: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new("test-group".to_string(), topics);
    assert_eq!(gtp.group_id(), "test-group");
    assert_eq!(gtp.topics_data().len(), 2);
}

#[test]
fn test_group_topic_partition_data_default() {
    let gtp: GroupTopicPartitionData<i32> = GroupTopicPartitionData::default();
    assert_eq!(gtp.group_id(), "");
    assert!(gtp.topics_data().is_empty());
}

#[test]
fn test_group_topic_partition_data_empty() {
    let gtp: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new("group".to_string(), vec![]);
    assert_eq!(gtp.group_id(), "group");
    assert!(gtp.topics_data().is_empty());
}

#[rstest]
#[case("group1")]
#[case("group2")]
#[case("")]
#[case("my-test-group")]
fn test_group_id(#[case] group_id: &str) {
    let gtp: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new(group_id.to_string(), vec![]);
    assert_eq!(gtp.group_id(), group_id);
}

#[test]
fn test_group_topic_partition_data_equality() {
    fn make_topics() -> Vec<TopicData<i32>> {
        vec![TopicData::new(Uuid::new(1, 2), vec![1, 2])]
    }
    let a: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new("g".to_string(), make_topics());
    let b: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new("g".to_string(), make_topics());
    assert_eq!(a, b);

    let c: GroupTopicPartitionData<i32> =
        GroupTopicPartitionData::new("other".to_string(), make_topics());
    assert_ne!(a, c);
}
