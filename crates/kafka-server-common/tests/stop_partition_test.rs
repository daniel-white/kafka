use kafka_common::uuid::Uuid;
use kafka_server_common::{StopPartition, TopicIdPartition};
use rstest::rstest;

fn make_tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::new(1, 2), "test".to_string(), 0)
}

#[test]
fn test_stop_partition_new() {
    let tp = make_tp();
    let sp = StopPartition::new(tp.clone(), true, false, true);
    assert_eq!(sp.topic_partition(), &tp);
    assert!(sp.delete_local_log());
    assert!(!sp.delete_remote_log());
    assert!(sp.stop_remote_log_metadata_manager());
}

#[test]
fn test_stop_partition_display() {
    let tp = make_tp();
    let sp = StopPartition::new(tp, true, false, true);
    let s = format!("{}", sp);
    assert!(s.contains("deleteLocalLog=true"));
    assert!(s.contains("deleteRemoteLog=false"));
    assert!(s.contains("stopRemoteLogMetadataManager=true"));
}

#[rstest]
#[case(true, false, true, true, false, true, true)]
#[case(false, true, false, true, false, true, false)]
#[case(true, true, true, true, true, true, true)]
#[case(false, false, false, true, true, true, false)]
fn test_equality(
    #[case] dl1: bool,
    #[case] dr1: bool,
    #[case] rr1: bool,
    #[case] dl2: bool,
    #[case] dr2: bool,
    #[case] rr2: bool,
    #[case] expected: bool,
) {
    let tp = make_tp();
    let a = StopPartition::new(tp.clone(), dl1, dr1, rr1);
    let b = StopPartition::new(tp, dl2, dr2, rr2);
    assert_eq!(a == b, expected);
}

#[test]
fn test_stop_partition_different_topic_different() {
    let tp1 = TopicIdPartition::new(Uuid::new(1, 2), "a".to_string(), 0);
    let tp2 = TopicIdPartition::new(Uuid::new(3, 4), "b".to_string(), 0);
    let a = StopPartition::new(tp1, true, true, true);
    let b = StopPartition::new(tp2, true, true, true);
    assert_ne!(a, b);
}
