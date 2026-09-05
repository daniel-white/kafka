use kafka_common::{Node, PartitionInfo};
use rstest::rstest;

fn make_node(id: i32, host: &str) -> Node {
    Node::new(id, host.to_string(), 9092)
}

#[test]
fn test_partition_info_new() {
    let leader = make_node(1, "leader");
    let replicas = vec![make_node(1, "r1"), make_node(2, "r2")];
    let isr = vec![make_node(1, "r1")];
    let pi = PartitionInfo::new(
        "test-topic".to_string(),
        0,
        Some(leader.clone()),
        replicas.clone(),
        isr,
    );
    assert_eq!(pi.topic(), "test-topic");
    assert_eq!(pi.partition(), 0);
    assert_eq!(pi.leader(), &Some(leader.clone()));
    assert_eq!(pi.replicas().len(), 2);
    assert_eq!(pi.in_sync_replicas().len(), 1);
    assert!(pi.offline_replicas().is_empty());
}

#[test]
fn test_partition_info_with_offline() {
    let leader = make_node(1, "leader");
    let replicas = vec![make_node(1, "r1")];
    let isr = vec![make_node(1, "r1")];
    let offline = vec![make_node(2, "r2")];
    let pi = PartitionInfo::with_offline(
        "topic".to_string(),
        1,
        Some(leader),
        replicas,
        isr,
        offline.clone(),
    );
    assert_eq!(pi.offline_replicas().len(), 1);
    assert_eq!(pi.offline_replicas()[0].id(), 2);
}

#[test]
fn test_partition_info_no_leader() {
    let pi = PartitionInfo::new("topic".to_string(), 0, None, vec![], vec![]);
    assert_eq!(pi.leader(), &None);
}

#[test]
fn test_partition_info_default() {
    let pi = PartitionInfo::default();
    assert_eq!(pi.topic(), "");
    assert_eq!(pi.partition(), -1);
    assert_eq!(pi.leader(), &None);
}

#[test]
fn test_partition_info_display() {
    let leader = make_node(1, "leader");
    let replicas = vec![make_node(1, "r1"), make_node(2, "r2")];
    let pi = PartitionInfo::new(
        "my-topic".to_string(),
        3,
        Some(leader),
        replicas,
        vec![],
    );
    let s = format!("{}", pi);
    assert!(s.contains("topic = my-topic"));
    assert!(s.contains("partition = 3"));
    assert!(s.contains("leader = 1"));
    assert!(s.contains("replicas = [1,2]"));
    assert!(s.contains("isr = []"));
}

#[test]
fn test_partition_info_display_no_leader() {
    let pi = PartitionInfo::new("topic".to_string(), 0, None, vec![], vec![]);
    let s = format!("{}", pi);
    assert!(s.contains("leader = none"));
}

#[rstest]
#[case("topic-a", 0, "topic-a", 0, true)]
#[case("topic-a", 1, "topic-b", 0, false)]
#[case("topic-a", 0, "topic-a", 1, false)]
#[case("", 0, "", 0, true)]
fn test_equality(
    #[case] topic1: &str,
    #[case] part1: i32,
    #[case] topic2: &str,
    #[case] part2: i32,
    #[case] expected: bool,
) {
    let n = make_node(1, "host");
    let a = PartitionInfo::new(topic1.to_string(), part1, Some(n.clone()), vec![n.clone()], vec![n.clone()]);
    let b = PartitionInfo::new(topic2.to_string(), part2, Some(n.clone()), vec![n.clone()], vec![n]);
    assert_eq!(a == b, expected);
}
