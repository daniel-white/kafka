use kafka_common::Node;
use rstest::rstest;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[test]
fn test_node_new() {
    let node = Node::new(1, "localhost".to_string(), 9092);
    assert_eq!(node.id(), 1);
    assert_eq!(node.host(), "localhost");
    assert_eq!(node.port(), 9092);
    assert_eq!(node.id_string(), "1");
    assert!(!node.is_fenced());
    assert!(!node.has_rack());
    assert_eq!(node.rack(), None);
}

#[test]
fn test_node_with_rack() {
    let node = Node::with_rack(1, "host".to_string(), 9092, Some("rack1".to_string()), true);
    assert!(node.has_rack());
    assert_eq!(node.rack(), Some("rack1"));
    assert!(node.is_fenced());
}

#[test]
fn test_node_no_node() {
    let node = Node::no_node();
    assert!(node.is_empty());
    assert_eq!(node.id(), -1);
    assert_eq!(node.port(), -1);
}

#[test]
fn test_node_is_empty() {
    let node = Node::new(-1, "".to_string(), -1);
    assert!(node.is_empty());

    let node2 = Node::new(1, "host".to_string(), 9092);
    assert!(!node2.is_empty());
}

#[test]
fn test_node_default() {
    assert_eq!(Node::default(), Node::no_node());
}

#[test]
fn test_node_display() {
    let node = Node::new(1, "localhost".to_string(), 9092);
    let s = format!("{}", node);
    assert!(s.contains("localhost:9092"));
    assert!(s.contains("id: 1"));
}

#[test]
fn test_node_equality() {
    let a = Node::new(1, "host".to_string(), 9092);
    let b = Node::new(1, "host".to_string(), 9092);
    assert_eq!(a, b);

    let c = Node::new(2, "host".to_string(), 9092);
    assert_ne!(a, c);
}

#[rstest]
#[case(1, "host", 9092, 1, "host", 9092, true)]
#[case(1, "host", 9092, 2, "host", 9092, false)]
#[case(1, "host1", 9092, 1, "host2", 9092, false)]
#[case(1, "host", 9092, 1, "host", 9093, false)]
#[case(1, "host", 9092, 1, "host", 9092, true)]
fn test_equality(
    #[case] id1: i32,
    #[case] host1: &str,
    #[case] port1: i32,
    #[case] id2: i32,
    #[case] host2: &str,
    #[case] port2: i32,
    #[case] expected: bool,
) {
    let a = Node::new(id1, host1.to_string(), port1);
    let b = Node::new(id2, host2.to_string(), port2);
    assert_eq!(a == b, expected);
}

#[rstest]
#[case(1, "host", 9092, Some("rack1".to_string()), true)]
#[case(1, "host", 9092, None, false)]
#[case(1, "host", 9092, Some("rack2".to_string()), false)]
fn test_hash_equality(
    #[case] id1: i32,
    #[case] host1: &str,
    #[case] port1: i32,
    #[case] rack1: Option<String>,
    #[case] same_as_first: bool,
) {
    let a = Node::with_rack(id1, host1.to_string(), port1, rack1.clone(), false);
    let b = Node::with_rack(1, "host".to_string(), 9092, Some("rack1".to_string()), false);

    let mut ha = DefaultHasher::new();
    let mut hb = DefaultHasher::new();
    a.hash(&mut ha);
    b.hash(&mut hb);

    assert_eq!(ha.finish() == hb.finish(), same_as_first);
}
