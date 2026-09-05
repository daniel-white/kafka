use kafka_common::{ClientIdAndBroker, IsolationLevel};
use rstest::rstest;

// --- IsolationLevel tests ---

#[rstest]
#[case(IsolationLevel::ReadUncommitted, 0)]
#[case(IsolationLevel::ReadCommitted, 1)]
fn test_id(#[case] level: IsolationLevel, #[case] expected: i8) {
    assert_eq!(level.id(), expected);
}

#[rstest]
#[case(0, IsolationLevel::ReadUncommitted)]
#[case(1, IsolationLevel::ReadCommitted)]
fn test_for_id(#[case] id: i8, #[case] expected: IsolationLevel) {
    assert_eq!(IsolationLevel::for_id(id), expected);
}

#[test]
fn test_default() {
    assert_eq!(IsolationLevel::default(), IsolationLevel::ReadUncommitted);
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", IsolationLevel::ReadUncommitted), "read_uncommitted");
    assert_eq!(format!("{}", IsolationLevel::ReadCommitted), "read_committed");
}

#[test]
fn test_equality() {
    assert_eq!(IsolationLevel::ReadUncommitted, IsolationLevel::ReadUncommitted);
    assert_ne!(IsolationLevel::ReadUncommitted, IsolationLevel::ReadCommitted);
}

// --- ClientIdAndBroker tests ---

#[test]
fn test_cib_new() {
    let cib = ClientIdAndBroker::new("client-1".to_string(), "host".to_string(), 9092);
    assert_eq!(cib.client_id(), "client-1");
    assert_eq!(cib.broker_host(), "host");
    assert_eq!(cib.broker_port(), 9092);
}

#[test]
fn test_cib_display() {
    let cib = ClientIdAndBroker::new("c1".to_string(), "h1".to_string(), 9092);
    assert_eq!(format!("{}", cib), "c1-h1-9092");
}

#[test]
fn test_cib_default() {
    let cib = ClientIdAndBroker::default();
    assert_eq!(cib.client_id(), "");
    assert_eq!(cib.broker_host(), "");
    assert_eq!(cib.broker_port(), -1);
}

#[rstest]
#[case("c1", "h1", 9092, "c1", "h1", 9092, true)]
#[case("c1", "h1", 9092, "c2", "h1", 9092, false)]
#[case("c1", "h1", 9092, "c1", "h2", 9092, false)]
#[case("c1", "h1", 9092, "c1", "h1", 9093, false)]
fn test_cib_equality(
    #[case] cid1: &str,
    #[case] host1: &str,
    #[case] port1: i32,
    #[case] cid2: &str,
    #[case] host2: &str,
    #[case] port2: i32,
    #[case] expected: bool,
) {
    let a = ClientIdAndBroker::new(cid1.to_string(), host1.to_string(), port1);
    let b = ClientIdAndBroker::new(cid2.to_string(), host2.to_string(), port2);
    assert_eq!(a == b, expected);
}
