use kafka_server_common::{
    BrokerRegistrationFencingChange, BrokerRegistrationInControlledShutdownChange, BrokerRegistrationReply,
};
use rstest::rstest;

// --- BrokerRegistrationFencingChange tests ---

#[rstest]
#[case(BrokerRegistrationFencingChange::Fence, 1, Some(true))]
#[case(BrokerRegistrationFencingChange::None, 0, None)]
#[case(BrokerRegistrationFencingChange::Unfence, -1, Some(false))]
fn test_fencing_change_value_and_boolean(
    #[case] change: BrokerRegistrationFencingChange,
    #[case] expected_value: i8,
    #[case] expected_bool: Option<bool>,
) {
    assert_eq!(change.value(), expected_value);
    assert_eq!(change.as_boolean(), expected_bool);
}

#[rstest]
#[case(1)]
#[case(0)]
#[case(-1)]
fn test_fencing_change_from_value_known(#[case] value: i8) {
    let result = BrokerRegistrationFencingChange::from_value(value);
    assert!(result.is_some());
}

#[rstest]
#[case(2)]
#[case(99)]
#[case(127)]
fn test_fencing_change_from_value_unknown(#[case] value: i8) {
    let result = BrokerRegistrationFencingChange::from_value(value);
    assert!(result.is_none());
}

#[test]
fn test_fencing_change_from_value_specific() {
    assert_eq!(
        BrokerRegistrationFencingChange::from_value(1),
        Some(BrokerRegistrationFencingChange::Fence)
    );
    assert_eq!(
        BrokerRegistrationFencingChange::from_value(0),
        Some(BrokerRegistrationFencingChange::None)
    );
    assert_eq!(
        BrokerRegistrationFencingChange::from_value(-1),
        Some(BrokerRegistrationFencingChange::Unfence)
    );
}

#[test]
fn test_fencing_change_constants() {
    assert_eq!(BrokerRegistrationFencingChange::FENCE.value(), 1);
    assert_eq!(BrokerRegistrationFencingChange::NONE.value(), 0);
    assert_eq!(BrokerRegistrationFencingChange::UNFENCE.value(), -1);
}

#[test]
fn test_fencing_change_default() {
    assert_eq!(BrokerRegistrationFencingChange::default(), BrokerRegistrationFencingChange::NONE);
}

// --- BrokerRegistrationInControlledShutdownChange tests ---

#[rstest]
#[case(BrokerRegistrationInControlledShutdownChange::None, 0, None)]
#[case(BrokerRegistrationInControlledShutdownChange::InControlledShutdown, 1, Some(true))]
#[case(BrokerRegistrationInControlledShutdownChange::Unknown, -1, None)]
fn test_in_controlled_shutdown_value_and_boolean(
    #[case] change: BrokerRegistrationInControlledShutdownChange,
    #[case] expected_value: i8,
    #[case] expected_bool: Option<bool>,
) {
    assert_eq!(change.value(), expected_value);
    assert_eq!(change.as_boolean(), expected_bool);
}

#[rstest]
#[case(0, BrokerRegistrationInControlledShutdownChange::None)]
#[case(1, BrokerRegistrationInControlledShutdownChange::InControlledShutdown)]
#[case(99, BrokerRegistrationInControlledShutdownChange::Unknown)]
fn test_in_controlled_shutdown_from_value(
    #[case] value: i8,
    #[case] expected: BrokerRegistrationInControlledShutdownChange,
) {
    assert_eq!(
        BrokerRegistrationInControlledShutdownChange::from_value(value).unwrap(),
        expected
    );
}

#[test]
fn test_in_controlled_shutdown_constants() {
    assert_eq!(
        BrokerRegistrationInControlledShutdownChange::NONE.value(),
        0
    );
    assert_eq!(
        BrokerRegistrationInControlledShutdownChange::IN_CONTROLLED_SHUTDOWN.value(),
        1
    );
}

#[test]
fn test_in_controlled_shutdown_default() {
    assert_eq!(
        BrokerRegistrationInControlledShutdownChange::default(),
        BrokerRegistrationInControlledShutdownChange::NONE
    );
}

// --- BrokerRegistrationReply tests ---

#[test]
fn test_broker_registration_reply_new() {
    let reply = BrokerRegistrationReply::new(42);
    assert_eq!(reply.epoch(), 42);
}

#[test]
fn test_broker_registration_reply_default() {
    let reply = BrokerRegistrationReply::default();
    assert_eq!(reply.epoch(), 0);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(-1)]
#[case(100)]
#[case(i64::MAX)]
fn test_broker_registration_reply_epoch(#[case] epoch: i64) {
    assert_eq!(BrokerRegistrationReply::new(epoch).epoch(), epoch);
}
