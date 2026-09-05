use kafka_server_common::BrokerState;
use rstest::rstest;

#[rstest]
#[case(BrokerState::NotRunning, 0)]
#[case(BrokerState::Starting, 1)]
#[case(BrokerState::Recovery, 2)]
#[case(BrokerState::Running, 3)]
#[case(BrokerState::PendingControlledShutdown, 6)]
#[case(BrokerState::ShuttingDown, 7)]
#[case(BrokerState::Unknown, 127)]
fn test_value(#[case] state: BrokerState, #[case] expected: i8) {
    assert_eq!(state.value(), expected);
}

#[rstest]
#[case(0, BrokerState::NotRunning)]
#[case(1, BrokerState::Starting)]
#[case(2, BrokerState::Recovery)]
#[case(3, BrokerState::Running)]
#[case(6, BrokerState::PendingControlledShutdown)]
#[case(7, BrokerState::ShuttingDown)]
#[case(127, BrokerState::Unknown)]
#[case(99, BrokerState::Unknown)]
#[case(-1, BrokerState::Unknown)]
fn test_from_value(#[case] value: i8, #[case] expected: BrokerState) {
    assert_eq!(BrokerState::from_value(value), expected);
}

#[test]
fn test_round_trip() {
    for state in [
        BrokerState::NotRunning,
        BrokerState::Starting,
        BrokerState::Recovery,
        BrokerState::Running,
        BrokerState::PendingControlledShutdown,
        BrokerState::ShuttingDown,
        BrokerState::Unknown,
    ] {
        assert_eq!(BrokerState::from_value(state.value()), state);
    }
}

#[test]
fn test_default() {
    assert_eq!(BrokerState::default(), BrokerState::Unknown);
}
