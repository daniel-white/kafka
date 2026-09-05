use kafka_common::ConsumerGroupState;
use rstest::rstest;
use std::str::FromStr;

#[rstest]
#[case(ConsumerGroupState::Unknown, "Unknown")]
#[case(ConsumerGroupState::PreparingRebalance, "PreparingRebalance")]
#[case(ConsumerGroupState::CompletingRebalance, "CompletingRebalance")]
#[case(ConsumerGroupState::Stable, "Stable")]
#[case(ConsumerGroupState::Dead, "Dead")]
#[case(ConsumerGroupState::Empty, "Empty")]
#[case(ConsumerGroupState::Assigning, "Assigning")]
#[case(ConsumerGroupState::Reconciling, "Reconciling")]
#[case(ConsumerGroupState::NotReady, "NotReady")]
fn test_name(#[case] state: ConsumerGroupState, #[case] expected: &str) {
    assert_eq!(state.name(), expected);
}

#[rstest]
#[case("Unknown", ConsumerGroupState::Unknown)]
#[case("PreparingRebalance", ConsumerGroupState::PreparingRebalance)]
#[case("Stable", ConsumerGroupState::Stable)]
#[case("Dead", ConsumerGroupState::Dead)]
#[case("Empty", ConsumerGroupState::Empty)]
#[case("Reconciling", ConsumerGroupState::Reconciling)]
#[case("NotReady", ConsumerGroupState::NotReady)]
#[case("unknown", ConsumerGroupState::Unknown)]
#[case("STABLE", ConsumerGroupState::Stable)]
#[case("UnknownName", ConsumerGroupState::Unknown)]
fn test_from_str(#[case] input: &str, #[case] expected: ConsumerGroupState) {
    assert_eq!(ConsumerGroupState::from_str(input).unwrap(), expected);
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", ConsumerGroupState::Stable), "Stable");
    assert_eq!(format!("{}", ConsumerGroupState::Unknown), "Unknown");
}

#[test]
fn test_default() {
    assert_eq!(ConsumerGroupState::default(), ConsumerGroupState::Unknown);
}

#[test]
fn test_equality() {
    assert_eq!(ConsumerGroupState::Stable, ConsumerGroupState::Stable);
    assert_ne!(ConsumerGroupState::Stable, ConsumerGroupState::Dead);
}
