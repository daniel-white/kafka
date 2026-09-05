use kafka_coordinator_common::{CoordinatorError, CoordinatorResult};
use rstest::rstest;

// --- CoordinatorResult tests ---

#[test]
fn test_coordinator_result_new() {
    let result: CoordinatorResult<i32, String> =
        CoordinatorResult::new(vec!["record1".to_string()]);
    assert_eq!(result.records().len(), 1);
    assert_eq!(result.response(), &None);
    assert!(result.replay_records());
    assert!(result.is_atomic());
}

#[test]
fn test_coordinator_result_with_response() {
    let result: CoordinatorResult<String, Vec<u8>> =
        CoordinatorResult::with_response(vec![], "response".to_string());
    assert_eq!(result.response().as_deref(), Some("response"));
    assert!(result.records().is_empty());
    assert!(result.is_atomic());
}

#[test]
fn test_coordinator_result_not_atomic() {
    let result: CoordinatorResult<(), i32> = CoordinatorResult::new_atomic(vec![1, 2, 3], false);
    assert!(!result.is_atomic());
    assert!(result.replay_records());
}

#[test]
fn test_coordinator_result_builder() {
    let result: CoordinatorResult<i32, String> = CoordinatorResult::builder(vec!["a".to_string()])
        .replay_records(false)
        .atomic(false)
        .build();
    assert!(!result.replay_records());
    assert!(!result.is_atomic());
    assert_eq!(result.records().len(), 1);
}

#[test]
fn test_coordinator_result_display() {
    let result: CoordinatorResult<i32, String> = CoordinatorResult::new(vec!["data".to_string()]);
    let s = format!("{}", result);
    assert!(s.contains("replayRecords=true"));
    assert!(s.contains("isAtomic=true"));
}

#[test]
fn test_coordinator_result_equality() {
    let a: CoordinatorResult<i32, String> = CoordinatorResult::new(vec!["x".to_string()]);
    let b: CoordinatorResult<i32, String> = CoordinatorResult::new(vec!["x".to_string()]);
    assert_eq!(a, b);

    let c: CoordinatorResult<i32, String> = CoordinatorResult::new(vec!["y".to_string()]);
    assert_ne!(a, c);
}

#[rstest]
#[case(true, true, "new")]
#[case(true, false, "builder_not_atomic")]
#[case(true, true, "with_response")]
#[case(false, true, "builder_no_replay")]
fn test_replay_and_atomic(
    #[case] expected_replay: bool,
    #[case] expected_atomic: bool,
    #[case] scenario: &str,
) {
    let result: CoordinatorResult<i32, String> = match scenario {
        "new" => CoordinatorResult::new(vec![]),
        "builder_not_atomic" => {
            CoordinatorResult::builder(vec![]).atomic(false).build()
        }
        "with_response" => CoordinatorResult::with_response(vec![], 42),
        "builder_no_replay" => {
            CoordinatorResult::builder(vec![]).replay_records(false).build()
        }
        _ => unreachable!(),
    };
    assert_eq!(result.replay_records(), expected_replay, "replay mismatch for {}", scenario);
    assert_eq!(result.is_atomic(), expected_atomic, "atomic mismatch for {}", scenario);
}

// --- CoordinatorError tests ---

#[test]
fn test_unknown_record_type_error() {
    let err = CoordinatorError::UnknownRecordType { unknown_type: 99 };
    assert_eq!(err.to_string(), "Found an unknown record type 99");
}

#[test]
fn test_unknown_record_version_error() {
    let err = CoordinatorError::UnknownRecordVersion {
        unknown_version: 3,
        record_type: 5,
    };
    assert_eq!(
        err.to_string(),
        "Found an unknown record version 3 for record type 5"
    );
}

#[test]
fn test_coordinator_error() {
    let err = CoordinatorError::Coordinator {
        message: "failed".to_string(),
    };
    assert_eq!(err.to_string(), "Coordinator error: failed");
}
