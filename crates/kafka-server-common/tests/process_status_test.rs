use kafka_server_common::ProcessStatus;
use rstest::rstest;

#[rstest]
#[case(ProcessStatus::Shutdown, false, true)]
#[case(ProcessStatus::Starting, false, false)]
#[case(ProcessStatus::Started, true, false)]
#[case(ProcessStatus::ShuttingDown, false, true)]
#[case(ProcessStatus::Unknown, false, false)]
fn test_status(
    #[case] status: ProcessStatus,
    #[case] expected_running: bool,
    #[case] expected_stopped: bool,
) {
    assert_eq!(status.is_running(), expected_running);
    assert_eq!(status.is_stopped(), expected_stopped);
}

#[test]
fn test_default() {
    assert_eq!(ProcessStatus::default(), ProcessStatus::Unknown);
}

#[test]
fn test_started_is_running() {
    assert!(ProcessStatus::Started.is_running());
    assert!(!ProcessStatus::Started.is_stopped());
}

#[test]
fn test_shutdown_is_stopped() {
    assert!(!ProcessStatus::Shutdown.is_running());
    assert!(ProcessStatus::Shutdown.is_stopped());
}

#[test]
fn test_shutting_down_is_stopped() {
    assert!(!ProcessStatus::ShuttingDown.is_running());
    assert!(ProcessStatus::ShuttingDown.is_stopped());
}