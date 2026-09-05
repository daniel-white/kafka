use kafka_storage::LogCleaningState;

#[test]
fn test_in_progress() {
    let state = LogCleaningState::in_progress();
    assert!(matches!(state, LogCleaningState::InProgress));
    assert_eq!(state, LogCleaningState::InProgress);
    assert_eq!(state, LogCleaningState::default());
}

#[test]
fn test_aborted() {
    let state = LogCleaningState::aborted();
    assert!(matches!(state, LogCleaningState::Aborted));
}

#[test]
fn test_paused() {
    let state = LogCleaningState::paused(3);
    assert!(matches!(state, LogCleaningState::Paused { paused_count: 3 }));
    assert_eq!(state.paused_count(), 3);
}

#[test]
fn test_paused_zero() {
    let state = LogCleaningState::paused(0);
    assert_eq!(state.paused_count(), 0);
}

#[test]
fn test_equality() {
    assert_eq!(LogCleaningState::in_progress(), LogCleaningState::in_progress());
    assert_eq!(LogCleaningState::aborted(), LogCleaningState::aborted());
    assert_eq!(LogCleaningState::paused(5), LogCleaningState::paused(5));
    assert_ne!(LogCleaningState::in_progress(), LogCleaningState::aborted());
    assert_ne!(LogCleaningState::paused(1), LogCleaningState::paused(2));
    assert_ne!(LogCleaningState::InProgress, LogCleaningState::paused(0));
}

#[test]
fn test_default() {
    assert_eq!(
        LogCleaningState::default(),
        LogCleaningState::InProgress
    );
}

#[test]
fn test_debug() {
    let state = LogCleaningState::paused(2);
    let s = format!("{:?}", state);
    assert!(s.contains("Paused"));
    assert!(s.contains("paused_count: 2"));
}
