use kafka_protocol::errors::Errors;
use kafka_server_common::ApiError;
use rstest::rstest;

#[test]
fn test_api_error_none() {
    assert!(ApiError::NONE.is_success());
    assert!(!ApiError::NONE.is_failure());
    assert!(ApiError::NONE.message().is_none());
}

#[test]
fn test_api_error_from_error() {
    let err = ApiError::from_error(Errors::MessageTooLarge);
    assert!(!err.is_success());
    assert!(err.is_failure());
    assert_eq!(err.error(), &Errors::MessageTooLarge);
    assert!(err.message().is_some());
}

#[test]
fn test_api_error_new() {
    let err = ApiError::new(Errors::InvalidRequest, Some("bad".to_string()));
    assert_eq!(err.error(), &Errors::InvalidRequest);
    assert_eq!(err.message().as_deref(), Some("bad"));
}

#[test]
fn test_api_error_from_code() {
    let err = ApiError::from_code(7, None);
    assert_eq!(err.error(), &Errors::RequestTimedOut);
    assert!(err.message().is_none());
}

#[test]

fn test_api_error_message_with_fallback() {
    let err = ApiError::new(Errors::InvalidRequest, Some("custom".to_string()));
    assert_eq!(err.message_with_fallback(), "custom");

    let err2 = ApiError::from_error(Errors::InvalidRequest);
    assert_eq!(err2.message_with_fallback(), Errors::InvalidRequest.message());
}

#[test]
fn test_api_error_default() {
    assert_eq!(ApiError::default(), ApiError::NONE);
}

#[rstest]
#[case(Errors::None, true)]
#[case(Errors::InvalidRequest, false)]
#[case(Errors::UnknownServerError, false)]
#[case(Errors::MessageTooLarge, false)]
fn test_is_success(#[case] error: Errors, #[case] expected: bool) {
    let err = ApiError::from_error(error);
    assert_eq!(err.is_success(), expected);
}

#[rstest]
#[case(Errors::None, false)]
#[case(Errors::InvalidRequest, true)]
#[case(Errors::UnknownServerError, true)]
fn test_is_failure(#[case] error: Errors, #[case] expected: bool) {
    let err = ApiError::from_error(error);
    assert_eq!(err.is_failure(), expected);
}
