use kafka_common::ListenerName;
use rstest::rstest;

#[test]
fn test_new() {
    let ln = ListenerName::new("PLAINTEXT".to_string());
    assert_eq!(ln.value(), "PLAINTEXT");
}

#[test]
fn test_normalised() {
    let ln = ListenerName::normalised("plaintext").unwrap();
    assert_eq!(ln.value(), "PLAINTEXT");
}

#[test]
fn test_normalised_uppercase() {
    let ln = ListenerName::normalised("PLAINTEXT").unwrap();
    assert_eq!(ln.value(), "PLAINTEXT");
}

#[test]
fn test_normalised_empty_fails() {
    let result = ListenerName::normalised("");
    assert!(result.is_err());
}

#[rstest]
#[case("PLAINTEXT", "PLAINTEXT")]
#[case("plaintext", "PLAINTEXT")]
#[case("Ssl", "SSL")]
#[case("SASL_SSL", "SASL_SSL")]
fn test_normalised_uppercase_conversion(#[case] input: &str, #[case] expected: &str) {
    let ln = ListenerName::normalised(input).unwrap();
    assert_eq!(ln.value(), expected);
}

#[test]
fn test_display() {
    let ln = ListenerName::new("PLAINTEXT".to_string());
    assert_eq!(format!("{}", ln), "ListenerName(PLAINTEXT)");
}

#[test]
fn test_equality() {
    assert_eq!(
        ListenerName::new("PLAINTEXT".to_string()),
        ListenerName::new("PLAINTEXT".to_string())
    );
    assert_ne!(
        ListenerName::new("PLAINTEXT".to_string()),
        ListenerName::new("SSL".to_string())
    );
}

#[test]
fn test_config_prefix() {
    let ln = ListenerName::new("PLAINTEXT".to_string());
    assert_eq!(ln.config_prefix(), "listener.name.plaintext.");
}

#[test]
fn test_sasl_mechanism_config_prefix() {
    let ln = ListenerName::new("SASL_SSL".to_string());
    assert_eq!(
        ln.sasl_mechanism_config_prefix("PLAIN"),
        "listener.name.sasl_ssl.plain."
    );
}

#[test]
fn test_sasl_mechanism_prefix() {
    assert_eq!(ListenerName::sasl_mechanism_prefix("PLAIN"), "plain.");
    assert_eq!(ListenerName::sasl_mechanism_prefix("ScramSha256"), "scramsha256.");
}

#[test]
fn test_default() {
    let ln = ListenerName::default();
    assert_eq!(ln.value(), "");
}
