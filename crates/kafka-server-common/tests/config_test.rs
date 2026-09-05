use kafka_server_common::{BrokerEndPoint, ConfigSynonym, ConfigType};
use rstest::rstest;

// --- BrokerEndPoint tests ---

#[test]
fn test_broker_end_point_new() {
    let ep = BrokerEndPoint::new(1, "localhost".to_string(), 9092);
    assert_eq!(ep.id(), 1);
    assert_eq!(ep.host(), "localhost");
    assert_eq!(ep.port(), 9092);
}

#[test]
fn test_broker_end_point_default() {
    let ep = BrokerEndPoint::default();
    assert_eq!(ep.id(), -1);
    assert_eq!(ep.host(), "");
    assert_eq!(ep.port(), -1);
}

#[test]
fn test_broker_end_point_equality() {
    let a = BrokerEndPoint::new(1, "h".to_string(), 9092);
    let b = BrokerEndPoint::new(1, "h".to_string(), 9092);
    assert_eq!(a, b);

    let c = BrokerEndPoint::new(2, "h".to_string(), 9092);
    assert_ne!(a, c);
}

// --- ConfigType tests ---

#[rstest]
#[case(ConfigType::Topic, "topics")]
#[case(ConfigType::Client, "clients")]
#[case(ConfigType::User, "users")]
#[case(ConfigType::Broker, "brokers")]
#[case(ConfigType::Ip, "ips")]
#[case(ConfigType::ClientMetrics, "client-metrics")]
#[case(ConfigType::Group, "groups")]
fn test_config_type_value(#[case] ct: ConfigType, #[case] expected: &str) {
    assert_eq!(ct.value(), expected);
}

#[rstest]
#[case("topics", Some(ConfigType::Topic))]
#[case("clients", Some(ConfigType::Client))]
#[case("users", Some(ConfigType::User))]
#[case("brokers", Some(ConfigType::Broker))]
#[case("ips", Some(ConfigType::Ip))]
#[case("client-metrics", Some(ConfigType::ClientMetrics))]
#[case("groups", Some(ConfigType::Group))]
#[case("unknown", None)]
#[case("", None)]
fn test_config_type_from_value(#[case] value: &str, #[case] expected: Option<ConfigType>) {
    assert_eq!(ConfigType::from_value(value), expected);
}

#[test]
fn test_config_type_display() {
    assert_eq!(format!("{}", ConfigType::Topic), "topics");
    assert_eq!(format!("{}", ConfigType::Broker), "brokers");
}

#[test]
fn test_config_type_default() {
    assert_eq!(ConfigType::default(), ConfigType::Topic);
}

// --- ConfigSynonym tests ---

#[test]
fn test_config_synonym_new() {
    let cs = ConfigSynonym::new("old.name".to_string());
    assert_eq!(cs.name(), "old.name");
    assert_eq!(cs.convert("test"), "test");
}

#[test]
fn test_config_synonym_with_converter() {
    let cs = ConfigSynonym::with_converter(
        "converter".to_string(),
        std::sync::Arc::new(|s: &str| s.to_uppercase()),
    );
    assert_eq!(cs.name(), "converter");
    assert_eq!(cs.convert("hello"), "HELLO");
}

#[test]
fn test_config_synonym_hours_to_milliseconds() {
    assert_eq!(ConfigSynonym::hours_to_milliseconds("2"), "7200000");
    assert_eq!(ConfigSynonym::hours_to_milliseconds("0"), "0");
    assert_eq!(ConfigSynonym::hours_to_milliseconds(""), "0");
    assert_eq!(ConfigSynonym::hours_to_milliseconds("  3  "), "10800000");
}

#[test]
fn test_config_synonym_minutes_to_milliseconds() {
    assert_eq!(ConfigSynonym::minutes_to_milliseconds("1"), "60000");
    assert_eq!(ConfigSynonym::minutes_to_milliseconds("0"), "0");
    assert_eq!(ConfigSynonym::minutes_to_milliseconds(""), "0");
    assert_eq!(ConfigSynonym::minutes_to_milliseconds("  5  "), "300000");
}

#[rstest]
#[case("2", "7200000")]
#[case("0", "0")]
#[case("", "0")]
#[case("  3  ", "10800000")]
fn test_hours_to_ms(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(ConfigSynonym::hours_to_milliseconds(input), expected);
}

#[rstest]
#[case("1", "60000")]
#[case("0", "0")]
#[case("", "0")]
#[case("  5  ", "300000")]
fn test_minutes_to_ms(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(ConfigSynonym::minutes_to_milliseconds(input), expected);
}

#[test]
fn test_config_synonym_default() {
    let cs = ConfigSynonym::default();
    assert_eq!(cs.name(), "");
    assert_eq!(cs.convert("test"), "test");
}
