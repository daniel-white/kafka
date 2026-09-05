use kafka_protocol::ApiKey;

#[test]
fn test_produce_key() {
    let key = ApiKey::from_id(0);
    assert_eq!(key, ApiKey::Produce);
    assert_eq!(key.name(), "Produce");
    assert_eq!(key.id(), 0);
}

#[test]
fn test_api_versions_key() {
    let key = ApiKey::from_id(15);
    assert_eq!(key, ApiKey::ApiVersions);
    assert_eq!(key.name(), "ApiVersions");
    assert_eq!(key.id(), 15);
}

#[test]
fn test_unknown_key() {
    let key = ApiKey::from_id(999);
    assert_eq!(key, ApiKey::Unknown(999));
    assert_eq!(key.name(), "Unknown");
    assert_eq!(key.id(), 999);
}

#[test]
fn test_versions() {
    let produce = ApiKey::Produce;
    assert_eq!(produce.min_version(), 0);
    assert_eq!(produce.max_version(), 13);
}

#[test]
fn test_round_trip() {
    for id in [0i16, 1, 2, 3, 15, 17, 18, 19, 20, 21] {
        let key = ApiKey::from_id(id);
        assert_eq!(key.id(), id);
    }
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", ApiKey::Fetch), "Fetch");
    assert_eq!(format!("{}", ApiKey::Unknown(42)), "Unknown");
}
