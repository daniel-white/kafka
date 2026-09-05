use kafka_record::compression_ratio_estimator::{
    CompressionRatioEstimator, COMPRESSION_RATIO_IMPROVING_STEP,
};
use kafka_record::CompressionType;

// Ported from CompressionRatioEstimatorTest.testUpdateEstimation
#[test]
fn test_update_estimation() {
    let test_cases: [(f32, f32); 4] = [
        (0.8, 0.84),
        (0.6, 0.7),
        (0.6, 0.4),
        (0.004, 0.001),
    ];

    for (current_estimation, observed_ratio) in test_cases {
        let topic = "tp";
        let mut estimator = CompressionRatioEstimator::new();
        estimator.set_estimation(topic, CompressionType::ZSTD, current_estimation);

        let updated = estimator.update_estimation(topic, CompressionType::ZSTD, observed_ratio);

        // Updated ratio should never be smaller than the observed ratio
        assert!(
            updated >= observed_ratio,
            "updated {} should be >= observed {}",
            updated,
            observed_ratio
        );
    }
}

#[test]
fn test_estimation_defaults_to_rate() {
    let estimator = CompressionRatioEstimator::new();
    assert_eq!(estimator.estimation("test", CompressionType::ZSTD), 1.0);
    assert_eq!(estimator.estimation("test", CompressionType::GZIP), 1.0);
    assert_eq!(estimator.estimation("test", CompressionType::NONE), 1.0);
}

#[test]
fn test_set_estimation() {
    let mut estimator = CompressionRatioEstimator::new();
    estimator.set_estimation("topic1", CompressionType::GZIP, 0.5);
    assert_eq!(estimator.estimation("topic1", CompressionType::GZIP), 0.5);
    // Other types should still be default
    assert_eq!(estimator.estimation("topic1", CompressionType::ZSTD), 1.0);
}

#[test]
fn test_reset_estimation() {
    let mut estimator = CompressionRatioEstimator::new();
    estimator.set_estimation("topic1", CompressionType::GZIP, 0.5);
    estimator.set_estimation("topic1", CompressionType::ZSTD, 0.3);

    estimator.reset_estimation("topic1");

    assert_eq!(estimator.estimation("topic1", CompressionType::GZIP), 1.0);
    assert_eq!(estimator.estimation("topic1", CompressionType::ZSTD), 1.0);
}

#[test]
fn test_estimation_per_topic_isolation() {
    let mut estimator = CompressionRatioEstimator::new();
    estimator.set_estimation("topic1", CompressionType::GZIP, 0.5);
    estimator.set_estimation("topic2", CompressionType::GZIP, 0.8);

    assert_eq!(estimator.estimation("topic1", CompressionType::GZIP), 0.5);
    assert_eq!(estimator.estimation("topic2", CompressionType::GZIP), 0.8);
}

#[test]
fn test_update_estimation_deteriorate() {
    let mut estimator = CompressionRatioEstimator::new();
    let topic = "test";
    estimator.set_estimation(topic, CompressionType::ZSTD, 0.8);

    // observed > current → deteriorate: current + step, clamped to observed
    let updated = estimator.update_estimation(topic, CompressionType::ZSTD, 0.84);
    assert_eq!(updated, 0.85); // max(0.8 + 0.05, 0.84) = max(0.85, 0.84) = 0.85
    assert!(updated >= 0.84);
}

#[test]
fn test_update_estimation_improving() {
    let mut estimator = CompressionRatioEstimator::new();
    let topic = "test";
    estimator.set_estimation(topic, CompressionType::ZSTD, 0.6);

    // observed < current → improve: current - step, clamped to observed
    let updated = estimator.update_estimation(topic, CompressionType::ZSTD, 0.4);
    // max(0.6 - 0.005, 0.4) = max(0.595, 0.4) = 0.595
    assert_eq!(updated, 0.6 - COMPRESSION_RATIO_IMPROVING_STEP);
    assert!(updated >= 0.4);
}
