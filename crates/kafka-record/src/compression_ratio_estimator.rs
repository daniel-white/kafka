//! Compression ratio estimator, tracking per-topic compression ratios.
//!
//! Mirrors `org.apache.kafka.common.record.internal.CompressionRatioEstimator`.

use crate::compression_type::CompressionType;
use std::collections::HashMap;

pub const COMPRESSION_RATIO_IMPROVING_STEP: f32 = 0.005;
pub const COMPRESSION_RATIO_DETERIORATE_STEP: f32 = 0.05;

const NUM_COMPRESSION_TYPES: usize = 5;

pub struct CompressionRatioEstimator {
    ratios: HashMap<String, [f32; NUM_COMPRESSION_TYPES]>,
}

impl CompressionRatioEstimator {
    pub fn new() -> Self {
        CompressionRatioEstimator {
            ratios: HashMap::new(),
        }
    }

    fn initial_ratios() -> [f32; NUM_COMPRESSION_TYPES] {
        [1.0; NUM_COMPRESSION_TYPES]
    }

    fn get_or_create(&mut self, topic: &str) -> &mut [f32; NUM_COMPRESSION_TYPES] {
        self.ratios.entry(topic.to_string()).or_insert_with(Self::initial_ratios)
    }

    pub fn update_estimation(
        &mut self,
        topic: &str,
        compression_type: CompressionType,
        observed_ratio: f32,
    ) -> f32 {
        let ratios = self.get_or_create(topic);
        let type_id = compression_type.id() as usize;
        let current_estimation = ratios[type_id];

        if current_estimation < observed_ratio {
            ratios[type_id] = (current_estimation + COMPRESSION_RATIO_DETERIORATE_STEP)
                .max(observed_ratio);
        } else {
            ratios[type_id] = (current_estimation - COMPRESSION_RATIO_IMPROVING_STEP)
                .max(observed_ratio);
        }

        ratios[type_id]
    }

    pub fn estimation(&self, topic: &str, compression_type: CompressionType) -> f32 {
        let type_id = compression_type.id() as usize;
        self.ratios
            .get(topic)
            .map(|r| r[type_id])
            .unwrap_or(compression_type.rate())
    }

    pub fn set_estimation(
        &mut self,
        topic: &str,
        compression_type: CompressionType,
        ratio: f32,
    ) {
        let ratios = self.get_or_create(topic);
        ratios[compression_type.id() as usize] = ratio;
    }

    pub fn reset_estimation(&mut self, topic: &str) {
        let ratios = self.get_or_create(topic);
        for r in ratios.iter_mut() {
            *r = 1.0;
        }
    }
}

impl Default for CompressionRatioEstimator {
    fn default() -> Self {
        Self::new()
    }
}
