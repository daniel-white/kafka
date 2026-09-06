//! Kafka API keys — maps integer api_key values to their enum variants.
//!
//! This file includes the auto-generated `ApiKey` enum and associated methods
//! from the Kafka message JSON specs (clients/src/main/resources/common/message/*Request.json).
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java

include!(concat!(env!("OUT_DIR"), "/gen_api_key.rs"));
