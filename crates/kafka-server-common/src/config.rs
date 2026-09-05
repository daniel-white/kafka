//! Config types: ConfigType enum and ConfigSynonym.
//!
//! Mirrors types from `server-common/src/main/java/org/apache/kafka/server/config/`.
//!
//! MIGRATION_SOURCE:
//! - server-common/.../config/ConfigType.java
//! - server-common/.../config/ConfigSynonym.java

use getset::Getters;
use std::sync::Arc;

/// Configuration resource type.
///
/// Mirrors Java's `ConfigType` enum.
///
/// MIGRATION_SOURCE: server-common/.../config/ConfigType.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConfigType {
    /// Topic-level configs.
    #[default]
    Topic,
    /// Client-level configs.
    Client,
    /// User-level configs.
    User,
    /// Broker-level configs.
    Broker,
    /// IP-level configs.
    Ip,
    /// Client-metrics configs.
    ClientMetrics,
    /// Group-level configs.
    Group,
}

impl ConfigType {
    /// String value for this config type (e.g. "topics", "clients").
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigType.java
    pub fn value(&self) -> &'static str {
        match self {
            ConfigType::Topic => "topics",
            ConfigType::Client => "clients",
            ConfigType::User => "users",
            ConfigType::Broker => "brokers",
            ConfigType::Ip => "ips",
            ConfigType::ClientMetrics => "client-metrics",
            ConfigType::Group => "groups",
        }
    }

    /// Parse from a string value.
    /// Returns `None` for unrecognized values.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigType.java
    pub fn from_value(value: &str) -> Option<ConfigType> {
        match value {
            "topics" => Some(ConfigType::Topic),
            "clients" => Some(ConfigType::Client),
            "users" => Some(ConfigType::User),
            "brokers" => Some(ConfigType::Broker),
            "ips" => Some(ConfigType::Ip),
            "client-metrics" => Some(ConfigType::ClientMetrics),
            "groups" => Some(ConfigType::Group),
            _ => None,
        }
    }
}

impl std::fmt::Display for ConfigType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

/// A configuration synonym: maps old config names to new ones with a value converter.
///
/// Mirrors Java's `ConfigSynonym` class.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
#[derive(Clone, Getters)]
pub struct ConfigSynonym {
    #[get = "pub"]
    name: String,
    converter: Arc<dyn Fn(&str) -> String + Send + Sync>,
}

impl std::fmt::Debug for ConfigSynonym {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConfigSynonym(name={}, ...)", self.name)
    }
}

impl ConfigSynonym {
    /// Create a new ConfigSynonym with a converter function.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
    pub fn with_converter(
        name: String,
        converter: Arc<dyn Fn(&str) -> String + Send + Sync>,
    ) -> Self {
        ConfigSynonym { name, converter }
    }

    /// Create a new ConfigSynonym with identity converter.
    ///
    /// Mirrors Java's `ConfigSynonym(String name)` constructor.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
    pub fn new(name: String) -> Self {
        ConfigSynonym {
            name,
            converter: Arc::new(|s: &str| s.to_string()),
        }
    }

    /// Convert a config value using this synonym's converter.
    ///
    /// Mirrors Java's `converter().apply()`.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
    pub fn convert(&self, input: &str) -> String {
        (self.converter)(input)
    }

    /// Hours-to-milliseconds converter.
    ///
    /// Mirrors Java's `ConfigSynonym.HOURS_TO_MILLISECONDS`.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
    pub fn hours_to_milliseconds(input: &str) -> String {
        let hours = parse_int(input, 0);
        (hours * 3_600_000).to_string()
    }

    /// Minutes-to-milliseconds converter.
    ///
    /// Mirrors Java's `ConfigSynonym.MINUTES_TO_MILLISECONDS`.
    ///
    /// MIGRATION_SOURCE: server-common/.../config/ConfigSynonym.java
    pub fn minutes_to_milliseconds(input: &str) -> String {
        let minutes = parse_int(input, 0);
        (minutes * 60_000).to_string()
    }
}

fn parse_int(input: &str, default: i32) -> i32 {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return default;
    }
    trimmed.parse::<i32>().unwrap_or(default)
}

impl Default for ConfigSynonym {
    fn default() -> Self {
        ConfigSynonym::new(String::new())
    }
}
