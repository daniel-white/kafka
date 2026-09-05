//! ListenerName: name of a Kafka network listener.
//!
//! Mirrors Java's `ListenerName` class from `clients/common/network/`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/network/ListenerName.java

use getset::Getters;

/// Name of a Kafka network listener.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: clients/.../network/ListenerName.java
#[derive(Debug, Clone, PartialEq, Eq, Hash, Getters)]
pub struct ListenerName {
    #[get = "pub"]
    value: String,
}

impl ListenerName {
    /// Configuration static prefix for listener names.
    const CONFIG_STATIC_PREFIX: &'static str = "listener.name";

    /// Create a new ListenerName with the given value.
    ///
    /// Mirrors Java's `ListenerName(String value)`.
    ///
    /// MIGRATION_SOURCE: clients/.../network/ListenerName.java
    pub fn new(value: String) -> Self {
        ListenerName { value }
    }

    /// Create a ListenerName normalised to uppercase.
    ///
    /// Mirrors Java's `ListenerName.normalised(String)`.
    ///
    /// MIGRATION_SOURCE: clients/.../network/ListenerName.java
    pub fn normalised(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Err("The provided listener name is null or empty string".to_string());
        }
        Ok(ListenerName {
            value: value.to_uppercase(),
        })
    }

    /// Build a config prefix for this listener.
    ///
    /// Mirrors Java's `configPrefix()`.
    ///
    /// MIGRATION_SOURCE: clients/.../network/ListenerName.java
    pub fn config_prefix(&self) -> String {
        format!("{}.{}.", Self::CONFIG_STATIC_PREFIX, self.value.to_lowercase())
    }

    /// Build a SASL mechanism config prefix.
    ///
    /// Mirrors Java's `saslMechanismConfigPrefix(String)`.
    ///
    /// MIGRATION_SOURCE: clients/.../network/ListenerName.java
    pub fn sasl_mechanism_config_prefix(&self, sasl_mechanism: &str) -> String {
        self.config_prefix() + &Self::sasl_mechanism_prefix(sasl_mechanism)
    }

    /// Build a SASL mechanism prefix string.
    ///
    /// Mirrors Java's `saslMechanismPrefix(String)`.
    ///
    /// MIGRATION_SOURCE: clients/.../network/ListenerName.java
    pub fn sasl_mechanism_prefix(sasl_mechanism: &str) -> String {
        format!("{}.", sasl_mechanism.to_lowercase())
    }
}

impl Default for ListenerName {
    fn default() -> Self {
        ListenerName::new(String::new())
    }
}

impl std::fmt::Display for ListenerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListenerName({})", self.value)
    }
}
