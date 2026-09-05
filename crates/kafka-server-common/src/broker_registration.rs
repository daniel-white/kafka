//! Broker registration related types.
//!
//! Mirrors types from `metadata/src/main/java/org/apache/kafka/metadata/`.
//!
//! MIGRATION_SOURCE:
//! - metadata/.../BrokerRegistrationFencingChange.java
//! - metadata/.../BrokerRegistrationInControlledShutdownChange.java
//! - metadata/.../BrokerRegistrationReply.java

use getset::CopyGetters;

/// Fencing state change for broker registration.
///
/// Mirrors Java's `BrokerRegistrationFencingChange` enum.
///
/// MIGRATION_SOURCE: metadata/.../BrokerRegistrationFencingChange.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrokerRegistrationFencingChange {
    /// Fence the broker (value=1, asBoolean=Some(true)).
    Fence,
    /// No fencing change (value=0, asBoolean=None).
    None,
    /// Unfence the broker (value=-1, asBoolean=Some(false)).
    Unfence,
}

impl BrokerRegistrationFencingChange {
    /// Fence the broker.
    pub const FENCE: BrokerRegistrationFencingChange = BrokerRegistrationFencingChange::Fence;

    /// No fencing change.
    pub const NONE: BrokerRegistrationFencingChange = BrokerRegistrationFencingChange::None;

    /// Unfence the broker.
    pub const UNFENCE: BrokerRegistrationFencingChange = BrokerRegistrationFencingChange::Unfence;

    /// Wire byte value for this change.
    ///
    /// Mirrors Java's `value()`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationFencingChange.java
    pub fn value(&self) -> i8 {
        match self {
            BrokerRegistrationFencingChange::Fence => 1,
            BrokerRegistrationFencingChange::None => 0,
            BrokerRegistrationFencingChange::Unfence => -1,
        }
    }

    /// Optional boolean representation.
    ///
    /// Mirrors Java's `asBoolean()`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationFencingChange.java
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            BrokerRegistrationFencingChange::Fence => Some(true),
            BrokerRegistrationFencingChange::None => None,
            BrokerRegistrationFencingChange::Unfence => Some(false),
        }
    }

    /// Parse from a wire byte value.
    /// Returns `None` for unrecognized values.
    ///
    /// Mirrors Java's `fromValue(byte)`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationFencingChange.java
    pub fn from_value(value: i8) -> Option<BrokerRegistrationFencingChange> {
        match value {
            1 => Some(BrokerRegistrationFencingChange::FENCE),
            0 => Some(BrokerRegistrationFencingChange::NONE),
            -1 => Some(BrokerRegistrationFencingChange::UNFENCE),
            _ => None,
        }
    }
}

impl Default for BrokerRegistrationFencingChange {
    fn default() -> Self {
        BrokerRegistrationFencingChange::NONE
    }
}

/// In-controlled-shutdown state change for broker registration.
///
/// Mirrors Java's `BrokerRegistrationInControlledShutdownChange` enum.
///
/// MIGRATION_SOURCE: metadata/.../BrokerRegistrationInControlledShutdownChange.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrokerRegistrationInControlledShutdownChange {
    /// No change (value=0, asBoolean=None).
    None,
    /// Enter controlled shutdown (value=1, asBoolean=Some(true)).
    InControlledShutdown,
    /// Unknown state.
    Unknown,
}

impl BrokerRegistrationInControlledShutdownChange {
    /// No change.
    pub const NONE: BrokerRegistrationInControlledShutdownChange =
        BrokerRegistrationInControlledShutdownChange::None;

    /// Enter controlled shutdown.
    pub const IN_CONTROLLED_SHUTDOWN: BrokerRegistrationInControlledShutdownChange =
        BrokerRegistrationInControlledShutdownChange::InControlledShutdown;

    /// Wire byte value for this change.
    ///
    /// Mirrors Java's `value()`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationInControlledShutdownChange.java
    pub fn value(&self) -> i8 {
        match self {
            BrokerRegistrationInControlledShutdownChange::None => 0,
            BrokerRegistrationInControlledShutdownChange::InControlledShutdown => 1,
            BrokerRegistrationInControlledShutdownChange::Unknown => -1,
        }
    }

    /// Optional boolean representation.
    ///
    /// Mirrors Java's `asBoolean()`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationInControlledShutdownChange.java
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            BrokerRegistrationInControlledShutdownChange::None => None,
            BrokerRegistrationInControlledShutdownChange::InControlledShutdown => Some(true),
            BrokerRegistrationInControlledShutdownChange::Unknown => None,
        }
    }

    /// Parse from a wire byte value.
    /// Returns `None` for unrecognized values.
    ///
    /// Mirrors Java's `fromValue(byte)`.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationInControlledShutdownChange.java
    pub fn from_value(value: i8) -> Option<BrokerRegistrationInControlledShutdownChange> {
        match value {
            0 => Some(BrokerRegistrationInControlledShutdownChange::NONE),
            1 => Some(BrokerRegistrationInControlledShutdownChange::IN_CONTROLLED_SHUTDOWN),
            _ => Some(BrokerRegistrationInControlledShutdownChange::Unknown),
        }
    }
}

impl Default for BrokerRegistrationInControlledShutdownChange {
    fn default() -> Self {
        BrokerRegistrationInControlledShutdownChange::NONE
    }
}

/// Reply to a broker registration request.
///
/// Mirrors Java's `BrokerRegistrationReply` record.
///
/// MIGRATION_SOURCE: metadata/.../BrokerRegistrationReply.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct BrokerRegistrationReply {
    #[get_copy = "pub"]
    epoch: i64,
}

impl BrokerRegistrationReply {
    /// Create a new BrokerRegistrationReply.
    ///
    /// MIGRATION_SOURCE: metadata/.../BrokerRegistrationReply.java
    pub const fn new(epoch: i64) -> Self {
        BrokerRegistrationReply { epoch }
    }
}

impl Default for BrokerRegistrationReply {
    fn default() -> Self {
        BrokerRegistrationReply::new(0)
    }
}
