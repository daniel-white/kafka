//! Connection channel states.
//!
//! Mirrors `org.apache.kafka.common.network.ChannelState`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelState {
    NotConnected,
    Authenticate,
    Ready,
    Expired,
    FailedSend,
    AuthenticationFailed { reason: String },
    LocalClose,
}

impl ChannelState {
    pub fn is_ready(&self) -> bool {
        matches!(self, ChannelState::Ready)
    }

    pub fn is_not_connected(&self) -> bool {
        matches!(self, ChannelState::NotConnected)
    }
}
