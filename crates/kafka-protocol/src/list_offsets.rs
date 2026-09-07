//! ListOffsets request message (Readable).
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsRequest.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::message_context::MessageContext;

/// ListOffsets request message.
///
/// For this broker implementation, the request body is not parsed
/// (the broker returns minimal response with empty topic list). This type
/// implements `Readable` so handlers can use the standard
/// `ctx.read_msg::<ListOffsetsRequest>()` pattern.
#[derive(Debug, Clone, Default)]
pub struct ListOffsetsRequest;

impl Readable for ListOffsetsRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(ListOffsetsRequest)
    }
}