//! Fetch request message (Readable).
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::message_context::MessageContext;

/// Fetch request message.
///
/// For this broker implementation, the request body is not parsed
/// (the broker returns stored records). This type implements `Readable`
/// so handlers can use the standard `ctx.read_msg::<FetchRequest>()` pattern.
#[derive(Debug, Clone, Default)]
pub struct FetchRequest;

impl Readable for FetchRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(FetchRequest)
    }
}