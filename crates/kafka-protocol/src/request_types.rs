//! Request message types for ListOffsets, DescribeTopics, and Fetch.
//!
//! These request types implement `Readable` so that handlers can use
//! `ctx.read_msg::<RequestType>()` to follow the standard pattern.
//!
//! MIGRATION_SOURCE: (new) — protocol crate

use crate::errors::ProtocolError;
use crate::message_context::MessageContext;
use crate::{Readable, Reader};

/// ListOffsets request message.
///
/// For this broker implementation, ListOffsets does not need to parse
/// the request body.
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

/// DescribeTopics request message.
///
/// For this broker, DescribeTopics does not need to parse the request body.
#[derive(Debug, Clone, Default)]
pub struct DescribeTopicsRequest;

impl Readable for DescribeTopicsRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(DescribeTopicsRequest)
    }
}

/// Fetch request message.
///
/// This request has a body but for this broker implementation we read it
/// using the standard pattern then parse manually.
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