//! Tagged fields section for flexible protocol messages (v2+).
//!
//! Mirrors `org.apache.kafka.common.protocol.types.TaggedFields`.
//!
//! Wire format (for flexible versions):
//!   count   unsigned varint — number of tagged fields
//!   for each field:
//!     tag   unsigned varint — tag number (field index)
//!     size  unsigned varint — size of the field payload
//!     data  <size> bytes    — raw field payload
//!
//! In the generated `MessageData` code, known tagged fields are deserialized
//! as typed values; unknown ones become `RawTaggedField` entries.

use crate::errors::ProtocolError;
use crate::io::reader::{Readable, Reader};
use crate::io::writer::{Writable, Writer};
use crate::{MessageContext};
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaggedFields {
    fields: BTreeMap<i32, Vec<RawTaggedField>>,
}

impl TaggedFields {
    pub fn empty() -> Self {
        TaggedFields::default()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn add(&mut self, field: RawTaggedField) {
        let tag = field.tag();
        self.fields.entry(*tag).or_default().push(field);
    }

    pub fn get(&self, tag: i32) -> Option<&[RawTaggedField]> {
        self.fields.get(&tag).map(|v| v.as_slice())
    }

    pub fn all_fields(&self) -> impl Iterator<Item = &RawTaggedField> {
        self.fields.values().flatten()
    }
}

impl Writable for TaggedFields {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let total: usize = self.fields.values().map(|v| v.len()).sum();
        w.write_unsigned_varint(total as u32);
        for (tag, list) in self.fields.iter() {
            for field in list {
                w.write_unsigned_varint(*tag as u32);
                field.write(w, ctx);
            }
        }
    }
}

impl Readable for TaggedFields {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let count = r.read_unsigned_varint()?;
        let mut fields: BTreeMap<i32, Vec<RawTaggedField>> = BTreeMap::new();
        for _ in 0..count {
            let field = RawTaggedField::read(r, ctx)?;
            fields.entry(*field.tag()).or_default().push(field);
        }
        Ok(TaggedFields { fields })
    }
}

/// A raw tagged field: a tag number and its opaque payload bytes.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
#[derive(Getters, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct RawTaggedField {
    #[get = "pub"]
    tag: i32,
    #[get = "pub"]
    data: Box<[u8]>,
}

impl RawTaggedField {
    /// Construct a new RawTaggedField.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
    pub fn new(tag: i32, data: Vec<u8>) -> Self {
        RawTaggedField { tag, data: data.into_boxed_slice() }
    }

    /// Number of payload bytes.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/types/RawTaggedField.java
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl Writable for RawTaggedField {
    fn write<W: Writer>(&self, w: &mut W, _ctx: &MessageContext) {
        w.write_unsigned_varint(self.data().len() as u32);
        w.write_bytes(self.data());
    }
}

impl Readable for RawTaggedField {
    fn read<R: Reader>(r: &mut R, _ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let tag = r.read_unsigned_varint()?;
        let size = r.read_unsigned_varint()?;
        let data = r.read_bytes_vec(size as usize)?;
        let field = RawTaggedField::new(tag as i32, data);
        Ok(field)
    }
}