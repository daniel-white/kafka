use getset::CopyGetters;
use crate::io::writer::Writable;
use crate::{MessageContext, Writer};

/// `Writer` implementation for `SizeCounter` — counts bytes without storing them.
#[derive(Default, CopyGetters)]
struct SizeCounter {
    #[getset(get_copy = "pub")]
    size: usize,
}

impl Writer for SizeCounter {
    fn write_byte(&mut self, _val: u8) {
        self.size += 1;
    }
    fn write_short(&mut self, _val: i16) {
        self.size += 2;
    }
    fn write_int(&mut self, _val: i32) {
        self.size += 4;
    }
    fn write_long(&mut self, _val: i64) {
        self.size += 8;
    }
    fn write_bytes(&mut self, arr: &[u8]) {
        self.size += arr.len();
    }
}

/// Compute the serialized size of a `Writable` message by writing to a
/// `SizeCounter` (which counts bytes without storing them).
pub fn compute_size(msg: &impl Writable, ctx: &MessageContext) -> usize {
    let mut counter = SizeCounter::default();
    msg.write(&mut counter, ctx);
    counter.size()
}