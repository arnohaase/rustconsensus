use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub message_len: u32,
}

impl MessageHeader {
    pub const SERIALIZED_LEN: usize = size_of::<u32>();
    pub const SERIALIZED_LEN_U16: u16 = Self::SERIALIZED_LEN as u16;

    pub(crate) fn for_message(message: &[u8]) -> MessageHeader {
        MessageHeader {
            message_len: message.len() as u32, //TODO overflow
        }
    }

    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u32(self.message_len);
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<Self> {
        let message_len = buf.try_get_u32()?;
        Ok(MessageHeader {
            message_len,
        })
    }
}