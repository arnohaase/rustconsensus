use bytes::{Buf, BytesMut};

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
        todo!()
    }

    pub fn deser(mut buf: &impl Buf) -> anyhow::Result<Self> {
        todo!()
    }
}