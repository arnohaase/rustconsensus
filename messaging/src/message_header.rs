use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;

//TODO human readable Debug
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

    //TODO unit test ser / deser
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(9999)]
    #[case(99999)]
    #[case(u32::MAX)]
    fn test_ser(#[case] len: u32) {
        let original = MessageHeader { message_len: len };

        let mut buf = BytesMut::new();
        original.ser(&mut buf);
        let mut b: &[u8] = &buf;
        let deser = MessageHeader::deser(&mut b).unwrap();
        assert!(b.is_empty());
        assert_eq!(deser, original);
    }
}