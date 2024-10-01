use std::net::SocketAddr;

use anyhow::anyhow;
use bytes::{Buf, BufMut, BytesMut};

use crate::comm::message_module::MessageModuleId;
use crate::node_addr::NodeAddr;


#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Envelope {
    //TODO hashcode / checksum
    from: NodeAddr,
    to: NodeAddr,
    pub message_module_id: MessageModuleId,
}
impl Envelope {
    const ADDR_SIZE: usize = size_of::<u32>();
    const ENVELOPE_SIZE: usize = 2*Self::ADDR_SIZE + size_of::<MessageModuleId>();

    pub fn try_read(buf: &mut impl Buf, from: SocketAddr, to: SocketAddr) -> anyhow::Result<Envelope> {
        if buf.remaining() < Self::ENVELOPE_SIZE {
            return Err(anyhow!("message is shorter than envelope size: {} < {} bytes", buf.remaining(), Self::ENVELOPE_SIZE));
        }

        let from = Self::read_addr(buf, from);
        let to = Self::read_addr(buf, to);

        let message_module_id = buf.get_u64_le();

        Ok(Envelope {
            from,
            to,
            message_module_id: MessageModuleId(message_module_id),
        })
    }

    fn read_addr(buf: &mut impl Buf, socket_addr: SocketAddr) -> NodeAddr {
        let unique = buf.get_u32_le();
        NodeAddr {
            unique,
            addr: socket_addr,
        }
    }

    pub fn write(from: NodeAddr, to: NodeAddr, message_module_id: MessageModuleId, buf: &mut BytesMut) {
        buf.put_u32_le(from.unique);
        buf.put_u32_le(to.unique);
        buf.put_u64_le(message_module_id.0);
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::str::FromStr;

    use bytes::BytesMut;
    use rstest::rstest;

    use crate::comm::message_module::MessageModuleId;
    use crate::node_addr::NodeAddr;
    use super::*;

    #[rstest]
    #[case::just_envelope(b"1\0\0\04\0\0\0abcdefgh", b"", "1.2.3.4:5678", "9.8.7.6:1234", Some(Envelope {
    from: NodeAddr { unique: 0x31, addr: SocketAddr::from_str("1.2.3.4:5678").unwrap() },
    to:   NodeAddr { unique: 0x34, addr: SocketAddr::from_str("9.8.7.6:1234").unwrap() },
    message_module_id: MessageModuleId::from(b"abcdefgh".clone())
    }))]
    #[case::remainder(b"2\0\0\03\0\0\012345678abc", b"abc", "4.3.2.1:5678", "1.2.3.4:1234", Some(Envelope {
    from: NodeAddr { unique: 0x32, addr: SocketAddr::from_str("4.3.2.1:5678").unwrap() },
    to:   NodeAddr { unique: 0x33, addr: SocketAddr::from_str("1.2.3.4:1234").unwrap() },
    message_module_id: MessageModuleId::from(b"12345678".clone())
    }))]
    #[case::too_short(b"123412341234567", b"", "1.2.3.4:5678", "9.8.7.6:1234", None)]
    fn test_envelope_try_read(#[case] mut buf: &[u8], #[case] buf_after: &[u8], #[case] from: &str, #[case] to: &str, #[case] expected: Option<Envelope>) {
        let from = SocketAddr::from_str(from).unwrap();
        let to = SocketAddr::from_str(to).unwrap();
        match Envelope::try_read(&mut buf, from, to) {
            Ok(actual) => {
                assert_eq!(actual, expected.unwrap());
                assert_eq!(buf, buf_after);
            }
            Err(e) => {
                println!("{}", e);
                assert!(expected.is_none());
            }
        }
    }

    #[rstest]
    #[case::complete(b"\0\0\0\0", b"", "127.0.0.1:8888", 0)]
    #[case::remainder(b"\x04\0\0\0\x01", b"\x01", "127.0.2.3:8765", 4)]
    fn test_envelope_read_addr(#[case] mut buf: &[u8], #[case] buf_after: &[u8], #[case] addr: &str, #[case] unique: u32) {
        let addr = SocketAddr::from_str(addr).unwrap();
        let actual = Envelope::read_addr(&mut buf, addr.clone());
        assert_eq!(actual, NodeAddr { unique, addr, });
        assert_eq!(buf, buf_after);
    }

    #[rstest]
    #[case(1, 2, "abc", b"\x01\0\0\0\x02\0\0\0abc\0\0\0\0\0")]
    fn test_envelope_write(#[case] from: u32, #[case] to: u32, #[case] module_id: &str, #[case] expected: &[u8]) {
        let mut buf = BytesMut::new();
        Envelope::write(NodeAddr::localhost(from), NodeAddr::localhost(to), MessageModuleId::from(module_id), &mut buf);
        assert_eq!(&buf, expected);
    }

}
