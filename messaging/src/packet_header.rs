use crate::packet_id::PacketId;
use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;
use std::fmt::Debug;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use crc::Crc;
use tracing::{error, warn};

bitflags! {
    #[derive(PartialEq, Eq, Copy, Clone)]
    struct Flags: u8 {
        const MASK_IP  = 0b0000_0011;
        const MASK_KIND = 0b0001_1100;

        const IPV4 = 0b0000_0000;
        const IPV6 = 0b0000_0001;
        const IP_SAME = 0b0000_0010;

        const KIND_SEQUENCE   = 0b0000_0000;
        const KIND_OUT_OF_SEQ = 0b0000_0100;
        const KIND_INIT       = 0b0000_1000;
        const KIND_NAK        = 0b0000_1100;
        const KIND_RECV_SYNC  = 0b0001_0000;
        const KIND_SEND_SYNC  = 0b0001_0100;
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PacketHeader {
    pub protocol_version: u8,
    pub checksum: u64,
    pub reply_to_address: Option<SocketAddr>,
    pub packet_kind: PacketKind,
}
impl Debug for PacketHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reply_to = self.reply_to_address
            .map(|addr| format!("[{}]", addr))
            .unwrap_or("".to_string());

        write!(f, "PCKT{{V{}{}:{:?}}}", 1, reply_to, self.packet_kind)
    }
}

impl PacketHeader {
    pub const PROTOCOL_VERSION_1: u8 = 0;
    pub const OFFSET_MESSAGE_CONTINUES: u16 = u16::MAX;

    const OFFSET_START_CHECKSUM: usize = 1;
    const OFFSET_AFTER_CHECKSUM: usize = Self::OFFSET_START_CHECKSUM + size_of::<u64>();

    pub fn new(reply_to_address: Option<SocketAddr>, packet_kind: PacketKind) -> PacketHeader {
        PacketHeader {
            protocol_version: Self::PROTOCOL_VERSION_1,
            checksum: 0,
            reply_to_address,
            packet_kind,
        }
    }

    pub fn serialized_len_for_stream_header(reply_to_address: Option<SocketAddr>) -> usize {
        let reply_to_len = match &reply_to_address {
            None => 0,
            Some(SocketAddr::V4(_)) => 4 + 2,
            Some(SocketAddr::V6(_)) => 16 + 2,
        };

        size_of::<u8>()          // protocol version
            + size_of::<u64>()   // checksum
            + size_of::<u8>()    // flags
            + reply_to_len
            + size_of::<u16>()   // stream id
            + size_of::<u16>()   // first message offset
            + size_of::<u64>()   // packet number
    }

    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u8(self.protocol_version);
        buf.put_u64(self.checksum);

        let flags_ip = match self.reply_to_address {
            None => Flags::IP_SAME,
            Some(addr) => {
                if addr.is_ipv4() {
                    Flags::IPV4
                }
                else {
                    Flags::IPV6
                }
            }
        };
        let flags_kind = match self.packet_kind {
            PacketKind::RegularSequenced { .. } => Flags::KIND_SEQUENCE,
            PacketKind::OutOfSequence => Flags::KIND_OUT_OF_SEQ,
            PacketKind::ControlInit { .. } => Flags::KIND_INIT,
            PacketKind::ControlRecvSync { .. } => Flags::KIND_RECV_SYNC,
            PacketKind::ControlSendSync { .. } => Flags::KIND_SEND_SYNC,
            PacketKind::ControlNak { .. } => Flags::KIND_NAK,
        };
        buf.put_u8((flags_ip | flags_kind).bits());

        match self.reply_to_address {
            Some(SocketAddr::V4(addr)) => {
                buf.put_u32(addr.ip().to_bits());
                buf.put_u16(addr.port());
            }
            Some(SocketAddr::V6(addr)) => {
                buf.put_u128(addr.ip().to_bits());
                buf.put_u16(addr.port());
            }
            None => {}
        }

        let stream_id = match self.packet_kind {
            PacketKind::RegularSequenced { stream_id, .. } => Some(stream_id),
            PacketKind::OutOfSequence => None,
            PacketKind::ControlInit { stream_id, .. } => Some(stream_id),
            PacketKind::ControlRecvSync { stream_id, .. } => Some(stream_id),
            PacketKind::ControlSendSync { stream_id, .. } => Some(stream_id),
            PacketKind::ControlNak { stream_id, .. } => Some(stream_id)
        };
        if let Some(stream_id) = stream_id {
            buf.put_u16(stream_id);
        }

        if let PacketKind::RegularSequenced { first_message_offset, packet_sequence_number, .. } = self.packet_kind {
            buf.put_u16(first_message_offset.unwrap_or(Self::OFFSET_MESSAGE_CONTINUES));
            buf.put_u64(packet_sequence_number.to_raw());
        }
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<PacketHeader> {
        let protocol_version = buf.try_get_u8()?;
        if protocol_version != Self::PROTOCOL_VERSION_1 {
            return Err(anyhow::anyhow!("Unsupported protocol version {}", protocol_version));
        }

        let checksum = buf.try_get_u64()?; //TODO verify

        let flags = Flags::from_bits_truncate(buf.try_get_u8()?);

        let reply_to_address: Option<SocketAddr> = match flags & Flags::MASK_IP {
            Flags::IPV4 => Some(SocketAddrV4::new(buf.try_get_u32()?.into(), buf.try_get_u16()?).into()),
            Flags::IPV6 => Some(SocketAddrV6::new(buf.try_get_u128()?.into(), buf.try_get_u16()?, 0, 0).into()),
            Flags::IP_SAME => None,
            _ => return Err(anyhow::anyhow!("Unsupported flags for reply-to address: {:x}", flags.bits())),
        };

        let packet_kind = match flags & Flags::MASK_KIND {
            Flags::KIND_SEQUENCE => {
                let stream_id = buf.try_get_u16()?;
                let first_message_offset = match buf.try_get_u16()? {
                    Self::OFFSET_MESSAGE_CONTINUES => None,
                    x => Some(x),
                };
                let packet_sequence_number = PacketId::from_raw(buf.try_get_u64()?);

                PacketKind::RegularSequenced {
                    stream_id,
                    first_message_offset,
                    packet_sequence_number,
                }
            },
            Flags::KIND_OUT_OF_SEQ => PacketKind::OutOfSequence,
            Flags::KIND_INIT => PacketKind::ControlInit { stream_id: buf.try_get_u16()? },
            Flags::KIND_NAK => PacketKind::ControlNak { stream_id: buf.try_get_u16()? },
            Flags::KIND_SEND_SYNC => PacketKind::ControlSendSync { stream_id: buf.try_get_u16()? },
            Flags::KIND_RECV_SYNC => PacketKind::ControlRecvSync { stream_id: buf.try_get_u16()? },
            _ => return Err(anyhow::anyhow!("Unsupported flags for packet kind: {:x}", flags.bits())),
        };

        Ok(PacketHeader {
            checksum,
            protocol_version,
            reply_to_address,
            packet_kind,
        })
    }

    pub fn calc_checksum(buf: &[u8]) -> u64 {
        assert!(buf.len() >= Self::OFFSET_MESSAGE_CONTINUES as usize, "packet buffer too short for checksum");

        let hasher = Crc::<u64>::new(&crc::CRC_64_REDIS);
        let mut digest = hasher.digest();

        digest.update(&buf[Self::OFFSET_AFTER_CHECKSUM..]);

        digest.finalize()
    }

    pub fn init_checksum(buf: &mut [u8]) {
        let checksum = Self::calc_checksum(buf);

        let mut buf = &mut buf[Self::OFFSET_START_CHECKSUM..];
        buf.put_u64(checksum);
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum PacketKind {
    RegularSequenced {
        stream_id: u16,
        first_message_offset: Option<u16>,
        packet_sequence_number: PacketId,
    },
    OutOfSequence,
    ControlInit { stream_id: u16 },
    ControlRecvSync { stream_id: u16 },
    ControlSendSync { stream_id: u16 },
    ControlNak { stream_id: u16 },
}
impl Debug for PacketKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketKind::RegularSequenced { stream_id, first_message_offset, packet_sequence_number } => {
                write!(f, "SEQ({}@{}:{})",
                       stream_id,
                       packet_sequence_number.to_raw(),
                       first_message_offset.map(|x| x.to_string()).unwrap_or("-".to_string())
                )
            }
            PacketKind::OutOfSequence => write!(f, "OOS"),
            PacketKind::ControlInit { stream_id } => write!(f, "INIT({})", stream_id),
            PacketKind::ControlRecvSync { stream_id } => write!(f, "RECV_SYNC({})", stream_id),
            PacketKind::ControlSendSync { stream_id } => write!(f, "SEND_SYNC({})", stream_id),
            PacketKind::ControlNak { stream_id } => write!(f, "NAK({})", stream_id),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use PacketKind::*;
    use std::str::FromStr;

    #[rstest]
    #[case::no_addr(PacketHeader::new(None, ControlInit{ stream_id: 1 }), "PCKT{V1:INIT(1)}")]
    #[case::v4_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), OutOfSequence), "PCKT{V1[1.2.3.4:888]:OOS}")]
    #[case::v6_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlNak { stream_id: 9}), "PCKT{V1[[1111:2222::3333:4444]:888]:NAK(9)}")]
    fn test_packet_header_debug(#[case] header: PacketHeader, #[case] expected: &str) {
        assert_eq!(format!("{:?}", header), expected);
    }

    #[rstest]
    #[case::no_addr(PacketHeader::new(None, ControlInit{ stream_id: 1 }))]
    #[case::v4_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), OutOfSequence))]
    #[case::v6_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlNak { stream_id: 9}))]
    #[case::seq_start(PacketHeader::new(None, RegularSequenced { stream_id: 0, first_message_offset: Some(999), packet_sequence_number: PacketId::from_raw(8)}))]
    #[case::seq_continued(PacketHeader::new(None, RegularSequenced { stream_id: 4, first_message_offset: None, packet_sequence_number: PacketId::from_raw(6)}))]
    #[case::oos(PacketHeader::new(None, OutOfSequence))]
    #[case::init_0(PacketHeader::new(None, ControlInit { stream_id: 0 }))]
    #[case::init_1(PacketHeader::new(None, ControlInit { stream_id: 1 }))]
    #[case::recv_sync_0(PacketHeader::new(None, ControlRecvSync { stream_id: 0 }))]
    #[case::recv_sync_3(PacketHeader::new(None, ControlRecvSync { stream_id: 3 }))]
    #[case::send_sync_0(PacketHeader::new(None, ControlSendSync { stream_id: 0 }))]
    #[case::send_sync_3(PacketHeader::new(None, ControlSendSync { stream_id: 2 }))]
    #[case::nak_0(PacketHeader::new(None, ControlNak { stream_id: 0 }))]
    #[case::nak_5(PacketHeader::new(None, ControlNak { stream_id: 5 }))]
    fn test_packet_header_ser(#[case] header: PacketHeader) {
        let mut buf = BytesMut::new();
        header.ser(&mut buf);
        let mut b: &[u8] = &mut buf;
        let deser = PacketHeader::deser(&mut b).unwrap();
        assert!(b.is_empty());
        assert_eq!(header, deser);
    }

    #[rstest]
    #[case::no_addr_none(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }))]
    #[case::no_addr_0(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    #[case::no_addr_8000(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    #[case::v4_addr_none(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }))]
    #[case::v4_addr_0(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    #[case::v4_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    #[case::v6_addr_none(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }))]
    #[case::v6_addr_0(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    #[case::v6_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }))]
    fn test_packet_header_serialized_len_for_stream_header(#[case] header: PacketHeader) {
        let mut buf = BytesMut::new();
        header.ser(&mut buf);
        let expected = buf.len();
        assert_eq!(PacketHeader::serialized_len_for_stream_header(header.reply_to_address), expected);
    }

    #[rstest]
    #[case::seq_start(RegularSequenced { stream_id: 0, first_message_offset: Some(999), packet_sequence_number: PacketId::from_raw(8)}, "SEQ(0@8:999)")]
    #[case::seq_continued(RegularSequenced { stream_id: 4, first_message_offset: None, packet_sequence_number: PacketId::from_raw(6)}, "SEQ(4@6:-)")]
    #[case::oos(OutOfSequence, "OOS")]
    #[case::init_0(ControlInit { stream_id: 0 }, "INIT(0)")]
    #[case::init_1(ControlInit { stream_id: 1 }, "INIT(1)")]
    #[case::recv_sync_0(ControlRecvSync { stream_id: 0 }, "RECV_SYNC(0)")]
    #[case::recv_sync_3(ControlRecvSync { stream_id: 3 }, "RECV_SYNC(3)")]
    #[case::send_sync_0(ControlSendSync { stream_id: 0 }, "SEND_SYNC(0)")]
    #[case::send_sync_3(ControlSendSync { stream_id: 2 }, "SEND_SYNC(2)")]
    #[case::nak_0(ControlNak { stream_id: 0 }, "NAK(0)")]
    #[case::nak_5(ControlNak { stream_id: 5 }, "NAK(5)")]
    fn test_packet_kind_debug(#[case] kind: PacketKind, #[case] expected: &str) {
        assert_eq!(format!("{:?}", kind), expected);
    }

    #[test]
    fn test_calc_checksum() {
        todo!()
    }

    #[test]
    fn test_init_checksum() {
        todo!()
    }
}