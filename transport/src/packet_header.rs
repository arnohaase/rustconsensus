use crate::packet_id::PacketId;
use bitflags::bitflags;
use bytes::{Buf, BufMut};
use std::fmt::Debug;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use safe_converter::PrecheckedCast;
use crate::safe_converter;

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
        const KIND_RECV_SYNC  = 0b0001_0000;
        const KIND_SEND_SYNC  = 0b0001_0100;
        const KIND_PING       = 0b0001_1000; 
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PacketHeader {
    pub protocol_version: u8,
    pub reply_to_address: Option<SocketAddr>,
    pub packet_kind: PacketKind,
    pub sender_generation: u64,
    pub receiver_generation: Option<u64>,
}
impl Debug for PacketHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reply_to = self.reply_to_address
            .map(|addr| format!("[{}]", addr))
            .unwrap_or("".to_string());

        write!(f, "PCKT{{V{}{}:{:?}@{}}}", 1, reply_to, self.packet_kind, self.sender_generation)
    }
}

impl PacketHeader {
    pub const PROTOCOL_VERSION_1: u8 = 0;
    pub const OFFSET_MESSAGE_CONTINUES: u16 = u16::MAX;

    pub fn new(reply_to_address: Option<SocketAddr>, packet_kind: PacketKind, sender_generation: u64, receiver_generation: Option<u64>) -> PacketHeader {
        PacketHeader {
            protocol_version: Self::PROTOCOL_VERSION_1,
            reply_to_address,
            packet_kind,
            sender_generation,
            receiver_generation,
        }
    }

    pub fn serialized_len_for_stream_header(reply_to_address: Option<SocketAddr>) -> usize {
        let reply_to_len = match &reply_to_address {
            None => 0,
            Some(SocketAddr::V4(_)) => 4 + 2,
            Some(SocketAddr::V6(_)) => 16 + 2,
        };

        size_of::<u8>()          // flags
            + 6                  // sender_generation as u48
            + 6                  // receiver_generation as u48
            + reply_to_len
            + size_of::<u16>()   // stream id
            + size_of::<u16>()   // first message offset
            + size_of::<u64>()   // packet number
    }

    pub fn ser(&self, buf: &mut impl BufMut) {
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
            PacketKind::FireAndForget => Flags::KIND_OUT_OF_SEQ,
            PacketKind::ControlRecvSync { .. } => Flags::KIND_RECV_SYNC,
            PacketKind::ControlSendSync { .. } => Flags::KIND_SEND_SYNC,
            PacketKind::Ping => Flags::KIND_PING,
        };
        buf.put_u8((flags_ip | flags_kind).bits());

        // write sender generation as u48
        buf.put_u16((self.sender_generation >> 32) as u16);
        buf.put_u32(self.sender_generation as u32);

        // write receiver generation as u48
        buf.put_u16(((self.receiver_generation.unwrap_or(0) >> 32) & 0xffff).prechecked_cast());
        buf.put_u32((self.receiver_generation.unwrap_or(0) & 0xffff_ffff).prechecked_cast());

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
            PacketKind::FireAndForget => None,
            PacketKind::ControlRecvSync { stream_id, .. } => Some(stream_id),
            PacketKind::ControlSendSync { stream_id, .. } => Some(stream_id),
            PacketKind::Ping => None,
        };
        if let Some(stream_id) = stream_id {
            buf.put_u16(stream_id);
        }

        if let PacketKind::RegularSequenced { first_message_offset, packet_sequence_number, .. } = self.packet_kind {
            buf.put_u16(first_message_offset.unwrap_or(Self::OFFSET_MESSAGE_CONTINUES));
            buf.put_u64(packet_sequence_number.to_raw());
        }
    }

    pub fn deser(buf: &mut impl Buf, protocol_version: u8) -> anyhow::Result<PacketHeader> {
        if protocol_version != Self::PROTOCOL_VERSION_1 {
            return Err(anyhow::anyhow!("Unsupported protocol version {}", protocol_version));
        }

        let flags = Flags::from_bits_truncate(buf.try_get_u8()?);
        // read sender generation as u48
        let sender_generation = ((buf.try_get_u16()? as u64) << 32) + buf.try_get_u32()? as u64;

        // read receiver generation as u48
        let receiver_generation = ((buf.try_get_u16()? as u64) << 32) + buf.try_get_u32()? as u64;
        let receiver_generation = if receiver_generation == 0 { None } else { Some(receiver_generation) };

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
            Flags::KIND_OUT_OF_SEQ => PacketKind::FireAndForget,
            Flags::KIND_SEND_SYNC => PacketKind::ControlSendSync { stream_id: buf.try_get_u16()? },
            Flags::KIND_RECV_SYNC => PacketKind::ControlRecvSync { stream_id: buf.try_get_u16()? },
            Flags::KIND_PING => PacketKind::Ping,
            _ => return Err(anyhow::anyhow!("Unsupported flags for packet kind: {:x}", flags.bits())),
        };

        Ok(PacketHeader {
            sender_generation,
            receiver_generation,
            protocol_version,
            reply_to_address,
            packet_kind,
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum PacketKind {
    RegularSequenced {
        stream_id: u16,
        first_message_offset: Option<u16>,
        packet_sequence_number: PacketId,
    },
    FireAndForget,
    ControlRecvSync { stream_id: u16 },
    ControlSendSync { stream_id: u16 },
    Ping,
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
            PacketKind::FireAndForget => write!(f, "OOS"),
            PacketKind::ControlRecvSync { stream_id } => write!(f, "RECV_SYNC({})", stream_id),
            PacketKind::ControlSendSync { stream_id } => write!(f, "SEND_SYNC({})", stream_id),
            PacketKind::Ping => write!(f, "PING"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use PacketKind::*;
    use std::str::FromStr;
    use crate::buffers::fixed_buffer::FixedBuf;

    #[rstest]
    #[case::v4_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), FireAndForget, 4, Some(9)), "PCKT{V1[1.2.3.4:888]:OOS@4}")]
    #[case::v4_addr_wo_peer_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), FireAndForget, 4, None), "PCKT{V1[1.2.3.4:888]:OOS@4}")]
    #[case::v6_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlSendSync { stream_id: 9}, 5, Some(4)), "PCKT{V1[[1111:2222::3333:4444]:888]:SEND_SYNC(9)@5}")]
    #[case::v6_addr_wo_peer_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlSendSync { stream_id: 9}, 5, None), "PCKT{V1[[1111:2222::3333:4444]:888]:SEND_SYNC(9)@5}")]
    fn test_packet_header_debug(#[case] header: PacketHeader, #[case] expected: &str) {
        assert_eq!(format!("{:?}", header), expected);
    }

    #[rstest]
    #[case::v4_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), FireAndForget, 4, Some(9)))]
    #[case::v4_addr(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), FireAndForget, 4, None))]
    #[case::v6_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlRecvSync { stream_id: 9}, 5, Some(15)))]
    #[case::v6_addr(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), ControlRecvSync { stream_id: 9}, 5, None))]
    #[case::seq_start(PacketHeader::new(None, RegularSequenced { stream_id: 0, first_message_offset: Some(999), packet_sequence_number: PacketId::from_raw(8)}, 6, Some(9999)))]
    #[case::seq_start(PacketHeader::new(None, RegularSequenced { stream_id: 0, first_message_offset: Some(999), packet_sequence_number: PacketId::from_raw(8)}, 6, None))]
    #[case::seq_continued(PacketHeader::new(None, RegularSequenced { stream_id: 4, first_message_offset: None, packet_sequence_number: PacketId::from_raw(6)}, 7, Some(23879289375)))]
    #[case::seq_continued(PacketHeader::new(None, RegularSequenced { stream_id: 4, first_message_offset: None, packet_sequence_number: PacketId::from_raw(6)}, 7, None))]
    #[case::oos(PacketHeader::new(None, FireAndForget, 8, Some(2000000000)))]
    #[case::oos(PacketHeader::new(None, FireAndForget, 8, None))]
    #[case::recv_sync_0(PacketHeader::new(None, ControlRecvSync { stream_id: 0 }, 11, Some(54378435786)))]
    #[case::recv_sync_0(PacketHeader::new(None, ControlRecvSync { stream_id: 0 }, 11, None))]
    #[case::recv_sync_3(PacketHeader::new(None, ControlRecvSync { stream_id: 3 }, 12, Some(213423)))]
    #[case::recv_sync_3(PacketHeader::new(None, ControlRecvSync { stream_id: 3 }, 12, None))]
    #[case::send_sync_0(PacketHeader::new(None, ControlSendSync { stream_id: 0 }, 13345346578, Some(999)))]
    #[case::send_sync_0(PacketHeader::new(None, ControlSendSync { stream_id: 0 }, 13345346578, None))]
    #[case::send_sync_3(PacketHeader::new(None, ControlSendSync { stream_id: 2 }, 4357686734895, Some(132)))]
    #[case::send_sync_3(PacketHeader::new(None, ControlSendSync { stream_id: 2 }, 4357686734895, None))]
    #[case::ping(PacketHeader::new(None, Ping, 1234, Some(5678)))]
    #[case::ping(PacketHeader::new(None, Ping, 567890, None))]
    fn test_packet_header_ser(#[case] header: PacketHeader) {
        let mut buf = FixedBuf::new(100);
        header.ser(&mut buf);
        let mut b: &[u8] = buf.as_mut();
        let deser = PacketHeader::deser(&mut b, PacketHeader::PROTOCOL_VERSION_1).unwrap();
        assert!(b.is_empty());
        assert_eq!(header, deser);
    }

    #[rstest]
    #[case::no_addr_none(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 0, Some(435764983763287)))]
    #[case::no_addr_none(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 0, None))]
    #[case::no_addr_0(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 0xffff_ffff_ffff, Some(3478943597)))]
    #[case::no_addr_0(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 0xffff_ffff_ffff, None))]
    #[case::no_addr_8000(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, Some(234682)))]
    #[case::no_addr_8000(PacketHeader::new(None, RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, None))]
    #[case::v4_addr_none(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 3, Some(1)))]
    #[case::v4_addr_none(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 3, None))]
    #[case::v4_addr_0(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, Some(234795)))]
    #[case::v4_addr_0(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, None))]
    #[case::v4_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, Some(345897345897)))]
    #[case::v4_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("1.2.3.4:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, None))]
    #[case::v6_addr_none(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 3, Some(23478)))]
    #[case::v6_addr_none(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: None, packet_sequence_number: PacketId::from_raw(1) }, 3, None))]
    #[case::v6_addr_0(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, Some(4378543224)))]
    #[case::v6_addr_0(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(0), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, None))]
    #[case::v6_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, Some(23456781234)))]
    #[case::v6_addr_8000(PacketHeader::new(Some(SocketAddr::from_str("[1111:2222::3333:4444]:888").unwrap()), RegularSequenced{ stream_id: 1, first_message_offset: Some(8000), packet_sequence_number: PacketId::from_raw(u64::MAX) }, 3, None))]
    fn test_packet_header_serialized_len_for_stream_header(#[case] header: PacketHeader) {
        let mut buf = FixedBuf::new(1000);
        header.ser(&mut buf);
        let expected = buf.len();
        assert_eq!(PacketHeader::serialized_len_for_stream_header(header.reply_to_address), expected);
    }

    #[rstest]
    #[case::seq_start(RegularSequenced { stream_id: 0, first_message_offset: Some(999), packet_sequence_number: PacketId::from_raw(8)}, "SEQ(0@8:999)")]
    #[case::seq_continued(RegularSequenced { stream_id: 4, first_message_offset: None, packet_sequence_number: PacketId::from_raw(6)}, "SEQ(4@6:-)")]
    #[case::oos(FireAndForget, "OOS")]
    #[case::recv_sync_0(ControlRecvSync { stream_id: 0 }, "RECV_SYNC(0)")]
    #[case::recv_sync_3(ControlRecvSync { stream_id: 3 }, "RECV_SYNC(3)")]
    #[case::send_sync_0(ControlSendSync { stream_id: 0 }, "SEND_SYNC(0)")]
    #[case::send_sync_3(ControlSendSync { stream_id: 2 }, "SEND_SYNC(2)")]
    #[case::ping(Ping, "PING")]
    fn test_packet_kind_debug(#[case] kind: PacketKind, #[case] expected: &str) {
        assert_eq!(format!("{:?}", kind), expected);
    }
}