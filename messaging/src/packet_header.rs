use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};
use crate::packet_id::PacketId;
use bytes_varint::try_get_fixed::TryGetFixedSupport;


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


//TODO human readable Debug
pub struct PacketHeader {
    pub checksum: u32, //TODO do the checksum at the encryption level?
    pub protocol_version: u8,
    pub reply_to_address: Option<SocketAddr>,
    pub packet_kind: PacketKind,
}
impl PacketHeader {
    pub const PROTOCOL_VERSION_1: u8 = 0;
    pub const OFFSET_MESSAGE_CONTINUES: u16 = u16::MAX;

    pub fn new(reply_to_address: Option<SocketAddr>, packet_kind: PacketKind) -> PacketHeader {
        PacketHeader {
            checksum: 0,
            protocol_version: Self::PROTOCOL_VERSION_1,
            reply_to_address,
            packet_kind,
        }
    }

    //TODO unit test
    pub fn serialized_len_for_stream_header(reply_to_address: Option<SocketAddr>) -> usize {
        let reply_to_len = match &reply_to_address {
            None => 0,
            Some(SocketAddr::V4(_)) => 4 + 2,
            Some(SocketAddr::V6(_)) => 16 + 2,
        };

        size_of::<u8>()          // protocol version
            + size_of::<u32>()   // checksum
            + size_of::<u8>()    // flags
            + reply_to_len
            + size_of::<u16>()   // stream id
            + size_of::<u16>()   // first message offset
            + size_of::<u64>()   // packet number
    }

    //TODO unit test
    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u8(self.protocol_version);
        buf.put_u32(self.checksum);

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

    //TODO unit test
    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<PacketHeader> {
        let protocol_version = buf.try_get_u8()?;
        if protocol_version != Self::PROTOCOL_VERSION_1 {
            return Err(anyhow::anyhow!("Unsupported protocol version {}", protocol_version));
        }

        let checksum = buf.try_get_u32()?; //TODO verify

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

    pub fn init_checksum(buf: &mut [u8]) {
        //TODO init checksum - or handle checksumming in the encryption wrapper?
    }
}

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
