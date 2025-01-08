use std::net::SocketAddr;
use bytes::{Buf, BytesMut};

pub struct PacketHeader {
    pub checksum: u32,
    pub protocol_version: u8,
    pub reply_to_address: Option<SocketAddr>,
    pub packet_kind: PacketKind,
}
impl PacketHeader {
    const PROTOCOL_VERSION_1: u8 = 0;

    pub fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<PacketHeader> {
        todo!()
    }
}

pub enum PacketKind {
    RegularSequenced {
        stream_id: u16,
        first_message_offset: usize,
        packet_sequence_number: u32,
    },
    OutOfSequence,
    ControlInit { stream_id: u16 },
    ControlRecvSync { stream_id: u16 },
    ControlSendSync { stream_id: u16 },
    ControlNak { stream_id: u16 },
}
