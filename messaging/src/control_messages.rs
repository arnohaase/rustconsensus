use bytes::{Buf, BytesMut};
use crate::packet_id::PacketId;

#[derive(Debug)]
pub struct ControlMessageRecvSync {
    pub receive_buffer_high_water_mark: Option<PacketId>,
    pub receive_buffer_low_water_mark: Option<PacketId>,
    pub receive_buffer_ack_threshold: Option<PacketId>,
}
impl ControlMessageRecvSync {
    pub fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageRecvSync> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ControlMessageSendSync {
    pub send_buffer_high_water_mark: PacketId,
    pub send_buffer_low_water_mark: PacketId,
}
impl ControlMessageSendSync {
    pub fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageSendSync> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ControlMessageNak {
    pub packet_id_resend_set: Vec<PacketId>,
}

impl ControlMessageNak {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageNak> {
        todo!()
    }
}
