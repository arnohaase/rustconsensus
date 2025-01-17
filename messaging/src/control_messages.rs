use bytes::{Buf, BytesMut};
use crate::packet_id::PacketId;

#[derive(Debug)]
pub struct ControlMessageRecvSync {
    //TODO update specification to reflect this

    /// The id *after* the highest received packet (to allow for 0 initially)
    pub receive_buffer_high_water_mark: PacketId,
    /// The id *after* the lowest packet that was not fully dispatched yet.
    pub receive_buffer_low_water_mark: PacketId,
    /// All ids *below* (excluding) this id are acknowledged and can be removed from the send buffer.
    ///
    /// NB: This can be higher than the 'low water mark' for multi-packet messages
    pub receive_buffer_ack_threshold: PacketId,
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
    /// The id *after* the most recently sent packet
    pub send_buffer_high_water_mark: PacketId,
    /// The id *after* the oldest packet still in the send buffer
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
