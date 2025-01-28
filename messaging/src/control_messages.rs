use bytes::{Buf, BufMut, BytesMut};
use crate::packet_id::PacketId;
use bytes_varint::try_get_fixed::TryGetFixedSupport;
use bytes_varint::{VarIntSupport, VarIntSupportMut};

#[derive(Debug)]
pub struct ControlMessageRecvSync {
    //TODO update specification to reflect this

    /// The id *after* the highest received packet (to allow for 0 initially)
    pub receive_buffer_high_water_mark: PacketId,
    /// The id *after* the lowest packet that was not fully dispatched yet.
    pub receive_buffer_low_water_mark: PacketId,
    /// All ids *below* (excluding) this id are acknowledged and can be removed from the send buffer.
    ///
    /// NB: This can be higher than the 'low-water mark' for multi-packet messages
    pub receive_buffer_ack_threshold: PacketId,
}
impl ControlMessageRecvSync {
    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.receive_buffer_high_water_mark.to_raw());
        buf.put_u64(self.receive_buffer_low_water_mark.to_raw());
        buf.put_u64(self.receive_buffer_ack_threshold.to_raw());
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageRecvSync> {
        let high_water_mark = buf.try_get_u64()?;
        let low_water_mark = buf.try_get_u64()?;
        let ack_threshold = buf.try_get_u64()?;
        Ok(ControlMessageRecvSync {
            receive_buffer_high_water_mark: PacketId::from_raw(high_water_mark),
            receive_buffer_low_water_mark: PacketId::from_raw(low_water_mark),
            receive_buffer_ack_threshold: PacketId::from_raw(ack_threshold),
        })
    }
}

#[derive(Debug)]
pub struct ControlMessageSendSync {
    /// The id *after* the highest sent packet
    pub send_buffer_high_water_mark: PacketId,
    /// The id *after* the oldest packet still in the send buffer
    pub send_buffer_low_water_mark: PacketId,
}
impl ControlMessageSendSync {
    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.send_buffer_high_water_mark.to_raw());
        buf.put_u64(self.send_buffer_low_water_mark.to_raw());
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageSendSync> {
        let high_water_mark = buf.try_get_u64()?;
        let low_water_mark = buf.try_get_u64()?;
        Ok(ControlMessageSendSync {
            send_buffer_high_water_mark: PacketId::from_raw(high_water_mark),
            send_buffer_low_water_mark: PacketId::from_raw(low_water_mark),
        })
    }
}

#[derive(Debug)]
pub struct ControlMessageNak {
    pub packet_id_resend_set: Vec<PacketId>,
}

impl ControlMessageNak {
    fn ser(&self, buf: &mut BytesMut) {
        buf.put_usize_varint(self.packet_id_resend_set.len());
        for &packet_id in &self.packet_id_resend_set {
            buf.put_u64(packet_id.to_raw());
        }
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageNak> {
        let num_naks = buf.try_get_usize_varint()?;
        let mut packet_id_resend_set = Vec::with_capacity(num_naks);
        for _ in 0..num_naks {
            packet_id_resend_set.push(PacketId::from_raw(buf.try_get_u64()?));
        }
        Ok(ControlMessageNak { packet_id_resend_set })
    }
}
