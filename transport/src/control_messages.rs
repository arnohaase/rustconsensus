use std::fmt::{Debug, Formatter};
use bytes::{Buf, BufMut};
use crate::packet_id::PacketId;
use bytes_varint::{VarIntSupport, VarIntSupportMut};

#[derive(Clone, Eq, PartialEq)]
pub struct ControlMessageRecvSync {
    /// The id *after* the highest received packet (to allow for 0 initially)
    pub receive_buffer_high_water_mark: PacketId,

    /// The id *after* the lowest packet that was not fully dispatched yet.
    pub receive_buffer_low_water_mark: PacketId,

    /// All ids *below* (excluding) this id are acknowledged and can be removed from the send buffer.
    ///
    /// NB: This can be higher than the 'low-water mark' for multi-packet messages
    pub receive_buffer_ack_threshold: PacketId,

    /// a list of packets that the receiver considers missing
    pub packet_id_resend_set: Vec<PacketId>,
}
impl Debug for ControlMessageRecvSync {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RECV_SYNC ({}/{}/{}: {:?})",
            self.receive_buffer_low_water_mark.to_raw(),
            self.receive_buffer_ack_threshold.to_raw(),
            self.receive_buffer_high_water_mark.to_raw(),
            self.packet_id_resend_set
                .iter()
                .map(|id| id.to_raw())
                .collect::<Vec<_>>()
        )
    }
}
impl ControlMessageRecvSync {
    pub fn ser(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.receive_buffer_high_water_mark.to_raw());
        buf.put_u64(self.receive_buffer_low_water_mark.to_raw());
        buf.put_u64(self.receive_buffer_ack_threshold.to_raw());

        buf.put_usize_varint(self.packet_id_resend_set.len());
        for &packet_id in &self.packet_id_resend_set {
            buf.put_u64(packet_id.to_raw());
        }
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageRecvSync> {
        let high_water_mark = buf.try_get_u64()?;
        let low_water_mark = buf.try_get_u64()?;
        let ack_threshold = buf.try_get_u64()?;

        let num_naks = buf.try_get_usize_varint()?;
        let mut packet_id_resend_set = Vec::with_capacity(num_naks);
        for _ in 0..num_naks {
            packet_id_resend_set.push(PacketId::from_raw(buf.try_get_u64()?));
        }

        Ok(ControlMessageRecvSync {
            receive_buffer_high_water_mark: PacketId::from_raw(high_water_mark),
            receive_buffer_low_water_mark: PacketId::from_raw(low_water_mark),
            receive_buffer_ack_threshold: PacketId::from_raw(ack_threshold),
            packet_id_resend_set,
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ControlMessageSendSync {
    /// The id *after* the highest sent packet
    pub send_buffer_high_water_mark: PacketId,
    /// The id *after* the oldest packet still in the send buffer
    pub send_buffer_low_water_mark: PacketId,
}
impl Debug for ControlMessageSendSync {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SEND_SYNC ({}/{})",
            self.send_buffer_low_water_mark.to_raw(),
            self.send_buffer_high_water_mark.to_raw(),
        )
    }
}
impl ControlMessageSendSync {
    pub fn ser(&self, buf: &mut impl BufMut) {
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case(0, 0, 0, vec![])]
    #[case(0, 1, 2, vec![])]
    #[case(1231231231, 1231231236, 1231231239, vec![])]
    #[case(u64::MAX, u64::MAX, u64::MAX, vec![])]

    #[case(0, 0, 0, vec![])]
    #[case(0, 0, 0, vec![0])]
    #[case(0, 0, 0, vec![1])]
    #[case(0, 0, 0, vec![u64::MAX])]
    #[case(0, 0, 0, vec![1, 2])]
    #[case(0, 0, 0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])]
    fn test_ser_recv_sync(#[case] low_water_mark: u64, #[case] ack_threshold: u64, #[case] high_water_mark: u64, #[case] nak_ids: Vec<u64>) {
        let nak_ids = nak_ids.into_iter().map(|id| PacketId::from_raw(id)).collect();
        let original = ControlMessageRecvSync {
            receive_buffer_low_water_mark: PacketId::from_raw(low_water_mark),
            receive_buffer_ack_threshold: PacketId::from_raw(ack_threshold),
            receive_buffer_high_water_mark: (PacketId::from_raw(high_water_mark)),
            packet_id_resend_set: nak_ids,
        };

        let mut buf = BytesMut::new();
        original.ser(&mut buf);
        let mut b: &[u8] = &buf;
        let deser = ControlMessageRecvSync::deser(&mut b).unwrap();
        assert!(b.is_empty());
        assert_eq!(deser, original);
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 2)]
    #[case(1231231231231231, 1231231231231239)]
    #[case(u64::MAX, u64::MAX)]
    fn test_ser_send_sync(#[case] low_water_mark: u64, #[case] high_water_mark: u64) {
        let original = ControlMessageSendSync {
            send_buffer_low_water_mark: PacketId::from_raw(low_water_mark),
            send_buffer_high_water_mark: PacketId::from_raw(high_water_mark),
        };

        let mut buf = BytesMut::new();
        original.ser(&mut buf);
        let mut b: &[u8] = &buf;
        let deser = ControlMessageSendSync::deser(&mut b).unwrap();
        assert!(b.is_empty());
        assert_eq!(deser, original);
    }
}
