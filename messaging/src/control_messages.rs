use bytes::{Buf, Bytes, BytesMut};

pub struct ControlMessageRecvSync {
    opaque_timestamp: u64,
    receive_buffer_high_water_mark: Option<u32>,
    receive_buffer_low_water_mark: Option<u32>,
    receive_buffer_ack_threshold: Option<u32>,
}
impl ControlMessageRecvSync {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    fn deser(buf: &mut Bytes) -> anyhow::Result<ControlMessageRecvSync> {
        todo!()
    }
}

pub struct ControlMessageSendSync {
    opaque_timestamp: u64,
    send_buffer_high_water_mark: u32,
    send_buffer_low_water_mark: u32,
}
impl ControlMessageSendSync {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    fn deser(buf: &mut Bytes) -> anyhow::Result<ControlMessageSendSync> {
        todo!()
    }
}

pub struct ControlMessageNak {
    packet_id_resend_set: Vec<u32>,
}

impl ControlMessageNak {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<ControlMessageNak> {
        todo!()
    }
}
