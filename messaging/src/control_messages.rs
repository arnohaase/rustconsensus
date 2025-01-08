use bytes::{Bytes, BytesMut};

struct ControlMessageSync {
    request_sync_reply: bool,
    send_buffer_high_water_mark: u32,
    send_buffer_low_water_mark: u32,
    receive_buffer_high_water_mark: Option<u32>,
    receive_buffer_low_water_mark: Option<u32>,
    receive_buffer_ack_threshold: Option<u32>,
}
impl ControlMessageSync {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    fn deser(buf: &mut Bytes) -> anyhow::Result<ControlMessageSync> {
        todo!()
    }
}

struct ControlMessageNak {
    packet_id_resend_set: Vec<u32>,
}

impl ControlMessageNak {
    fn ser(&self, buf: &mut BytesMut) {
        todo!()
    }

    fn deser(buf: &mut Bytes) -> anyhow::Result<ControlMessageNak> {
        todo!()
    }
}
