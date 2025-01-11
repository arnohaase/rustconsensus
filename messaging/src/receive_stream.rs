use crate::control_messages::ControlMessageSendSync;
use std::net::SocketAddr;
use tokio::sync::RwLock;

struct ReceiveStreamData {

}

pub struct ReceiveStream {
    data: RwLock<ReceiveStreamData>,
}

impl ReceiveStream {
    pub fn new() -> ReceiveStream {
        todo!()
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        todo!()
    }

    pub async fn on_send_sync_message(&self, message: ControlMessageSendSync) {
        todo!()
    }

    pub async fn on_packet(&self, sequence_number: u32, first_message_offset: u16, payload: &[u8]) {


        todo!()
    }
}
