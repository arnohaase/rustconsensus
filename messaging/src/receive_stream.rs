use crate::control_messages::ControlMessageSendSync;
use std::net::SocketAddr;

pub struct ReceiveStream {

}
impl ReceiveStream {
    pub fn new() -> ReceiveStream {
        todo!()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        todo!()
    }

    pub async fn on_send_sync_message(&mut self, message: ControlMessageSendSync) {
        todo!()
    }

    pub async fn on_packet(&mut self, sequence_number: u32, first_message_offset: u16, payload: &[u8]) {


        todo!()
    }
}
