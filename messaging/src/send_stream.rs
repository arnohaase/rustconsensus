use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

pub struct SendStream {
    send_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
}

impl SendStream {
    pub fn new() -> SendStream {
        todo!()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub async fn on_init_message(&mut self) {
        todo!()
    }

    pub async fn on_recv_sync_message(&mut self, message: ControlMessageRecvSync) {
        todo!()
    }

    pub async fn on_nak_message(&self, message: ControlMessageNak) {
        todo!()
    }
}