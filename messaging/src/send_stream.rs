use std::collections::BTreeMap;
use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync};
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::BytesMut;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

pub struct SendStreamConfig {

}

struct SendStreamData {
    send_buffer: BTreeMap<u32, BytesMut>,
    work_in_progress: Option<BytesMut>,
}

pub struct SendStream {
    config: Arc<SendStreamConfig>,
    send_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,
    data: Arc<RwLock<SendStreamData>>,
}

impl SendStream {
    pub fn new() -> SendStream {
        todo!()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub async fn on_init_message(&self) {
        todo!()
    }

    pub async fn on_recv_sync_message(&self, message: ControlMessageRecvSync) {
        todo!()
    }

    pub async fn on_nak_message(&self, message: ControlMessageNak) {
        todo!()
    }

    pub async fn send_message(&mut self, message: &[u8]) {

    }
}
