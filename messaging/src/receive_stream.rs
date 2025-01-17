use crate::control_messages::ControlMessageSendSync;
use crate::packet_id::PacketId;
use crate::send_socket::SendSocket;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

struct ReceiveStreamConfig {

}

struct ReceiveStreamInner {
    config: Arc<ReceiveStreamConfig>,

    stream_id: u16,
    send_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,

    /// This is the
    receive_window_lower_bound: Option<PacketId>,

    high_water_mark: Option<PacketId>,
    low_water_mark: Option<PacketId>,
    ack_threshold: Option<PacketId>,
    receive_buffer: BTreeMap<PacketId, Vec<u8>>,
}
impl ReceiveStreamInner {
    async fn do_send_init(&self) {
        self.send_socket.send_control_init(self.self_reply_to_addr, self.peer_addr, self.stream_id)
            .await
    }

    async fn do_send_recv_sync(&self) {
        // self.send_socket.send_recv_sync(self.self_reply_to_addr, self.peer_addr, self.stream_id, self.high_water_mark, self.low_water_mark, self.ack_threshold)
        //     .await
    }
}

pub struct ReceiveStream {
    inner: RwLock<ReceiveStreamInner>,
}

impl ReceiveStream {
    pub fn new() -> ReceiveStream {
        todo!()
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn do_send_init(&self) {
        self.inner.read().await
            .do_send_init().await;
    }

    pub async fn do_send_recv_sync(&self) {
        self.inner.read().await
            .do_send_recv_sync().await;
    }

    pub async fn on_send_sync_message(&self, message: ControlMessageSendSync) {
        let mut inner = self.inner.write().await;

        // adjust the receive window based on the send window:
        // *



        // we only have a defined receive window once we received at least one packet
        if let Some(high_water_mark) = inner.high_water_mark {

            // evict packets from the that only contain messages that have a missing part
            //  below the new sender's low water mark: the sender does not have that missing
            //  packet in its send buffer any longer, so it is not going to arrive







            // NB: in-flight packets are ignored




            message.send_buffer_low_water_mark;



        }





        // the send buffer's low water mark now determines the valid range of packet IDs -
        //  at most a range of u32::MAX / 4 packets, but possibly limited by the config
        //TODO the high water mark should determine the window, right?



        //TODO message.send_buffer_low_water_mark

        todo!()
    }

    pub async fn on_packet(&self, sequence_number: PacketId, first_message_offset: Option<u16>, payload: &[u8]) {
        let mut inner = self.inner.write().await;

        inner.receive_buffer.insert(sequence_number, payload.to_vec());

        todo!()
    }
}
