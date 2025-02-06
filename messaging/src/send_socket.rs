use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::UdpSocket;
use tracing::{error, trace};
use crate::packet_header::PacketHeader;


/// This is an abstraction for sending a buffer on a UDP socket, introduced to facilitate mocking
///  the I/O part away for testing
#[async_trait]
pub trait RawSendSocket: Send + Sync + 'static {
    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]);

    fn local_addr(&self) -> SocketAddr;
}

#[async_trait]
impl RawSendSocket for Arc<UdpSocket> {
    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]) {
        trace!("UDP socket: sending packet to {:?}", to);

        //TODO traffic shaping
        if let Err(e) = self.send_to(&packet_buf, to).await {
            error!("error sending UDP packet to {:?}: {}", to, e);
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.as_ref().local_addr()
            .expect("UdpSocket should have an initialized local addr")
    }
}


/// Convenience methods for the mechanics of sending different kinds of packet
pub struct SendSocket {
    socket: Arc<dyn RawSendSocket>,
}

impl SendSocket {
    pub fn new(socket: Arc<dyn RawSendSocket>) -> SendSocket {
        SendSocket { socket }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local_addr()
    }

    pub async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]) {
        PacketHeader::init_checksum(packet_buf);
        self.socket.do_send_packet(to, packet_buf).await;
    }

    pub async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]) {
        self.socket.do_send_packet(to, packet_buf).await;
    }
}