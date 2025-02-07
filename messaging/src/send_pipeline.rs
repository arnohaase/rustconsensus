use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::UdpSocket;
use tracing::{error, trace};
use crate::packet_header::PacketHeader;

/// This is an abstraction for sending a buffer on a UDP socket, introduced to facilitate mocking
///  the I/O part away for testing
#[async_trait]
pub trait SendSocket: Send + Sync + 'static {
    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]);

    fn local_addr(&self) -> SocketAddr;
}

#[async_trait]
impl SendSocket for Arc<UdpSocket> {
    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]) {
        trace!("UDP socket: sending packet to {:?}", to);

        if let Err(e) = self.send_to(&packet_buf, to).await {
            error!("error sending UDP packet to {:?}: {}", to, e);
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.as_ref().local_addr()
            .expect("UdpSocket should have an initialized local addr")
    }
}


#[derive(Clone)]
pub struct SendPipeline {
    socket: Arc<dyn SendSocket>,
}

impl SendPipeline {
    pub fn new(socket: Arc<dyn SendSocket>) -> SendPipeline {
        SendPipeline { socket }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local_addr()
    }

    pub async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]) {
        PacketHeader::init_checksum(packet_buf);
        self.socket.do_send_packet(to, packet_buf).await;
    }

    pub async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]) {
        //TODO traffic shaping
        self.socket.do_send_packet(to, packet_buf).await;
    }
}