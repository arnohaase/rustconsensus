use std::net::SocketAddr;
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::net::UdpSocket;
use crate::packet_header::{PacketHeader, PacketKind};

/// Convenience methods for the mechanics of sending different kinds of packet
#[async_trait]
pub trait RawSendSocket {
    async fn send_control_init(&self, reply_to: SocketAddr, to: SocketAddr, stream_id: u16);
}

#[async_trait]
impl RawSendSocket for UdpSocket {
    async fn send_control_init(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16) -> anyhow::Result<()> {
        let header = PacketHeader::new(reply_to, PacketKind::ControlInit { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool
        header.ser(&mut send_buf);

        self.send_to(&send_buf, to).await?;
        Ok(())
    }
}