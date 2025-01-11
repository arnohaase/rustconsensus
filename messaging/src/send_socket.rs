use std::net::SocketAddr;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use tokio::net::UdpSocket;
use crate::packet_header::{PacketHeader, PacketKind};

/// Convenience methods for the mechanics of sending different kinds of packet
#[async_trait]
pub trait SendSocket {
    async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]) -> anyhow::Result<()>;

    async fn send_control_init(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16) -> anyhow::Result<()>;

    async fn send_send_sync(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, high_water_mark: u32, low_water_mark: u32) -> anyhow::Result<()>;
}

#[async_trait]
impl SendSocket for UdpSocket {
    async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]) -> anyhow::Result<()> {
        PacketHeader::init_checksum(packet_buf);
        self.send_to(&packet_buf, to).await?;
        Ok(())
    }

    async fn send_control_init(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16) -> anyhow::Result<()> {
        let header = PacketHeader::new(reply_to, PacketKind::ControlInit { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        self.finalize_and_send_packet(to, &mut send_buf).await
    }

    async fn send_send_sync(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, high_water_mark: u32, low_water_mark: u32) -> anyhow::Result<()> {
        let header = PacketHeader::new(reply_to, PacketKind::ControlSendSync { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        send_buf.put_u32(high_water_mark);
        send_buf.put_u32(low_water_mark);

        self.finalize_and_send_packet(to, &mut send_buf).await
    }
}