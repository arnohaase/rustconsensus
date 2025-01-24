use std::net::SocketAddr;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use bytes_varint::VarIntSupportMut;
use tokio::net::UdpSocket;
use tracing::error;
use crate::control_messages::{ControlMessageRecvSync, ControlMessageSendSync};
use crate::packet_header::{PacketHeader, PacketKind};
use crate::packet_id::PacketId;

/// Convenience methods for the mechanics of sending different kinds of packet
#[async_trait]
pub trait SendSocket {
    async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]);

    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]);

    async fn send_control_init(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16);

    async fn send_nak(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, nak_packets: &[PacketId]);

    async fn send_recv_sync(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, high_water_mark: PacketId, low_water_mark: PacketId, ack_threshold: PacketId);

    async fn send_send_sync(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, high_water_mark: PacketId, low_water_mark: PacketId);
}

#[async_trait]
impl SendSocket for UdpSocket {
    async fn finalize_and_send_packet(&self, to: SocketAddr, packet_buf: &mut [u8]) {
        PacketHeader::init_checksum(packet_buf);
        self.do_send_packet(to, packet_buf).await;
    }

    async fn do_send_packet(&self, to: SocketAddr, packet_buf: &[u8]) {
        //TODO traffic shaping
        if let Err(e) = self.send_to(&packet_buf, to).await {
            error!("error sending UDP packet to {:?}: {}", to, e);
        }
    }


    async fn send_control_init(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16) {
        let header = PacketHeader::new(reply_to, PacketKind::ControlInit { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        self.finalize_and_send_packet(to, &mut send_buf).await
    }

    async fn send_nak(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, nak_packets: &[PacketId]) { //TODO StreamId type instead of u16
        let header = PacketHeader::new(reply_to, PacketKind::ControlNak { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        send_buf.put_usize_varint(nak_packets.len());
        for &packet_id in nak_packets {
            send_buf.put_u64(packet_id.to_raw());
        }

        self.finalize_and_send_packet(to, &mut send_buf).await;
    }


    async fn send_recv_sync(
        &self,
        reply_to: Option<SocketAddr>,
        to: SocketAddr,
        stream_id: u16,
        high_water_mark: PacketId,
        low_water_mark: PacketId,
        ack_threshold: PacketId,
    ) {
        let header = PacketHeader::new(reply_to, PacketKind::ControlRecvSync { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        ControlMessageRecvSync {
            receive_buffer_high_water_mark: high_water_mark,
            receive_buffer_low_water_mark: low_water_mark,
            receive_buffer_ack_threshold: ack_threshold,
        }.ser(&mut send_buf);

        self.finalize_and_send_packet(to, &mut send_buf).await
    }

    //TODO rename 'high water mark' and 'low water mark' to 'send buffer upper bound' and 'send buffer lower bound'
    async fn send_send_sync(&self, reply_to: Option<SocketAddr>, to: SocketAddr, stream_id: u16, high_water_mark: PacketId, low_water_mark: PacketId) {
        let header = PacketHeader::new(reply_to, PacketKind::ControlSendSync { stream_id });

        let mut send_buf = BytesMut::with_capacity(1500); //TODO from pool?
        header.ser(&mut send_buf);

        ControlMessageSendSync {
            send_buffer_high_water_mark: high_water_mark,
            send_buffer_low_water_mark: low_water_mark,
        }.ser(&mut send_buf);

        self.finalize_and_send_packet(to, &mut send_buf).await
    }
}