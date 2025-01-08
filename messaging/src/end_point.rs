use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::Arc;
use rustc_hash::FxHashMap;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};
use crate::control_messages::ControlMessageNak;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::receive_stream::ReceiveStream;
use crate::send_stream::SendStream;

struct EndPoint {
    receive_socket: Arc<UdpSocket>,
    send_socket_v4: Arc<UdpSocket>,
    send_socket_v6: Arc<UdpSocket>,
    receive_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<RwLock<ReceiveStream>>>>,
    send_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<RwLock<SendStream>>>>,
}
impl EndPoint {
    async fn new(addrs: impl ToSocketAddrs) -> anyhow::Result<EndPoint> {
        //TODO "don't fragment" flag
        let receive_socket = Arc::new(UdpSocket::bind(addrs).await?);
        let (send_socket_v4, send_socket_v6) = if receive_socket.local_addr()?.is_ipv6() {
            (Arc::new(UdpSocket::bind("0.0.0.0:0")), receive_socket.clone())
        }
        else {
            (receive_socket.clone(), Arc::new(UdpSocket::bind("[::]:0")))
        };

        Ok(EndPoint {
            receive_socket,
            send_socket_v4,
            send_socket_v6,
            receive_streams: Default::default(),
            send_streams: Default::default(),
        })
    }

    async fn recv_loop(&self)  {
        let mut buf = [0u8; 1500]; //TODO configurable size
        loop {
            let (num_read, from) = match self.receive_socket.recv_from(&mut buf).await {
                Ok(x) => {
                    x
                }
                Err(e) => {
                    error!("socket error: {}", e);
                    continue;
                }
            };

            let parse_buf = &mut &buf[..num_read];
            let packet_header = match PacketHeader::deser(parse_buf) {
                Ok(header) => {
                    header
                },
                Err(e) => {
                    warn!("received packet with unparsable header from {:?}, dropping", from);
                    continue;
                },
            };

            //TODO verify checksum

            let peer_addr = packet_header.reply_to_address
                .unwrap_or(from);

            match packet_header.packet_kind {
                PacketKind::RegularSequenced { stream_id, .. } => { todo!() }
                PacketKind::OutOfSequence => { todo!() }
                PacketKind::ControlInit { stream_id } => { Self::handle_init_message(self.get_send_stream(peer_addr, stream_id)) }
                PacketKind::ControlRecvSync { stream_id } => { todo!() }
                PacketKind::ControlSendSync { stream_id } => { todo!() }
                PacketKind::ControlNak { stream_id } => { Self::handle_nak(parse_buf, self.get_send_stream(peer_addr, stream_id)) }
            }
        }
    }

    async fn get_receive_stream(&self, addr: SocketAddr, stream_id: u16) -> Arc<RwLock<ReceiveStream>> {
        match self.receive_streams
            .lock().await
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Arc::new(RwLock::new(ReceiveStream::new()))).clone()
        }
    }

    async fn get_send_stream(&self, addr: SocketAddr, stream_id: u16) -> Arc<RwLock<SendStream>> {
        match self.send_streams
            .lock().await
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Arc::new(RwLock::new(SendStream::new()))).clone()
        }
    }

    async fn handle_init_message(send_stream: Arc<RwLock<SendStream>>) {
        send_stream.write().await
            .on_init_message().await
    }

    async fn handle_nak(mut parse_buf: &[u8], send_stream: Arc<RwLock<SendStream>>) {
        let nak_message = match ControlMessageNak::deser(&mut parse_buf) {
            Ok(msg) => msg,
            Err(_) => {
                warn!("received unparseable NAK message from {:?}", send_stream.read().await.peer_addr());
                return;
            }
        };

        send_stream.read().await
            .on_nak_message(nak_message).await;
    }
}
