use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::Arc;
use rustc_hash::FxHashMap;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::Mutex;
use tracing::{error, warn};
use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync, ControlMessageSendSync};
use crate::message_dispatcher::MessageDispatcher;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::receive_stream::{ReceiveStream, ReceiveStreamConfig};
use crate::send_stream::SendStream;


//TODO everywhere: stream -> channel

/// EndPoint is the place where all other parts of the protocol come together: It listens on a
///  UdpSocket, dispatching incoming packets to their corresponding channels, and has an API for
///  application code to send messages.
pub struct EndPoint {
    receive_socket: Arc<UdpSocket>,
    send_socket_v4: Arc<UdpSocket>,
    send_socket_v6: Arc<UdpSocket>,
    receive_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<ReceiveStream>>>, //TODO persistent collection instead of Mutex
    send_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<SendStream>>>,
    message_dispatcher: Arc<dyn MessageDispatcher>,
}
impl EndPoint {
    async fn new(addrs: impl ToSocketAddrs, message_dispatcher: Arc<dyn MessageDispatcher>) -> anyhow::Result<EndPoint> {
        //TODO "don't fragment" flag
        let receive_socket = Arc::new(UdpSocket::bind(addrs).await?);
        let (send_socket_v4, send_socket_v6) = if receive_socket.local_addr()?.is_ipv6() {
            (Arc::new(UdpSocket::bind("0.0.0.0:0").await?), receive_socket.clone())
        }
        else {
            (receive_socket.clone(), Arc::new(UdpSocket::bind("[::]:0").await?))
        };

        Ok(EndPoint {
            receive_socket,
            send_socket_v4,
            send_socket_v6,
            receive_streams: Default::default(),
            send_streams: Default::default(),
            message_dispatcher,
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
                PacketKind::RegularSequenced { stream_id, first_message_offset, packet_sequence_number} => {
                    self.get_receive_stream(peer_addr, stream_id).await
                        .on_packet(packet_sequence_number, first_message_offset, parse_buf).await
                },
                PacketKind::OutOfSequence => self.message_dispatcher.on_message(peer_addr, None, parse_buf).await,
                PacketKind::ControlInit { stream_id } => Self::handle_init_message(self.get_send_stream(peer_addr, stream_id).await).await,
                PacketKind::ControlRecvSync { stream_id } => Self::handle_recv_sync(parse_buf, self.get_send_stream(peer_addr, stream_id).await).await,
                PacketKind::ControlSendSync { stream_id } => Self::handle_send_sync(parse_buf, self.get_receive_stream(peer_addr, stream_id).await).await,
                PacketKind::ControlNak { stream_id } => Self::handle_nak(parse_buf, self.get_send_stream(peer_addr, stream_id).await).await,
            }
        }
    }

    fn get_receive_config(&self, stream_id: u16) -> Arc<ReceiveStreamConfig> {
        todo!()
    }

    fn get_send_socket(&self, peer_addr: SocketAddr) -> Arc<UdpSocket> {
        if peer_addr.is_ipv4() {
            self.send_socket_v4.clone()
        }
        else {
            self.send_socket_v6.clone()
        }
    }

    async fn get_receive_stream(&self, addr: SocketAddr, stream_id: u16) -> Arc<ReceiveStream> {
        match self.receive_streams
            .lock().await
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let recv_strm = ReceiveStream::new(
                    self.get_receive_config(stream_id),
                    stream_id,
                    addr,
                    self.get_send_socket(addr),
                    self.receive_socket.local_addr().unwrap(),
                    self.message_dispatcher.clone(),
                );
                e.insert(Arc::new(recv_strm)).clone()
            }
        }
    }

    async fn get_send_stream(&self, addr: SocketAddr, stream_id: u16) -> Arc<SendStream> {
        match self.send_streams
            .lock().await
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Arc::new(SendStream::new())).clone()
        }
    }

    async fn handle_init_message(send_stream: Arc<SendStream>) {
        send_stream.on_init_message().await
    }

    async fn handle_recv_sync(mut parse_buf: &[u8], send_stream: Arc<SendStream>) {
        let sync_message = match ControlMessageRecvSync::deser(&mut parse_buf) {
            Ok(msg) => msg,
            Err(_) => {
                warn!("received unparseable RECV_SYNC message from {:?}", send_stream.peer_addr().await);
                return;
            }
        };

        send_stream.on_recv_sync_message(sync_message).await
    }

    async fn handle_send_sync(mut parse_buf: &[u8], receive_stream: Arc<ReceiveStream>) {
        let sync_message = match ControlMessageSendSync::deser(&mut parse_buf) {
            Ok(msg) => msg,
            Err(_) => {
                warn!("received unparseable SEND_SYNC message from {:?}", receive_stream.peer_addr().await);
                return;
            }
        };

        receive_stream.on_send_sync_message(sync_message).await
    }

    async fn handle_nak(mut parse_buf: &[u8], send_stream: Arc<SendStream>) {
        let nak_message = match ControlMessageNak::deser(&mut parse_buf) {
            Ok(msg) => msg,
            Err(_) => {
                warn!("received unparseable NAK message from {:?}", send_stream.peer_addr().await);
                return;
            }
        };

        send_stream.on_nak_message(nak_message).await;
    }
}
