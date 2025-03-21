use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::Arc;
use rustc_hash::FxHashMap;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::Mutex;
use tracing::{error, info, trace, warn};
use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync, ControlMessageSendSync};
use crate::message_dispatcher::MessageDispatcher;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::receive_stream::{ReceiveStream, ReceiveStreamConfig};
use crate::send_pipeline::SendPipeline;
use crate::send_stream::{SendStream, SendStreamConfig};


//TODO unit test

//TODO everywhere: stream -> channel

/// EndPoint is the place where all other parts of the protocol come together: It listens on a
///  UdpSocket, dispatching incoming packets to their corresponding channels, and has an API for
///  application code to send messages.
pub struct EndPoint {
    receive_socket: Arc<UdpSocket>,
    send_socket_v4: Arc<SendPipeline>,
    send_socket_v6: Arc<SendPipeline>,
    receive_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<ReceiveStream>>>, //TODO persistent collection instead of Mutex
    send_streams: Mutex<FxHashMap<(SocketAddr, u16), Arc<SendStream>>>,
    message_dispatcher: Arc<dyn MessageDispatcher>,
    default_receive_config: Arc<ReceiveStreamConfig>,
    specific_receive_configs: FxHashMap<u16, Arc<ReceiveStreamConfig>>,
    default_send_config: Arc<SendStreamConfig>,
    specific_send_configs: FxHashMap<u16, Arc<SendStreamConfig>>,
}
impl EndPoint {
    pub async fn new(
        addrs: impl ToSocketAddrs,
        message_dispatcher: Arc<dyn MessageDispatcher>,
        default_receive_config: Arc<ReceiveStreamConfig>,
        specific_receive_configs: FxHashMap<u16, Arc<ReceiveStreamConfig>>,
        default_send_config: Arc<SendStreamConfig>,
        specific_send_configs: FxHashMap<u16, Arc<SendStreamConfig>>,
    ) -> anyhow::Result<EndPoint> {
        //TODO "don't fragment" flag
        let receive_socket = Arc::new(UdpSocket::bind(addrs).await?);
        info!("bound receive socket to {:?}", receive_socket.local_addr()?);
        let (send_socket_v4, send_socket_v6) = if receive_socket.local_addr()?.is_ipv6() {
            (Arc::new(UdpSocket::bind("0.0.0.0:0").await?), receive_socket.clone())
        }
        else {
            (receive_socket.clone(), Arc::new(UdpSocket::bind("[::]:0").await?))
        };

        Ok(EndPoint {
            receive_socket,
            send_socket_v4: Arc::new(SendPipeline::new(Arc::new(send_socket_v4))),
            send_socket_v6: Arc::new(SendPipeline::new(Arc::new(send_socket_v6))),
            receive_streams: Default::default(),
            send_streams: Default::default(),
            message_dispatcher,
            default_receive_config,
            specific_receive_configs,
            default_send_config,
            specific_send_configs,
        })
    }

    //TODO send without stream

    pub async fn send_message(&self, to: SocketAddr, stream_id: u16, message: &[u8]) {
        let send_stream = self.get_send_stream(to, stream_id).await;
        send_stream.send_message(message).await;
    }

    pub async fn recv_loop(&self)  {
        info!("starting receive loop");

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

            trace!("received packet from {:?}", from); //TODO instrument with unique ID per packet

            let parse_buf = &mut &buf[..num_read];
            let packet_header = match PacketHeader::deser(parse_buf) {
                Ok(header) => {
                    header
                },
                Err(_) => {
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
        self.specific_receive_configs.get(&stream_id)
            .cloned()
            .unwrap_or(self.default_receive_config.clone())
    }

    fn get_send_config(&self, stream_id: u16) -> Arc<SendStreamConfig> {
        self.specific_send_configs.get(&stream_id)
            .cloned()
            .unwrap_or(self.default_send_config.clone())
    }

    fn get_send_socket(&self, peer_addr: SocketAddr) -> Arc<SendPipeline> {
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
                let mut recv_strm = ReceiveStream::new(
                    self.get_receive_config(stream_id),
                    stream_id,
                    addr,
                    self.get_send_socket(addr),
                    self.receive_socket.local_addr().unwrap(),
                    self.message_dispatcher.clone(),
                );
                recv_strm.spawn_active_loop();
                e.insert(Arc::new(recv_strm)).clone()
            }
        }
    }

    fn get_reply_to_addr(&self, for_addr: SocketAddr) -> Option<SocketAddr> {
        let local_addr = self.receive_socket.local_addr().unwrap();
        match (local_addr.is_ipv4(), for_addr.is_ipv4()) {
            (true, true) | (false, false) => None,
            (true, false) | (false, true) => Some(local_addr),
        }
    }

    async fn get_send_stream(&self, addr: SocketAddr, stream_id: u16) -> Arc<SendStream> {
        match self.send_streams
            .lock().await
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                e.insert(Arc::new(SendStream::new(
                    self.get_send_config(stream_id),
                    stream_id,
                    self.get_send_socket(addr),
                    addr,
                    self.get_reply_to_addr(addr),
                ))).clone()
            }
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
