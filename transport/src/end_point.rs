use crate::buffers::atomic_map::AtomicMap;
use crate::buffers::buffer_pool::SendBufferPool;
use crate::config::RudpConfig;
use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync, ControlMessageSendSync};
use crate::message_dispatcher::MessageDispatcher;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::receive_stream::ReceiveStream;
use crate::send_pipeline::SendPipeline;
use crate::send_stream::SendStream;
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use aead::Buffer;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tracing::{debug, error, info, trace, warn};
use crate::buffers::encryption::{Aes256GcmEncryption, NoEncryption, RudpEncryption};
//TODO unit test

/// EndPoint is the place where all other parts of the protocol come together: It listens on a
///  UdpSocket, dispatching incoming packets to their corresponding channels, and has an API for
///  application code to send messages.
pub struct EndPoint {
    generation: u64,
    receive_socket: Arc<UdpSocket>,
    send_socket_v4: Arc<SendPipeline>,
    send_socket_v6: Arc<SendPipeline>,
    peer_generations: Arc<AtomicMap<SocketAddr, u64>>,
    send_streams: AtomicMap<(SocketAddr, u16), Arc<SendStream>>,
    message_dispatcher: Arc<dyn MessageDispatcher>,
    config: Arc<RudpConfig>,
    buffer_pool: Arc<SendBufferPool>,
    encryption: Arc<dyn RudpEncryption>,
}
impl EndPoint {
    pub async fn new(
        addrs: impl ToSocketAddrs,
        message_dispatcher: Arc<dyn MessageDispatcher>,
        config: RudpConfig,
    ) -> anyhow::Result<EndPoint> {
        config.validate()?;

        //TODO "don't fragment" flag
        let receive_socket = Arc::new(UdpSocket::bind(addrs).await?);
        info!("bound receive socket to {:?}", receive_socket.local_addr()?);
        let (send_socket_v4, send_socket_v6) = if receive_socket.local_addr()?.is_ipv6() {
            (Arc::new(UdpSocket::bind("0.0.0.0:0").await?), receive_socket.clone())
        }
        else {
            (receive_socket.clone(), Arc::new(UdpSocket::bind("[::]:0").await?))
        };

        let encryption = Self::create_encryption(&config);

        let buffer_pool = Arc::new(SendBufferPool::new(config.payload_size_inside_udp, config.buffer_pool_size, encryption.clone()));
        Ok(EndPoint {
            generation: Self::generation_from_timestamp()?,
            receive_socket,
            send_socket_v4: Arc::new(SendPipeline::new(Arc::new(send_socket_v4), encryption.clone())),
            send_socket_v6: Arc::new(SendPipeline::new(Arc::new(send_socket_v6), encryption.clone())),
            peer_generations: Default::default(),
            send_streams: Default::default(),
            message_dispatcher,
            config: Arc::new(config),
            buffer_pool,
            encryption,
        })
    }

    fn create_encryption(config: &RudpConfig) -> Arc<dyn RudpEncryption> {
        if let Some(key) = &config.encryption_key {
            info!("setting up AES encryption");
            Arc::new(Aes256GcmEncryption::new(key))
        }
        else {
            warn!("initializing without encryption - this is for debugging purposes and not recommended for production use");
            Arc::new(NoEncryption)
        }
    }

    fn generation_from_timestamp() -> anyhow::Result<u64> {
        let raw = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();

        if raw > 0xffff_ffff_ffff {
            anyhow::bail!("system clock is in the future");
        }
        Ok(raw as u64)
    }

    //TODO send without stream

    pub async fn send_message(&self, to: SocketAddr, stream_id: u16, message: &[u8]) {
        let send_stream = self.get_send_stream(to, stream_id).await;
        send_stream.send_message(message).await;
    }

    pub async fn recv_loop(&self)  {
        info!("starting receive loop");

        let mut receive_streams: FxHashMap<(SocketAddr, u16), Arc<ReceiveStream>> = FxHashMap::default();

        let mut buf = self.buffer_pool.get_from_pool();
        loop {
            buf.maximize_len();
            let (num_read, from) = match self.receive_socket.recv_from(buf.as_mut()).await {
                Ok(x) => {
                    x
                }
                Err(e) => {
                    error!("socket error: {}", e);
                    continue;
                }
            };
            buf.truncate(num_read);

            trace!("received packet from {:?}: {:?}", from, &buf.as_ref()); //TODO instrument with unique ID per packet

            if buf.len() < self.encryption.prefix_len() {
                debug!("incomplete packet header - dropping");
                continue;
            }
            if buf.as_ref()[0] != PacketHeader::PROTOCOL_VERSION_1 {
                debug!("wrong protocol version {} - dropping", buf.as_ref()[0]);
                continue;
            }
            if self.encryption.decrypt_buffer(&mut buf).is_err() {
                debug!("cryptographically invalid - dropping");
                continue;
            }

            let parse_buf = &mut &buf.as_ref()[self.encryption.prefix_len()..];
            let packet_header = match PacketHeader::deser(parse_buf, PacketHeader::PROTOCOL_VERSION_1) {
                Ok(header) => {
                    header
                },
                Err(_) => {
                    warn!("received packet with unparsable header from {:?}, dropping", from);
                    continue;
                },
            };

            let peer_addr = packet_header.reply_to_address
                .unwrap_or(from);

            if !self.check_peer_generation(&mut receive_streams, peer_addr, packet_header.sender_generation) {
                continue;
            }

            todo!("check own generation");

            match packet_header.packet_kind {
                PacketKind::RegularSequenced { stream_id, first_message_offset, packet_sequence_number} => {
                    self.get_receive_stream(&mut receive_streams, packet_header.sender_generation, peer_addr, stream_id)
                        .on_packet(packet_sequence_number, first_message_offset, parse_buf).await
                },
                PacketKind::OutOfSequence => self.message_dispatcher.on_message(peer_addr, None, parse_buf).await,
                PacketKind::ControlInit { stream_id } => Self::handle_init_message(self.get_send_stream(peer_addr, stream_id).await).await,
                PacketKind::ControlRecvSync { stream_id } => Self::handle_recv_sync(parse_buf, self.get_send_stream(peer_addr, stream_id).await).await,
                PacketKind::ControlSendSync { stream_id } => Self::handle_send_sync(parse_buf, self.get_receive_stream(&mut receive_streams, packet_header.sender_generation, peer_addr, stream_id)).await,
                PacketKind::ControlNak { stream_id } => Self::handle_nak(parse_buf, self.get_send_stream(peer_addr, stream_id).await).await,
            }
        }
    }

    #[must_use]
    fn check_peer_generation(
        &self,
        receive_streams: &mut FxHashMap<(SocketAddr, u16), Arc<ReceiveStream>>,
        peer_addr: SocketAddr,
        new_peer_generation: u64,
    ) -> bool {
        let peer_generations = self.peer_generations.load();

        if let Some(prev) = peer_generations.get(&peer_addr) {
            if *prev == new_peer_generation {
                true
            }
            else if *prev < new_peer_generation {
                debug!("peer {:?}: received packet for old generation {} - discarding", peer_addr, new_peer_generation);
                false
            }
            else {
                debug!("peer {:?} restarted (@{}), re-initializing local per-peer state", peer_addr, new_peer_generation);
                self.peer_generations.update(|m| {m.insert(peer_addr, new_peer_generation); });
                self.send_streams
                    .update(|map| {
                        map.retain(|(s, _), _| s != &peer_addr);
                    });
                receive_streams
                    .retain(|(s, _), _| s != &peer_addr);
                true
            }
        }
        else {
            self.peer_generations.update(|m| { m.insert(peer_addr, new_peer_generation); });
            true
        }
    }

    fn get_send_socket(&self, peer_addr: SocketAddr) -> Arc<SendPipeline> {
        if peer_addr.is_ipv4() {
            self.send_socket_v4.clone()
        }
        else {
            self.send_socket_v6.clone()
        }
    }

    fn get_receive_stream(&self, receive_streams: &mut FxHashMap<(SocketAddr, u16), Arc<ReceiveStream>>, peer_generation: u64, addr: SocketAddr, stream_id: u16) -> Arc<ReceiveStream> {
        match receive_streams
            .entry((addr, stream_id))
        {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                debug!("initializing receive stream {} for {:?}", stream_id, addr);
                let mut recv_strm = ReceiveStream::new(
                    Arc::new(self.config.get_effective_receive_stream_config(stream_id)),
                    self.buffer_pool.clone(),
                    self.generation,
                    peer_generation,
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
        if let Some(stream) = self.send_streams.load().get(&(addr, stream_id)) {
            return stream.clone();
        };

        debug!("initializing send stream {} for {:?}", stream_id, addr);
        let stream = Arc::new(SendStream::new(
            Arc::new(self.config.get_effective_send_stream_config(stream_id, self.encryption.as_ref())),
            self.generation,
            self.peer_generations.clone(),
            stream_id,
            self.get_send_socket(addr),
            addr,
            self.get_reply_to_addr(addr),
            self.buffer_pool.clone(),
        ));

        self.send_streams.update(|map| {
            map.insert((addr, stream_id), stream.clone());
        });
        stream
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
