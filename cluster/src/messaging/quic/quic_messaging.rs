//! QUIC-based implementation of the cluster `Messaging` trait.
//!
//! Connection model:
//!   - Single quinn `Endpoint` per node, acts as both server and client.
//!   - Outgoing connections are lazily opened on first send and cached by
//!     remote `SocketAddr`. Idle connections are dropped by QUIC's own
//!     idle-timeout (configured via `QuicConfig::idle_timeout`).
//!
//! Reliable-lane model:
//!   - Three reliable lanes share a connection but have two different
//!     stream-management strategies:
//!     * `LowLatency` and `Regular` use a *single long-lived uni-stream* per
//!       (peer, lane). The receiver delivers messages in send order by
//!       construction (per-(peer, lane) FIFO). The sender holds a per-(peer,
//!       lane) mutex across the framed write so concurrent senders interleave
//!       message-by-message, never byte-by-byte. A failure poisons the lane
//!       until the connection is re-established.
//!     * `Large` opens a *fresh uni-stream per message*, writes the message,
//!       and finishes the stream. There is **no ordering guarantee** between
//!       Large messages, no lane mutex, no poisoning: each transfer is
//!       independent so a failure on one cannot affect a successor. This
//!       avoids head-of-line blocking when a 256 MiB transfer would otherwise
//!       stall the next bulk send behind it.
//!   - QUIC guarantees streams cannot head-of-line-block one another, so the
//!     three lanes remain independent on the wire regardless of strategy.
//!   - Each lane has a configured stream priority forwarded to quinn once at
//!     stream open; when bytes contend for the wire, higher-priority lanes
//!     ship first.
//!   - Each lane enforces its own size cap before bytes hit the network.
//!
//! Wire format:
//!   datagram           : `[sender_unique:u64 BE][module_id:u64 BE][body...]`
//!   persistent stream  : prologue `[lane_id:u8]` (LowLatency=1, Regular=2),
//!                        then 0..N frames: `[len:u32 BE][sender_unique:u64 BE]
//!                                           [module_id:u64 BE][body...]`
//!                        where `len` counts bytes after the length field.
//!   large stream       : `[lane_id:u8 = 3][sender_unique:u64 BE]
//!                         [module_id:u64 BE][body...]`, exactly one message,
//!                        then `finish()`. No length prefix; the stream end
//!                        delimits the message.
//! The lane id is the very first byte so the inbound accept loop can route
//! a newly-opened stream to its handler after a single 1-byte read, without
//! parsing the rest of the header.

use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
    VarInt,
};
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, warn};

use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::messaging::{Delivery, MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::quic::quic_config::{LaneConfig, QuicConfig};
use crate::messaging::quic::spki_verifier::{
    SpkiClientVerifier, SpkiServerVerifier, TrustedSpki,
};
use crate::util::safe_converter::PrecheckedCast;

const COMMON_HEADER_LEN: usize = 16; // u64 unique + u64 module_id
const LEN_PREFIX_LEN: usize = 4; // u32 BE frame length
const FRAME_HEADER_LEN: usize = LEN_PREFIX_LEN + COMMON_HEADER_LEN;

/// Lane identifier carried as the first byte of every reliable stream.
/// Stable across versions; do not renumber. New lanes get new ids.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
enum LaneId {
    LowLatency = 1,
    Regular = 2,
    Large = 3,
}

impl LaneId {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(LaneId::LowLatency),
            2 => Some(LaneId::Regular),
            3 => Some(LaneId::Large),
            _ => None,
        }
    }

    fn from_delivery(d: Delivery) -> Option<Self> {
        match d {
            Delivery::Datagram => None,
            Delivery::ReliableLowLatency => Some(LaneId::LowLatency),
            Delivery::Reliable => Some(LaneId::Regular),
            Delivery::ReliableLarge => Some(LaneId::Large),
        }
    }

    fn all() -> [LaneId; 3] {
        [LaneId::LowLatency, LaneId::Regular, LaneId::Large]
    }

    /// `true` for lanes that use a single long-lived stream per (peer, lane)
    /// with per-lane FIFO. `false` for lanes that open a fresh stream per
    /// message with no ordering guarantee (currently just `Large`).
    fn is_persistent(self) -> bool {
        match self {
            LaneId::LowLatency | LaneId::Regular => true,
            LaneId::Large => false,
        }
    }
}

/// Per (peer, lane) sender state, kept behind an `Arc` so callers can drop the
/// global map lock before acquiring the lane-specific send lock.
struct LaneSendState {
    /// Held for the entire `open_uni → write_all → finish` window so that
    /// stream-open order on the wire matches caller invocation order.
    send_lock: Mutex<LaneSendInner>,
}

struct LaneSendInner {
    /// Lazily opened on first send, then reused for every subsequent send on
    /// this lane until the lane is poisoned or the connection is evicted.
    /// The stream's first byte is the lane id (written once at open time);
    /// every send appends a length-prefixed frame.
    stream: Option<SendStream>,
    /// If a previous send on this lane failed, every subsequent send returns
    /// this error verbatim until the lane is reset (the underlying connection
    /// is evicted, which discards the entire `LaneSendState`).
    poisoned: Option<String>,
}

/// Shared state behind an `Arc` so receive loop tasks can hold it without
/// borrowing `&self`. `QuicMessaging` is itself a thin wrapper over `Inner`.
struct Inner {
    self_addr: NodeAddr,
    endpoint: Endpoint,
    client_config: ClientConfig,
    /// Lazy outgoing-connection cache keyed by remote socket address.
    connections: RwLock<FxHashMap<SocketAddr, Connection>>,
    connect_lock: Mutex<()>,
    /// Per (peer, lane) sender ordering state.
    lane_states: RwLock<FxHashMap<(SocketAddr, LaneId), Arc<LaneSendState>>>,
    message_modules: ArcSwap<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>,
    message_modules_write_lock: Mutex<()>,

    lane_low_latency: LaneConfig,
    lane_regular: LaneConfig,
    lane_large: LaneConfig,
    /// Cached `max(lane caps)` — the global ceiling enforced on inbound streams.
    inbound_max: usize,
}

pub struct QuicMessaging {
    inner: Arc<Inner>,
}

impl Debug for QuicMessaging {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "QuicMessaging({:?})", self.inner.self_addr)
    }
}

impl QuicMessaging {
    pub async fn new(config: &QuicConfig) -> anyhow::Result<QuicMessaging> {
        // Safe to call repeatedly; only the first install wins.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let trusted = TrustedSpki::new(config.trusted_spki.iter().copied());

        // Server side.
        let server_crypto = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(Arc::new(SpkiClientVerifier::new(
                trusted.clone(),
                &provider,
            )))
            .with_single_cert(
                vec![config.cert_der.clone()],
                config.key_der.clone_key(),
            )?;
        let mut server_cfg =
            ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
        server_cfg.transport_config(Arc::new(build_transport(config)));

        // Client side (reused for every outgoing connection).
        let client_crypto = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SpkiServerVerifier::new(
                trusted,
                &provider,
            )))
            .with_client_auth_cert(
                vec![config.cert_der.clone()],
                config.key_der.clone_key(),
            )?;
        let mut client_cfg =
            ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
        client_cfg.transport_config(Arc::new(build_transport(config)));

        let mut endpoint = Endpoint::server(server_cfg, config.self_addr)?;
        endpoint.set_default_client_config(client_cfg.clone());

        let inner = Arc::new(Inner {
            self_addr: NodeAddr {
                unique: generation_from_timestamp()?,
                socket_addr: config.self_addr,
            },
            endpoint,
            client_config: client_cfg,
            connections: Default::default(),
            connect_lock: Mutex::new(()),
            lane_states: Default::default(),
            message_modules: ArcSwap::from_pointee(FxHashMap::default()),
            message_modules_write_lock: Mutex::new(()),
            lane_low_latency: config.lane_low_latency,
            lane_regular: config.lane_regular,
            lane_large: config.lane_large,
            inbound_max: config.max_inbound_message_size(),
        });
        Ok(QuicMessaging { inner })
    }
}

impl Inner {
    /// Get an open outgoing connection for `addr`, opening one if needed.
    async fn connection_for(&self, addr: SocketAddr) -> anyhow::Result<Connection> {
        if let Some(conn) = self.connections.read().await.get(&addr) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }

        let _guard = self.connect_lock.lock().await;
        // re-check under the connect lock to avoid duplicate connects
        if let Some(conn) = self.connections.read().await.get(&addr) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }

        debug!("opening QUIC connection to {}", addr);
        // server_name is irrelevant (verifier ignores it) but must parse.
        let connecting = self
            .endpoint
            .connect_with(self.client_config.clone(), addr, "cluster.peer")?;
        let conn = connecting.await?;
        self.connections.write().await.insert(addr, conn.clone());
        Ok(conn)
    }

    /// Drop the cached connection and every per-lane sender state for `addr`
    /// if the connection has been closed. Called after a send failure so the
    /// next call can re-establish.
    async fn evict_if_closed(&self, addr: SocketAddr) {
        let mut map = self.connections.write().await;
        let closed = map
            .get(&addr)
            .map(|c| c.close_reason().is_some())
            .unwrap_or(false);
        if closed {
            map.remove(&addr);
            drop(map);
            self.lane_states
                .write()
                .await
                .retain(|(a, _), _| *a != addr);
        }
    }

    async fn lane_state(&self, addr: SocketAddr, lane: LaneId) -> Arc<LaneSendState> {
        if let Some(s) = self.lane_states.read().await.get(&(addr, lane)) {
            return s.clone();
        }
        let mut w = self.lane_states.write().await;
        w.entry((addr, lane))
            .or_insert_with(|| {
                Arc::new(LaneSendState {
                    send_lock: Mutex::new(LaneSendInner {
                        stream: None,
                        poisoned: None,
                    }),
                })
            })
            .clone()
    }

    fn build_datagram_frame<T: Message + ?Sized>(&self, msg: &T) -> BytesMut {
        let mut buf = BytesMut::with_capacity(COMMON_HEADER_LEN + 256);
        buf.put_u64(self.self_addr.unique);
        buf.put_u64(msg.module_id().0);
        msg.ser(&mut buf);
        buf
    }

    /// Build a length-prefixed reliable frame:
    /// `[len:u32 BE][sender_unique:u64][module_id:u64][body...]`
    /// where `len` counts the bytes *after* the length field. The lane id is
    /// NOT part of the frame — it is written once as the stream prologue.
    fn build_reliable_frame<T: Message + ?Sized>(&self, msg: &T) -> BytesMut {
        let mut buf = BytesMut::with_capacity(FRAME_HEADER_LEN + 256);
        // Reserve the length prefix; we backfill once the body is serialized.
        buf.put_u32(0);
        buf.put_u64(self.self_addr.unique);
        buf.put_u64(msg.module_id().0);
        msg.ser(&mut buf);
        let body_len = (buf.len() - LEN_PREFIX_LEN) as u32;
        buf[..LEN_PREFIX_LEN].copy_from_slice(&body_len.to_be_bytes());
        buf
    }

    /// Build a Large-lane frame: `[sender_unique:u64][module_id:u64][body...]`.
    /// No length prefix — each Large stream carries exactly one message and is
    /// delimited by `finish()` / stream end.
    fn build_large_frame<T: Message + ?Sized>(&self, msg: &T) -> BytesMut {
        let mut buf = BytesMut::with_capacity(COMMON_HEADER_LEN + 256);
        buf.put_u64(self.self_addr.unique);
        buf.put_u64(msg.module_id().0);
        msg.ser(&mut buf);
        buf
    }

    async fn send_datagram_inner<T: Message + ?Sized>(
        &self,
        addr: SocketAddr,
        msg: &T,
    ) -> anyhow::Result<()> {
        let conn = self.connection_for(addr).await?;
        let buf = self.build_datagram_frame(msg).freeze();
        if let Some(max) = conn.max_datagram_size() {
            if buf.len() > max {
                anyhow::bail!(
                    "datagram of {} bytes exceeds peer MTU {}",
                    buf.len(),
                    max
                );
            }
        }
        match conn.send_datagram(buf) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.evict_if_closed(addr).await;
                Err(e.into())
            }
        }
    }

    async fn send_reliable_on_lane<T: Message + ?Sized>(
        &self,
        addr: SocketAddr,
        msg: &T,
        lane_id: LaneId,
        lane: &LaneConfig,
    ) -> anyhow::Result<()> {
        if lane_id.is_persistent() {
            self.send_reliable_persistent(addr, msg, lane_id, lane).await
        } else {
            self.send_reliable_stream_per_message(addr, msg, lane_id, lane)
                .await
        }
    }

    /// Persistent-stream path: reuse the single uni-stream for (peer, lane);
    /// hold the lane mutex to serialize concurrent senders so the receiver
    /// observes per-lane FIFO. A failure poisons the lane.
    async fn send_reliable_persistent<T: Message + ?Sized>(
        &self,
        addr: SocketAddr,
        msg: &T,
        lane_id: LaneId,
        lane: &LaneConfig,
    ) -> anyhow::Result<()> {
        // Build + size-check BEFORE acquiring the lane lock: oversize is a
        // caller bug, not a transport failure, so it must not poison the lane.
        // Cap is checked against the on-wire frame *body* (everything after
        // the length prefix) so the limit semantics match the previous
        // implementation.
        let buf = self.build_reliable_frame(msg);
        let body_len = buf.len() - LEN_PREFIX_LEN;
        if body_len > lane.max_msg_size {
            anyhow::bail!(
                "reliable message of {} bytes exceeds lane cap {}",
                body_len,
                lane.max_msg_size
            );
        }

        let conn = self.connection_for(addr).await?;
        let state = self.lane_state(addr, lane_id).await;
        let mut guard = state.send_lock.lock().await;

        if let Some(poisoned) = &guard.poisoned {
            anyhow::bail!(
                "lane {:?}->{} poisoned by earlier failure: {}",
                lane_id,
                addr,
                poisoned
            );
        }

        // The lane lock is held across (lazy open +) write so concurrent
        // senders interleave message-by-message, never byte-by-byte. The
        // single long-lived stream gives per-lane FIFO by construction:
        // the receiver reads framed messages off it in order.
        let res: anyhow::Result<()> = async {
            if guard.stream.is_none() {
                let mut s = conn.open_uni().await?;
                let _ = s.set_priority(lane.priority);
                // Stream prologue: one byte routing the stream to its lane
                // task on the receiver side.
                s.write_all(&[lane_id as u8]).await?;
                guard.stream = Some(s);
            }
            let stream = guard
                .stream
                .as_mut()
                .expect("just-initialized above");
            stream.write_all(&buf).await?;
            Ok(())
        }
        .await;

        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                // Poison the lane so a successor message cannot overtake a
                // failed predecessor. Drop the (possibly broken) stream; the
                // poison clears when the underlying connection is evicted
                // (see `evict_if_closed`).
                guard.poisoned = Some(format!("{e}"));
                guard.stream = None;
                drop(guard);
                self.evict_if_closed(addr).await;
                Err(e)
            }
        }
    }

    /// Stream-per-message path (Large lane): open a fresh uni-stream per
    /// message, write `[lane_id][unique][module_id][body]`, finish. No lane
    /// mutex, no `LaneSendState`, no poisoning — each transfer is independent
    /// so QUIC can ship them in parallel and a failure on one cannot affect
    /// any other.
    async fn send_reliable_stream_per_message<T: Message + ?Sized>(
        &self,
        addr: SocketAddr,
        msg: &T,
        lane_id: LaneId,
        lane: &LaneConfig,
    ) -> anyhow::Result<()> {
        let buf = self.build_large_frame(msg);
        if buf.len() > lane.max_msg_size {
            anyhow::bail!(
                "reliable message of {} bytes exceeds lane cap {}",
                buf.len(),
                lane.max_msg_size
            );
        }

        let conn = self.connection_for(addr).await?;

        let res: anyhow::Result<()> = async {
            let mut stream = conn.open_uni().await?;
            let _ = stream.set_priority(lane.priority);
            stream.write_all(&[lane_id as u8]).await?;
            stream.write_all(&buf).await?;
            stream.finish()?;
            Ok(())
        }
        .await;

        if let Err(e) = res {
            self.evict_if_closed(addr).await;
            return Err(e);
        }
        Ok(())
    }

    fn lane_for(&self, delivery: Delivery) -> Option<&LaneConfig> {
        match delivery {
            Delivery::Datagram => None,
            Delivery::ReliableLowLatency => Some(&self.lane_low_latency),
            Delivery::Reliable => Some(&self.lane_regular),
            Delivery::ReliableLarge => Some(&self.lane_large),
        }
    }

    fn get_message_module(&self, id: MessageModuleId) -> Option<Arc<dyn MessageModule>> {
        self.message_modules.load().get(&id).cloned()
    }

    async fn dispatch_datagram(&self, remote_addr: SocketAddr, buf: &[u8]) {
        if buf.len() < COMMON_HEADER_LEN {
            warn!("dropping short QUIC datagram from {} ({} bytes)", remote_addr, buf.len());
            return;
        }
        let mut cursor = buf;
        let sender_unique = cursor.get_u64();
        let module_id = MessageModuleId(cursor.get_u64());
        self.dispatch_to_module(remote_addr, sender_unique, module_id, cursor).await;
    }

    /// Dispatch a reliable stream's full body (lane byte already consumed by
    /// the accept loop). Parses the remaining common header and forwards to
    /// the registered module.
    async fn dispatch_reliable(&self, remote_addr: SocketAddr, buf: &[u8]) {
        if buf.len() < COMMON_HEADER_LEN {
            warn!("dropping short reliable frame from {} ({} bytes)", remote_addr, buf.len());
            return;
        }
        let mut cursor = buf;
        let sender_unique = cursor.get_u64();
        let module_id = MessageModuleId(cursor.get_u64());
        self.dispatch_to_module(remote_addr, sender_unique, module_id, cursor).await;
    }

    async fn dispatch_to_module(
        &self,
        remote_addr: SocketAddr,
        sender_unique: u64,
        module_id: MessageModuleId,
        body: &[u8],
    ) {
        let sender = NodeAddr {
            unique: sender_unique,
            socket_addr: remote_addr,
        };
        if let Some(module) = self.get_message_module(module_id) {
            module.on_message(sender, body).await;
        } else {
            warn!(
                "received QUIC message for unregistered module {:?} from {:?}; dropping",
                module_id, sender
            );
        }
    }
}

fn build_transport(config: &QuicConfig) -> TransportConfig {
    let mut tp = TransportConfig::default();
    tp.datagram_receive_buffer_size(Some(config.datagram_buffer_size as usize));
    tp.datagram_send_buffer_size(config.datagram_buffer_size as usize);
    if let Ok(idle) = config.idle_timeout.try_into() {
        tp.max_idle_timeout(Some(idle));
    }
    tp.max_concurrent_uni_streams(VarInt::from_u32(config.max_concurrent_uni_streams));
    tp
}

fn generation_from_timestamp() -> anyhow::Result<u64> {
    let raw = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    if raw > 0xffff_ffff_ffff {
        anyhow::bail!("system clock is way in the past");
    }
    Ok(raw.prechecked_cast())
}

#[async_trait]
impl MessageSender for QuicMessaging {
    fn get_self_addr(&self) -> NodeAddr {
        self.inner.self_addr
    }

    async fn send_to_node<T: Message + ?Sized>(
        &self,
        to: NodeAddr,
        delivery: Delivery,
        msg: &T,
    ) -> anyhow::Result<()> {
        match delivery {
            Delivery::Datagram => self.inner.send_datagram_inner(to.socket_addr, msg).await,
            other => {
                let lane_id = LaneId::from_delivery(other)
                    .expect("non-datagram delivery must map to a lane");
                let lane = self
                    .inner
                    .lane_for(other)
                    .expect("non-datagram delivery must map to a lane");
                self.inner
                    .send_reliable_on_lane(to.socket_addr, msg, lane_id, lane)
                    .await
            }
        }
    }

    async fn send_to_addr<T: Message + ?Sized>(
        &self,
        to: SocketAddr,
        msg: &T,
    ) -> anyhow::Result<()> {
        // Used before peer identity is known (join). Route through the
        // low-latency lane: join is small, latency-sensitive control traffic.
        self.inner
            .send_reliable_on_lane(to, msg, LaneId::LowLatency, &self.inner.lane_low_latency)
            .await
    }
}

#[async_trait]
impl Messaging for QuicMessaging {
    async fn register_module(&self, message_module: Arc<dyn MessageModule>) {
        let _guard = self.inner.message_modules_write_lock.lock().await;
        let mut map = self.inner.message_modules.load().as_ref().clone();
        map.insert(message_module.id(), message_module);
        self.inner.message_modules.store(Arc::new(map));
    }

    async fn deregister_module(&self, id: MessageModuleId) {
        let _guard = self.inner.message_modules_write_lock.lock().await;
        let mut map = self.inner.message_modules.load().as_ref().clone();
        map.remove(&id);
        self.inner.message_modules.store(Arc::new(map));
    }

    async fn recv(&self) {
        loop {
            let incoming = match self.inner.endpoint.accept().await {
                Some(i) => i,
                None => {
                    debug!("QUIC endpoint closed; recv loop exiting");
                    return;
                }
            };
            let inner = self.inner.clone();
            tokio::spawn(async move {
                pump_inbound_connection(inner, incoming).await;
            });
        }
    }
}

async fn pump_inbound_connection(inner: Arc<Inner>, incoming: quinn::Incoming) {
    let conn = match incoming.await {
        Ok(c) => c,
        Err(e) => {
            warn!("inbound QUIC handshake failed: {}", e);
            return;
        }
    };
    let remote = conn.remote_address();
    debug!("inbound QUIC connection from {}", remote);
    let inbound_max = inner.inbound_max;

    // One serial reader task per *persistent* lane on this connection. The
    // accept loop peeks the 1-byte lane prologue off each newly-opened stream
    // and either forwards persistent-lane streams to their reader task (which
    // loops length-prefixed frames), or spawns a one-shot per-stream task for
    // the Large lane that reads the entire payload to end.
    let mut lane_txs: FxHashMap<LaneId, mpsc::UnboundedSender<RecvStream>> = FxHashMap::default();
    for lane in LaneId::all().into_iter().filter(|l| l.is_persistent()) {
        let (tx, mut rx) = mpsc::unbounded_channel::<RecvStream>();
        lane_txs.insert(lane, tx);
        let inner = inner.clone();
        tokio::spawn(async move {
            while let Some(mut stream) = rx.recv().await {
                loop {
                    // Read the 4-byte frame length. UnexpectedEof at a
                    // frame boundary means the sender finished/dropped the
                    // stream cleanly — break and await the next stream.
                    let mut len_buf = [0u8; LEN_PREFIX_LEN];
                    match stream.read_exact(&mut len_buf).await {
                        Ok(()) => {}
                        Err(quinn::ReadExactError::FinishedEarly(0)) => break,
                        Err(e) => {
                            warn!(
                                "uni-stream frame-length read error from {} on lane {:?}: {}",
                                remote, lane, e
                            );
                            break;
                        }
                    }
                    let body_len = u32::from_be_bytes(len_buf) as usize;
                    if body_len > inbound_max {
                        warn!(
                            "frame of {} bytes from {} on lane {:?} exceeds inbound cap {}; dropping stream",
                            body_len, remote, lane, inbound_max
                        );
                        break;
                    }
                    let mut body = vec![0u8; body_len];
                    if let Err(e) = stream.read_exact(&mut body).await {
                        warn!(
                            "uni-stream frame-body read error from {} on lane {:?}: {}",
                            remote, lane, e
                        );
                        break;
                    }
                    inner.dispatch_reliable(remote, &body).await;
                }
            }
            debug!("lane {:?} reader for {} exiting", lane, remote);
        });
    }

    loop {
        tokio::select! {
            dg = conn.read_datagram() => match dg {
                Ok(bytes) => inner.dispatch_datagram(remote, &bytes).await,
                Err(e) => {
                    debug!("inbound connection {} closed (datagram side): {}", remote, e);
                    return;
                }
            },
            uni = conn.accept_uni() => match uni {
                Ok(mut stream) => {
                    // Peek the 1-byte lane id. Read inline because it is
                    // tiny and we need it to route the stream to either a
                    // persistent-lane reader or a one-shot Large reader.
                    let mut lane_buf = [0u8; 1];
                    match stream.read_exact(&mut lane_buf).await {
                        Ok(()) => {
                            let Some(lane) = LaneId::from_byte(lane_buf[0]) else {
                                warn!("unknown lane id {} from {}; dropping stream", lane_buf[0], remote);
                                continue;
                            };
                            if lane.is_persistent() {
                                // forwarding is infallible: tx is alive as
                                // long as this pump task is, which outlives
                                // accept.
                                if let Some(tx) = lane_txs.get(&lane) {
                                    let _ = tx.send(stream);
                                }
                            } else {
                                // Large lane: one stream = one message.
                                // Spawn a fresh task so multiple Large
                                // transfers can proceed concurrently with
                                // no per-lane serialization.
                                let inner = inner.clone();
                                tokio::spawn(async move {
                                    match stream.read_to_end(inbound_max).await {
                                        Ok(buf) => {
                                            inner.dispatch_reliable(remote, &buf).await;
                                        }
                                        Err(e) => warn!(
                                            "large-lane stream read error from {}: {}",
                                            remote, e
                                        ),
                                    }
                                });
                            }
                        }
                        Err(e) => {
                            warn!("failed to read lane id from {}: {}", remote, e);
                        }
                    }
                }
                Err(e) => {
                    debug!("inbound connection {} closed (uni side): {}", remote, e);
                    return;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::message_module::MessageModuleId;
    use crate::messaging::quic::spki_verifier::spki_hash_of_cert;
    use bytes::BytesMut;
    use rustc_hash::FxHashSet;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::any::Any;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn mint_identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>, [u8; 32]) {
        let mut params = rcgen::CertificateParams::new(vec!["test.node".to_string()]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(key_pair.serialize_der()).unwrap();
        let spki = spki_hash_of_cert(&cert_der).unwrap();
        (cert_der, key_der, spki)
    }

    /// A test message whose body is a 4-byte tag prefix + `payload_len` bytes
    /// of `0xAB`, so tests can assert delivery order.
    #[derive(Debug)]
    struct BlobMsg {
        tag: u32,
        payload: Vec<u8>,
    }
    impl Message for BlobMsg {
        fn module_id(&self) -> MessageModuleId {
            MessageModuleId::new(b"blobblob")
        }
        fn ser(&self, buf: &mut BytesMut) {
            buf.put_u32(self.tag);
            buf.put_slice(&self.payload);
        }
        fn box_clone(&self) -> Arc<dyn Any + Send + Sync + 'static> {
            Arc::new(self.payload.clone())
        }
    }

    /// Records every received message's (tag, len) and signals on each arrival.
    struct Recorder {
        events: StdMutex<Vec<(u32, usize)>>,
        notify: Notify,
    }
    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                events: StdMutex::new(Vec::new()),
                notify: Notify::new(),
            })
        }
    }
    #[async_trait]
    impl MessageModule for Recorder {
        fn id(&self) -> MessageModuleId {
            MessageModuleId::new(b"blobblob")
        }
        async fn on_message(&self, _sender: NodeAddr, buf: &[u8]) {
            let tag = u32::from_be_bytes(buf[..4].try_into().unwrap());
            self.events.lock().unwrap().push((tag, buf.len() - 4));
            self.notify.notify_waiters();
        }
    }

    fn free_port() -> u16 {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap().port()
    }

    async fn make_pair() -> (QuicMessaging, QuicMessaging) {
        let (a_cert, a_key, a_spki) = mint_identity();
        let (b_cert, b_key, b_spki) = mint_identity();
        let mut trusted: FxHashSet<[u8; 32]> = FxHashSet::default();
        trusted.insert(a_spki);
        trusted.insert(b_spki);

        let a_addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let b_addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();

        let a_cfg = QuicConfig::new(a_addr, a_cert, a_key, trusted.clone());
        let b_cfg = QuicConfig::new(b_addr, b_cert, b_key, trusted);

        let a = QuicMessaging::new(&a_cfg).await.unwrap();
        let b = QuicMessaging::new(&b_cfg).await.unwrap();
        (a, b)
    }

    async fn wait_for(recorder: &Recorder, count: usize, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if recorder.events.lock().unwrap().len() >= count {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let _ = tokio::time::timeout(remaining, recorder.notify.notified()).await;
        }
    }

    #[tokio::test]
    async fn three_lanes_all_deliver() {
        let (sender, receiver) = make_pair().await;
        let recorder = Recorder::new();
        receiver.register_module(recorder.clone()).await;
        let receiver_addr = receiver.get_self_addr();
        tokio::spawn(async move {
            receiver.recv().await;
        });

        let to = NodeAddr {
            unique: receiver_addr.unique,
            socket_addr: receiver_addr.socket_addr,
        };

        let small = BlobMsg { tag: 1, payload: vec![0xAB; 64] };
        let medium = BlobMsg { tag: 2, payload: vec![0xAB; 256 * 1024] };
        let large = BlobMsg { tag: 3, payload: vec![0xAB; 2 * 1024 * 1024] };

        sender.send_to_node(to, Delivery::ReliableLowLatency, &small).await.unwrap();
        sender.send_to_node(to, Delivery::Reliable, &medium).await.unwrap();
        sender.send_to_node(to, Delivery::ReliableLarge, &large).await.unwrap();

        assert!(wait_for(&recorder, 3, Duration::from_secs(10)).await,
            "did not receive 3 messages in time, got {:?}", recorder.events.lock().unwrap());

        let mut events = recorder.events.lock().unwrap().clone();
        events.sort_by_key(|e| e.0);
        assert_eq!(events, vec![(1, 64), (2, 256 * 1024), (3, 2 * 1024 * 1024)]);
    }

    #[tokio::test]
    async fn oversize_message_rejected_per_lane() {
        let (a_cert, a_key, a_spki) = mint_identity();
        let (b_cert, b_key, b_spki) = mint_identity();
        let mut trusted: FxHashSet<[u8; 32]> = FxHashSet::default();
        trusted.insert(a_spki);
        trusted.insert(b_spki);

        let a_addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let b_addr: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();

        let mut a_cfg = QuicConfig::new(a_addr, a_cert, a_key, trusted.clone());
        a_cfg.lane_regular.max_msg_size = 1024;
        let b_cfg = QuicConfig::new(b_addr, b_cert, b_key, trusted);

        let sender = QuicMessaging::new(&a_cfg).await.unwrap();
        let receiver = QuicMessaging::new(&b_cfg).await.unwrap();
        let receiver_addr = receiver.get_self_addr();
        tokio::spawn(async move {
            receiver.recv().await;
        });

        let to = NodeAddr {
            unique: receiver_addr.unique,
            socket_addr: receiver_addr.socket_addr,
        };

        let too_big = BlobMsg { tag: 99, payload: vec![0xAB; 4096] };
        let res = sender.send_to_node(to, Delivery::Reliable, &too_big).await;
        assert!(res.is_err(), "expected oversize message to be rejected");
        let msg = format!("{}", res.unwrap_err());
        assert!(msg.contains("exceeds lane cap"), "unexpected error: {msg}");

        // Oversize must NOT poison the lane.
        let small = BlobMsg { tag: 1, payload: vec![0xAB; 64] };
        sender.send_to_node(to, Delivery::Reliable, &small).await.unwrap();
    }

    /// Mix sizes within a lane so smaller successors could overtake larger
    /// predecessors at the QUIC scheduler level if ordering were not
    /// preserved. The single-reader-per-lane receiver must still emit
    /// `tag = 0..N` in order.
    #[tokio::test]
    async fn per_lane_messages_arrive_in_send_order() {
        let (sender, receiver) = make_pair().await;
        let recorder = Recorder::new();
        receiver.register_module(recorder.clone()).await;
        let receiver_addr = receiver.get_self_addr();
        tokio::spawn(async move {
            receiver.recv().await;
        });

        let to = NodeAddr {
            unique: receiver_addr.unique,
            socket_addr: receiver_addr.socket_addr,
        };

        const N: u32 = 30;
        for i in 0..N {
            let size = if i % 5 == 0 { 512 * 1024 } else { 256 };
            let msg = BlobMsg { tag: i, payload: vec![0xAB; size] };
            sender.send_to_node(to, Delivery::ReliableLowLatency, &msg).await.unwrap();
        }

        assert!(wait_for(&recorder, N as usize, Duration::from_secs(15)).await,
            "did not receive all {} messages", N);

        let events = recorder.events.lock().unwrap().clone();
        let tags: Vec<u32> = events.iter().map(|e| e.0).collect();
        let expected: Vec<u32> = (0..N).collect();
        assert_eq!(tags, expected, "messages arrived out of send order on lane");
    }

    /// Concurrent senders on the same lane: linearization point is lane
    /// mutex acquisition. The exact tag order is nondeterministic, but the
    /// receiver must observe each tag exactly once.
    #[tokio::test]
    async fn concurrent_sends_on_same_lane_dont_corrupt_ordering() {
        let (sender, receiver) = make_pair().await;
        let sender = Arc::new(sender);
        let recorder = Recorder::new();
        receiver.register_module(recorder.clone()).await;
        let receiver_addr = receiver.get_self_addr();
        tokio::spawn(async move {
            receiver.recv().await;
        });

        let to = NodeAddr {
            unique: receiver_addr.unique,
            socket_addr: receiver_addr.socket_addr,
        };

        const N: u32 = 50;
        let mut handles = Vec::new();
        for i in 0..N {
            let s = sender.clone();
            handles.push(tokio::spawn(async move {
                let msg = BlobMsg { tag: i, payload: vec![0xAB; 1024] };
                s.send_to_node(to, Delivery::Reliable, &msg).await.unwrap();
            }));
        }
        for h in handles { h.await.unwrap(); }

        assert!(wait_for(&recorder, N as usize, Duration::from_secs(10)).await);

        let events = recorder.events.lock().unwrap().clone();
        let mut tags: Vec<u32> = events.iter().map(|e| e.0).collect();
        tags.sort();
        let expected: Vec<u32> = (0..N).collect();
        assert_eq!(tags, expected);
    }

    /// Large lane uses stream-per-message with no ordering guarantee. Many
    /// concurrent transfers should all arrive, exactly once each, and should
    /// genuinely run in parallel (so the receiver must spawn per-stream
    /// tasks rather than serialize them on one lane reader).
    #[tokio::test]
    async fn large_lane_concurrent_streams_all_deliver() {
        let (sender, receiver) = make_pair().await;
        let sender = Arc::new(sender);
        let recorder = Recorder::new();
        receiver.register_module(recorder.clone()).await;
        let receiver_addr = receiver.get_self_addr();
        tokio::spawn(async move {
            receiver.recv().await;
        });

        let to = NodeAddr {
            unique: receiver_addr.unique,
            socket_addr: receiver_addr.socket_addr,
        };

        const N: u32 = 20;
        let mut handles = Vec::new();
        for i in 0..N {
            let s = sender.clone();
            handles.push(tokio::spawn(async move {
                let msg = BlobMsg {
                    tag: i,
                    payload: vec![0xAB; 1 * 1024 * 1024],
                };
                s.send_to_node(to, Delivery::ReliableLarge, &msg)
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert!(
            wait_for(&recorder, N as usize, Duration::from_secs(30)).await,
            "did not receive all {} large messages", N
        );

        let events = recorder.events.lock().unwrap().clone();
        let mut tags: Vec<u32> = events.iter().map(|e| e.0).collect();
        tags.sort();
        let expected: Vec<u32> = (0..N).collect();
        assert_eq!(tags, expected, "every large message must arrive exactly once");
        for (_tag, len) in &events {
            assert_eq!(*len, 1024 * 1024);
        }
    }
}
