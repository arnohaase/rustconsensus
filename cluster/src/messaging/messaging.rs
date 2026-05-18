use crate::messaging::large_stream::LargeSendStream;
use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

/// Default in-memory ceiling used by the mock transport in `test_util`.
/// The real QUIC transport derives its inbound cap for the persistent reliable
/// lanes from the configured lane sizes
/// (see `QuicConfig::max_persistent_inbound_message_size`) and does NOT use
/// this constant. Large streaming transfers have no size cap at the transport
/// level — back-pressure is provided by the QUIC concurrent-uni-stream limit
/// and by SPKI-based peer authentication.
pub const MAX_MSG_SIZE: usize = 256*1024;

/// Per-send delivery semantics. Transports map these onto their native primitives
/// (e.g. QUIC datagrams vs. unidirectional streams). Modules MUST pick explicitly
/// to make the trade-off visible at the call site.
///
/// The two reliable variants share semantics (reliable, ordered per (peer, lane))
/// but hint to the transport how the bytes should be scheduled on the wire and
/// how large the payload may grow. Concretely on QUIC each reliable lane opens
/// a separate uni-stream so head-of-line blocking between lanes is impossible,
/// and each lane has its own stream priority so e.g. a 16 MiB regular payload
/// does not delay a small gossip-delta message.
///
/// For arbitrarily large transfers, use `MessageSender::open_large_stream`
/// instead — those bypass this enum entirely and are pure streaming with no
/// ordering and no size cap.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Delivery {
    /// Best-effort, unordered, size-limited (~MTU). No retransmission.
    /// Suitable for periodic, idempotent traffic (heartbeats, gossip summaries).
    Datagram,
    /// Reliable, ordered (per (peer, lane)), small-to-medium payloads, highest
    /// stream priority. Suitable for latency-sensitive cluster control traffic
    /// (gossip delta, DownYourself) that must not be delayed by bulk transfers.
    ReliableLowLatency,
    /// Reliable, ordered (per (peer, lane)), medium-sized payloads, baseline
    /// priority. The default reliable lane for general cluster messages.
    Reliable,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait MessageSender: Debug + Send + Sync + 'static {
    fn get_self_addr(&self) -> NodeAddr;

    /// Send `msg` to a known cluster member using the given delivery class.
    ///
    /// Returns `Err` if the transport refuses the message (e.g. datagram over MTU,
    /// peer unreachable, connection setup failed, payload exceeds lane cap).
    /// Callers should handle errors rather than panic; transports must not panic
    /// on a per-send failure.
    async fn send_to_node<T: Message + ?Sized>(&self, to: NodeAddr, delivery: Delivery, msg: &T) -> anyhow::Result<()>;

    /// Send `msg` to a raw socket address whose `NodeAddr.unique` is not yet known
    /// (used during join, before a peer has been identified). Reliable; transport
    /// chooses the lane (QUIC routes this through the low-latency lane).
    async fn send_to_addr<T: Message + ?Sized>(&self, to: SocketAddr, msg: &T) -> anyhow::Result<()>;

    /// Open a unidirectional reliable streaming channel for arbitrarily large
    /// transfers. The returned `LargeSendStream` exposes `write` / `finish` /
    /// `cancel`; the receiving side dispatches to `MessageModule::on_stream`
    /// for the given `module_id`.
    ///
    /// Unlike `send_to_node` with a reliable `Delivery`, large streams have:
    ///   - **no size cap** enforced by the transport,
    ///   - **no ordering** with respect to other large transfers,
    ///   - **no poisoning** — each stream is fully independent.
    ///
    /// Trust is provided by SPKI peer authentication; do not call this from
    /// modules that do not trust the peer set to bound resource use.
    async fn open_large_stream(
        &self,
        to: NodeAddr,
        module_id: MessageModuleId,
    ) -> anyhow::Result<LargeSendStream>;
}

/// Convenience helpers so call sites read naturally without repeating the enum.
/// Provided as a blanket extension on `MessageSender`; kept off the main trait
/// to avoid interfering with `#[automock]` expectations.
#[async_trait]
pub trait MessageSenderExt: MessageSender {
    async fn send_datagram<T: Message + ?Sized>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.send_to_node(to, Delivery::Datagram, msg).await
    }
    async fn send_low_latency<T: Message + ?Sized>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.send_to_node(to, Delivery::ReliableLowLatency, msg).await
    }
    async fn send_reliable<T: Message + ?Sized>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.send_to_node(to, Delivery::Reliable, msg).await
    }
}
impl<M: MessageSender + ?Sized> MessageSenderExt for M {}

#[async_trait]
pub trait Messaging: MessageSender {
    async fn register_module(&self, message_module: Arc<dyn MessageModule>);
    async fn deregister_module(&self, id: MessageModuleId);
    async fn recv(&self);
}
