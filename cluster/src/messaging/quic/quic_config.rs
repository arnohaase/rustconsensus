//! Configuration for `QuicMessaging`.
//!
//! Each node owns a long-lived self-signed certificate + key. The cluster's
//! shared "secret" is the set of trusted SPKI SHA-256 hashes (one per node).
//! See `spki_verifier` for the verification semantics.

use std::net::SocketAddr;
use std::time::Duration;

use rustc_hash::FxHashSet;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::messaging::quic::spki_verifier::SpkiHash;

/// Per-lane policy for the *persistent* reliable lanes (`ReliableLowLatency`
/// and `Reliable`). Each `Delivery::Reliable*` variant maps to one of these.
/// The `priority` is forwarded to `quinn::SendStream::set_priority` so that
/// the QUIC scheduler favors higher-priority lanes when bytes contend for the
/// wire; `max_msg_size` is enforced before any bytes hit the network.
///
/// Large streaming transfers (`MessageSender::open_large_stream`) do NOT use
/// this config: they have no per-message cap and use a hard-coded stream
/// priority lower than every persistent lane.
#[derive(Debug, Clone, Copy)]
pub struct PersistentLaneConfig {
    /// Hard upper bound on serialized message size for this lane (bytes,
    /// excluding the 4-byte length prefix; the cap matches the on-wire frame
    /// body length).
    pub max_msg_size: usize,
    /// Stream priority forwarded to quinn. Higher means scheduled earlier.
    pub priority: i32,
}

#[derive(Debug)]
pub struct QuicConfig {
    /// Local UDP socket the QUIC endpoint binds to (accepts incoming and
    /// originates outgoing connections).
    pub self_addr: SocketAddr,

    /// This node's self-signed leaf certificate, DER-encoded.
    pub cert_der: CertificateDer<'static>,

    /// Private key matching `cert_der`, DER-encoded.
    pub key_der: PrivateKeyDer<'static>,

    /// SPKI SHA-256 hashes of all certs trusted as cluster peers.
    /// Must include this node's own SPKI hash if it ever talks to itself.
    pub trusted_spki: FxHashSet<SpkiHash>,

    /// QUIC idle timeout; the connection is dropped if no traffic flows
    /// for this long. Heartbeats keep healthy connections alive.
    pub idle_timeout: Duration,

    /// Per-connection datagram buffers (bytes).
    pub datagram_buffer_size: u32,

    /// Low-latency persistent reliable lane (e.g. gossip delta, DownYourself).
    pub lane_low_latency: PersistentLaneConfig,

    /// Default persistent reliable lane (general cluster control traffic).
    pub lane_regular: PersistentLaneConfig,

    /// Per-connection cap on concurrent inbound uni-streams. The only
    /// transport-level back-pressure on large streaming transfers, so peers
    /// must be authenticated (SPKI pinning) to use this safely.
    pub max_concurrent_uni_streams: u32,
}

impl QuicConfig {
    pub fn new(
        self_addr: SocketAddr,
        cert_der: CertificateDer<'static>,
        key_der: PrivateKeyDer<'static>,
        trusted_spki: FxHashSet<SpkiHash>,
    ) -> Self {
        Self {
            self_addr,
            cert_der,
            key_der,
            trusted_spki,
            idle_timeout: Duration::from_secs(30),
            datagram_buffer_size: 64 * 1024,
            lane_low_latency: PersistentLaneConfig {
                max_msg_size: 16 * 1024 * 1024,
                priority: 10,
            },
            lane_regular: PersistentLaneConfig {
                max_msg_size: 16 * 1024 * 1024,
                priority: 0,
            },
            max_concurrent_uni_streams: 64,
        }
    }

    /// Inbound stream byte ceiling for persistent reliable lanes — the
    /// largest single frame any persistent lane permits. The receiver is
    /// lane-agnostic so it enforces a single global cap for the framed
    /// persistent path. Large streaming transfers are NOT subject to this
    /// cap; they are bounded only by `max_concurrent_uni_streams` and by
    /// receiver back-pressure.
    pub fn max_persistent_inbound_message_size(&self) -> usize {
        self.lane_low_latency
            .max_msg_size
            .max(self.lane_regular.max_msg_size)
    }
}
