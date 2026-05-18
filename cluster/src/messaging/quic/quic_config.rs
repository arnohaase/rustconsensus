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

/// Per-lane policy for reliable sends. Each `Delivery::Reliable*` variant maps
/// to one of these. The `priority` is forwarded to `quinn::SendStream::set_priority`
/// so that the QUIC scheduler favors higher-priority lanes when bytes contend
/// for the wire; `max_msg_size` is enforced before any bytes hit the network.
#[derive(Debug, Clone, Copy)]
pub struct LaneConfig {
    /// Hard upper bound on serialized message size for this lane (bytes,
    /// including the 16-byte wire header).
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

    /// Low-latency reliable lane (e.g. gossip delta, DownYourself).
    pub lane_low_latency: LaneConfig,

    /// Default reliable lane (general cluster control traffic).
    pub lane_regular: LaneConfig,

    /// Large reliable lane (bulk application payloads).
    pub lane_large: LaneConfig,

    /// Per-connection cap on concurrent inbound uni-streams. Bounds the
    /// in-flight memory a peer can pin via the (256 MiB by default) large lane.
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
            lane_low_latency: LaneConfig {
                max_msg_size: 16 * 1024 * 1024,
                priority: 10,
            },
            lane_regular: LaneConfig {
                max_msg_size: 16 * 1024 * 1024,
                priority: 0,
            },
            lane_large: LaneConfig {
                max_msg_size: 256 * 1024 * 1024,
                priority: -10,
            },
            max_concurrent_uni_streams: 64,
        }
    }

    /// Inbound stream byte ceiling — the largest payload any lane permits.
    /// The receiver is lane-agnostic so it enforces a single global cap.
    pub fn max_inbound_message_size(&self) -> usize {
        self.lane_low_latency
            .max_msg_size
            .max(self.lane_regular.max_msg_size)
            .max(self.lane_large.max_msg_size)
    }
}
