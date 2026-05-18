use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;
use rustc_hash::{FxHashSet};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use crate::messaging::quic::quic_config::QuicConfig;
use crate::messaging::quic::spki_verifier::SpkiHash;

#[derive(Debug)]
pub struct ClusterConfig {
    pub transport_config: QuicConfig,

    pub roles: BTreeSet<String>,

    pub num_gossip_partners: usize,
    /// number between 0.0 and 1.0 that determines the probability to gossip with a node that has
    ///  proven differences from myself (rather than a node that may have an identical perception
    ///  of the cluster's state)
    pub gossip_with_differing_state_min_probability: f64,
    pub converged_gossip_interval: Duration,
    pub unconverged_gossip_interval: Duration,

    pub num_heartbeat_partners_per_node: usize,
    pub ignore_heartbeat_response_after: Duration,
    /// >0 and < 1
    pub rtt_moving_avg_new_weight: f64,

    pub heartbeat_interval: Duration,
    /// safety margin during which we decide not to worry about missing heartbeats
    pub heartbeat_grace_period: Duration,
    pub reachability_phi_threshold: f64,
    pub reachability_phi_min_stddev: Duration,

    pub stability_period_before_downing: Duration,
    pub unstable_thrashing_timeout: Duration,

    pub leader_action_interval: Duration,
    pub leader_eligible_roles: Option<FxHashSet<String>>,

    pub weakly_up_after: Option<Duration>,

    pub discovery_seed_node_retry_interval: Duration,
    pub discovery_seed_node_give_up_timeout: Duration,
}

impl ClusterConfig {
    /// Construct a default cluster config.
    ///
    /// Peer authentication uses self-signed certs and SPKI pinning:
    /// `cert_der` / `key_der` is this node's long-lived identity, and
    /// `trusted_spki` is the set of SPKI SHA-256 hashes of all peers'
    /// certificates that this node will accept. The peer set is operator-
    /// managed (no CA, no hostnames, no chain validation).
    pub fn new(
        self_addr: SocketAddr,
        cert_der: CertificateDer<'static>,
        key_der: PrivateKeyDer<'static>,
        trusted_spki: FxHashSet<SpkiHash>,
    ) -> ClusterConfig {
        ClusterConfig {
            transport_config: QuicConfig::new(self_addr, cert_der, key_der, trusted_spki),
            roles: Default::default(),
            num_gossip_partners: 3,
            gossip_with_differing_state_min_probability: 0.8,
            converged_gossip_interval: Duration::from_secs(1),
            unconverged_gossip_interval: Duration::from_millis(250),
            num_heartbeat_partners_per_node: 9,
            ignore_heartbeat_response_after: Duration::from_secs(4),
            rtt_moving_avg_new_weight: 0.5,
            heartbeat_interval: Duration::from_secs(1),
            heartbeat_grace_period: Duration::from_secs(1),
            reachability_phi_threshold: 0.99999999,
            reachability_phi_min_stddev: Duration::from_millis(100),
            stability_period_before_downing: Duration::from_secs(5),
            unstable_thrashing_timeout: Duration::from_secs(20),
            leader_action_interval: Duration::from_secs(1),
            leader_eligible_roles: None,
            weakly_up_after: Some(Duration::from_secs(7)),
            discovery_seed_node_retry_interval: Duration::from_secs(1),
            discovery_seed_node_give_up_timeout: Duration::from_secs(60),
        }
    }

    pub fn self_addr(&self) -> SocketAddr {
        self.transport_config.self_addr
    }
}

#[cfg(test)]
impl ClusterConfig {
    /// Test convenience: mint a throwaway self-signed cert and a trust set
    /// containing only that cert's SPKI. Equivalent to the old
    /// `ClusterConfig::new(self_addr, None)`.
    pub fn new_for_test(self_addr: SocketAddr) -> ClusterConfig {
        let (cert, key, spki) = mint_test_identity();
        let mut trusted = FxHashSet::default();
        trusted.insert(spki);
        ClusterConfig::new(self_addr, cert, key, trusted)
    }
}

#[cfg(test)]
fn mint_test_identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>, SpkiHash) {
    use crate::messaging::quic::spki_verifier::spki_hash_of_cert;
    let mut params = rcgen::CertificateParams::new(vec!["test.node".to_string()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der()).unwrap();
    let spki = spki_hash_of_cert(&cert_der).unwrap();
    (cert_der, key_der, spki)
}
