use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;
use rustc_hash::FxHashSet;
use transport::config::RudpConfig;

#[derive(Debug)]
pub struct ClusterConfig {
    pub transport_config: RudpConfig,

    pub roles: BTreeSet<String>,

    pub messaging_shared_secret: Vec<u8>,

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
    pub fn new(self_addr: SocketAddr, encryption_key: Option<Vec<u8>>) -> ClusterConfig {
        ClusterConfig {
            transport_config: RudpConfig::default(self_addr, encryption_key),
            roles: Default::default(),
            messaging_shared_secret: b"no secret".to_vec(),
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
