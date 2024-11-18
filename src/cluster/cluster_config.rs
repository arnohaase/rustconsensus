use std::time::Duration;
use rustc_hash::FxHashSet;

pub struct ClusterConfig {
    pub num_gossip_partners: usize,
    /// number between 0.0 and 1.0 that determines the probability to gossip with a node that has
    ///  proven differences from myself (rather than a node that may have an identical perception
    ///  of the cluster's state)
    pub gossip_with_differing_state_probability: f64,
    pub converged_gossip_interval: Duration,
    pub unconverged_gossip_interval: Duration,

    pub num_heartbeat_partners_per_node: usize,
    pub ignore_heartbeat_response_after_n_counter_increments: u32,
    pub ignore_heartbeat_response_after_n_seconds: u32,
    /// >0 and < 1
    pub rtt_moving_avg_new_weight: f64,
    pub rtt_min_std_dev: Duration,

    pub heartbeat_interval: Duration,
    /// safety margin during which we decide not to worry about missing heartbeats
    pub heartbeat_grace_period: Duration,
    pub reachability_phi_threshold: f64,

    pub leader_action_interval: Duration,
    pub leader_eligible_roles: Option<FxHashSet<String>>,

    pub weakly_up_after: Option<Duration>,

    pub discovery_seed_node_retry_interval: Duration,
    pub discovery_seed_node_give_up_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self { //TODO default values
        ClusterConfig {
            num_gossip_partners: 3,
            gossip_with_differing_state_probability: 0.8,
            converged_gossip_interval: Duration::from_secs(1),
            unconverged_gossip_interval: Duration::from_millis(250),
            num_heartbeat_partners_per_node: 9,
            ignore_heartbeat_response_after_n_counter_increments: 4,
            ignore_heartbeat_response_after_n_seconds: 4,
            rtt_moving_avg_new_weight: 0.95,
            rtt_min_std_dev: Duration::from_millis(20),
            heartbeat_interval: Duration::from_secs(1),
            heartbeat_grace_period: Duration::from_secs(1),
            reachability_phi_threshold: 8.0,
            leader_action_interval: Duration::from_secs(1),
            leader_eligible_roles: None,
            weakly_up_after: Some(Duration::from_secs(7)),
            discovery_seed_node_retry_interval: Duration::from_secs(1),
            discovery_seed_node_give_up_timeout: Duration::from_secs(60),
        }
    }
}