use std::time::Duration;
use rustc_hash::FxHashSet;

pub struct ClusterConfig {
    pub num_gossip_partners: usize,
    /// number between 0.0 and 1.0 that determines the probability to gossip with a node that has
    ///  proven differences from myself (rather than a node that may have an identical perception
    ///  of the cluster's state)
    pub gossip_with_differing_state_probability: f64,
    pub regular_gossip_interval: Duration,

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

    pub internal_event_queue_size: usize,
}