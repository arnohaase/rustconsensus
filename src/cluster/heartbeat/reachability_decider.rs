pub mod fixed_timeout;
mod phi_accrual;

use std::time::Duration;
use crate::cluster::cluster_config::ClusterConfig;

/// A [ReachabilityDecider] is a stateful component for one other node. It is notified of every
///  incoming heartbeat, and it decides based on heartbeat history and the current timestamp
///  whether the node is reachable or not
pub trait ReachabilityDecider {
    fn new(config: &ClusterConfig, initial_rtt: Duration) -> Self;

    fn on_heartbeat(&mut self, rtt: Duration);

    fn is_reachable(&self) -> bool;
}
