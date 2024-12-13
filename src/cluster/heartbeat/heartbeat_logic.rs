use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use std::time::Duration;
use rustc_hash::FxHasher;
use tokio::time::Instant;
use tracing::{debug, error, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::heartbeat::heartbeat_messages::{HeartbeatData, HeartbeatResponseData};
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;
use crate::messaging::node_addr::NodeAddr;

pub struct HeartBeat<D: ReachabilityDecider> {
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    counter: u32,
    reference_time: Instant,
    registry: HeartbeatRegistry<D>,
}
impl <D: ReachabilityDecider> HeartBeat<D> {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>) -> HeartBeat<D> {
        HeartBeat {
            myself,
            config: config.clone(),
            counter: 0,
            reference_time: Instant::now(),
            registry: HeartbeatRegistry {
                config,
                trackers: Default::default(),
            },
        }
    }

    //TODO unit test
    pub fn heartbeat_recipients(&mut self, cluster_state: &ClusterState) -> Vec<NodeAddr> {
        let mut result = Vec::new();
        let mut num_reachable = 0;

        let node_ring = cluster_state.node_states()
            .map(|s| SortedByHash(s.addr))
            .collect::<BTreeSet<_>>();

        // iterator over the ring, starting at `myself`
        let candidates = node_ring.range((Excluded(&SortedByHash(self.myself)), Unbounded))
            .chain(node_ring.range((Unbounded, Excluded(&SortedByHash(self.myself)))));

        for candidate in candidates {
            if num_reachable == self.config.num_heartbeat_partners_per_node  {
                break;
            }

            result.push(candidate.0.clone());
            if let Some(state) = cluster_state.get_node_state(&candidate.0) {
                if state.is_reachable() {
                    num_reachable += 1;
                }
            }
            else {
                error!("node ring for heartbeat out of sync with cluster state"); //TODO
            }
        }

        self.registry.clean_up_untracked_nodes(&result);
        result
    }

    pub fn new_heartbeat_message(&mut self) -> HeartbeatData {
        self.counter += 1;
        HeartbeatData {
            counter: self.counter,
            timestamp_nanos: self.timestamp_nanos_now(),
        }
    }

    fn timestamp_nanos_now(&mut self) -> u64 {
        match Instant::now().duration_since(self.reference_time).as_nanos().try_into() {
            Ok(nanos) => nanos,
            Err(e) => {
                error!("system clock appears to have jumped widely - trying to readjust: {}", e);
                self.reference_time = Instant::now();
                u64::MAX //TODO
            }
        }
    }

    //TODO unit test
    pub fn on_heartbeat_response(&mut self, response: &HeartbeatResponseData, from: NodeAddr) {
        // Start with some sanity checks: if the response has a version that is too old, or too much
        //  time has passed since the heartbeat was sent, we ignore the response.
        // We filter out these outliers to avoid polluting our network RTT measurements,

        let received_counter = response.counter as u64; // to avoid wrap-around in a simple and robust fashion
        if received_counter + self.config.ignore_heartbeat_response_after_n_counter_increments as u64 <= self.counter as u64 {
            warn!("received heartbeat response with outdated version from {:?} - ignoring", from);
            return;
        }

        let received_timestamp_nanos = response.timestamp_nanos;
        if received_timestamp_nanos + self.config.ignore_heartbeat_response_after_n_seconds as u64 * 1_000_000_000 < self.timestamp_nanos_now() { //TODO check overflow
            warn!("received heartbeat response that took too long from {:?} - ignoring", from);
            return;
        }

        match self.timestamp_nanos_now().checked_sub(response.timestamp_nanos) {
            None => {
                warn!("system clock apparently went backwards - this is not supposed to happen");
                return;
            }
            Some(rtt_nanos) => {
                let rtt = Duration::from_nanos(rtt_nanos);
                if rtt > Duration::from_secs(self.config.ignore_heartbeat_response_after_n_seconds as u64) { //TODO Duration in the config
                    warn!("received heartbeat response that took too long from {:?} - ignoring", from);
                    return;
                }
                self.registry.on_heartbeat_response(from, rtt);
            }
        };
    }

    pub fn get_current_reachability(&self) -> BTreeMap<NodeAddr, bool> {
        self.registry.trackers.iter()
            .map(|(addr, tracker)| (addr, tracker.is_reachable()))
            .map(|(addr, b)| (addr.clone(), b))
            .collect()
    }
}


struct HeartbeatRegistry<D: ReachabilityDecider> {
    config: Arc<ClusterConfig>,
    trackers: BTreeMap<NodeAddr, D>,
}
impl <D: ReachabilityDecider> HeartbeatRegistry<D> {
    //TODO clean up untracked remotes

    //TODO unit test
    fn on_heartbeat_response(&mut self, other: NodeAddr, rtt: Duration) {
        match self.trackers.entry(other) {
            Entry::Occupied(mut e) => e.get_mut().on_heartbeat(rtt),
            Entry::Vacant(e) => {
                let tracker = D::new(self.config.as_ref(), rtt);
                e.insert(tracker);
            }
        }

        // trace!("heartbeat response: rtt={}ms, moving avg rtt={}ms, moving stddev rtt={}ms",
        //     (rtt_nanos as f64) / 1000000.0,
        //     self.trackers.get(&other).unwrap()
        //         .moving_mean_rtt_millis.expect("was just set"),
        //     self.trackers.get(&other).unwrap()
        //         .moving_variance_rtt_millis_squared.sqrt(),
        // );
    }

    //TODO unit test
    fn clean_up_untracked_nodes(&mut self, heartbeat_recipients: &[NodeAddr]) {
        let untracked_nodes = self.trackers.keys()
            .filter(|addr| !heartbeat_recipients.contains(addr))
            .cloned()
            .collect::<Vec<_>>();

        for addr in untracked_nodes {
            debug!("heartbeat was previously exchanged with {:?}, is not tracked any more", addr);
            self.trackers.remove(&addr);
        }
    }
}


#[derive(Eq, PartialEq, Clone, Debug)]
struct SortedByHash(NodeAddr);
impl Ord for SortedByHash {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut hasher = FxHasher::default();
        self.0.hash(&mut hasher);
        let self_hash = hasher.finish();

        let mut hasher = FxHasher::default();
        other.0.hash(&mut hasher);
        let other_hash = hasher.finish();

        match self_hash.cmp(&other_hash) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.0.cmp(&other.0)
        }
    }
}
impl PartialOrd for SortedByHash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::test_util::node::test_node_addr_from_number;
    use std::time::Duration;

    fn new_config() -> ClusterConfig {
        let mut config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr);
        config.rtt_moving_avg_new_weight = 0.5;
        config.rtt_min_std_dev = Duration::from_millis(20);
        config.heartbeat_interval = Duration::from_secs(1);
        config.heartbeat_grace_period = Duration::from_secs(1);
        config
    }


    #[test]
    fn test_heartbeat_logic() {
        todo!()
    }
}
