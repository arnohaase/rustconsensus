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
    reference_time: Instant,
    registry: HeartbeatRegistry<D>,
}
impl <D: ReachabilityDecider> HeartBeat<D> {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>) -> HeartBeat<D> {
        HeartBeat {
            myself,
            config: config.clone(),
            reference_time: Instant::now(),
            registry: HeartbeatRegistry {
                config,
                per_node: Default::default(),
            },
        }
    }

    //TODO unit test
    pub fn heartbeat_recipients(&mut self, cluster_state: &ClusterState) -> Vec<NodeAddr> {
        let mut result = Vec::new();
        let mut num_reachable = 0;

        // shuffle the ordering of nodes to improve the likelihood of physically and topologically
        //  distant nodes monitoring each other
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

    pub fn create_heartbeat_message(&mut self) -> HeartbeatData {
        HeartbeatData {
            timestamp_nanos: self.now_as_nanos(),
        }
    }

    //TODO unit test
    /// The heartbeat protocol is request / response based: A sends a heartbeat message to monitored
    ///  node B, which echos the message back to A. This makes the protocol coordination free: Only
    ///  A needs to know that it monitors B, while B does not need to know which nodes are monitoring
    ///  it.
    ///
    /// This method is called in A when it receives a response from B: This means there was a
    ///  'successful' heartbeat, and B is (more or less) reachable from A.
    pub fn on_heartbeat_response(&mut self, response: &HeartbeatResponseData, from: NodeAddr) {
        let rtt = self.timestamp_from_nanos(response.timestamp_nanos).elapsed();

        // Start with some sanity checks: if too much time has passed since the heartbeat was sent,
        //  we ignore the response: Round trips that take forever and a day are the same as lost
        //  messages from an application perspective.
        if rtt.is_zero() {
            warn!("heartbeat from {:?}) arrived before it was sent - this points to manipulations at the network level", from);
            return;
        }
        if rtt > self.config.ignore_heartbeat_response_after {
            warn!("received heartbeat response that took too long from {:?} - ignoring", from);
            return;
        }

        self.registry.on_heartbeat_response(from, rtt);
    }

    //TODO unit test
    fn now_as_nanos(&self) -> u64 {
        self.reference_time.elapsed().as_nanos() as u64  //TODO overflow
    }
    //TODO unit test
    fn timestamp_from_nanos(&self, nanos: u64) -> Instant {
        self.reference_time + Duration::from_nanos(nanos)
    }

    pub fn get_current_reachability(&self) -> BTreeMap<NodeAddr, bool> {
        self.registry.per_node.iter()
            .map(|(addr, tracker)| (addr, tracker.is_reachable()))
            .map(|(addr, b)| (addr.clone(), b))
            .collect()
    }
}


struct HeartbeatRegistry<D: ReachabilityDecider> {
    config: Arc<ClusterConfig>,
    per_node: BTreeMap<NodeAddr, D>,
}
impl <D: ReachabilityDecider> HeartbeatRegistry<D> {
    //TODO unit test
    fn on_heartbeat_response(&mut self, other: NodeAddr, rtt: Duration) {
        match self.per_node.entry(other) {
            Entry::Occupied(mut e) => e.get_mut().on_heartbeat(rtt),
            Entry::Vacant(e) => {
                let tracker = D::new(self.config.as_ref(), rtt);
                e.insert(tracker);
            }
        }
    }

    //TODO unit test
    fn clean_up_untracked_nodes(&mut self, heartbeat_recipients: &[NodeAddr]) {
        let untracked_nodes = self.per_node.keys()
            .filter(|addr| !heartbeat_recipients.contains(addr))
            .cloned()
            .collect::<Vec<_>>();

        for addr in untracked_nodes {
            debug!("heartbeat was previously exchanged with {:?}, is not tracked any more", addr);
            self.per_node.remove(&addr);
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
