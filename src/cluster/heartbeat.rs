use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::f64::consts::PI;
use std::hash::{Hash, Hasher};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use rustc_hash::FxHasher;
use tokio::time::Instant;
use tracing::{debug, error, trace, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_messages::{HeartbeatData, HeartbeatResponseData};
use crate::cluster::cluster_state::ClusterState;
use crate::messaging::node_addr::NodeAddr;

pub struct HeartBeat {
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    counter: u32,
    reference_time: Instant,
    registry: HeartbeatRegistry,
}
impl HeartBeat {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>) -> HeartBeat {
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
                if rtt_nanos > self.config.ignore_heartbeat_response_after_n_seconds as u64 * 1_000_000_000 {
                    warn!("received heartbeat response that took too long from {:?} - ignoring", from);
                    return;
                }
                self.registry.on_heartbeat_response(from, rtt_nanos);
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


struct HeartbeatRegistry {
    config: Arc<ClusterConfig>,
    trackers: BTreeMap<NodeAddr, HeartbeatTracker>,
}
impl HeartbeatRegistry {
    //TODO clean up untracked remotes

    //TODO unit test
    fn on_heartbeat_response(&mut self, other: NodeAddr, rtt_nanos: u64) {
        match self.trackers.entry(other) {
            Entry::Occupied(mut e) => e.get_mut().on_heartbeat_roundtrip(rtt_nanos),
            Entry::Vacant(e) => {
                let mut tracker = HeartbeatTracker::new(self.config.clone());
                tracker.on_heartbeat_roundtrip(rtt_nanos);
                let _ = e.insert(tracker);
            }
        }

        trace!("heartbeat response: rtt={}ms, moving avg rtt={}ms, moving stddev rtt={}ms",
            (rtt_nanos as f64) / 1000000.0,
            self.trackers.get(&other).unwrap()
                .moving_mean_rtt_millis.expect("was just set"),
            self.trackers.get(&other).unwrap()
                .moving_variance_rtt_millis_squared.sqrt(),
        );
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


struct HeartbeatTracker {
    moving_mean_rtt_millis: Option<f64>,
    moving_variance_rtt_millis_squared: f64, //TODO lower bound during evaluation to avoid anomalies
    last_seen: Instant,
    config: Arc<ClusterConfig>,
}
impl HeartbeatTracker {
    fn new(config: Arc<ClusterConfig>) -> HeartbeatTracker {
        HeartbeatTracker {
            moving_mean_rtt_millis: None,
            moving_variance_rtt_millis_squared: 0.0,
            last_seen: Instant::now(),
            config,
        }
    }

    //TODO unit test
    fn on_heartbeat_roundtrip(&mut self, rtt_nanos: u64) {
        self.last_seen = Instant::now();

        let rtt_millis = (rtt_nanos as f64) / 1000000.0;

        if let Some(prev) = self.moving_mean_rtt_millis {
            // calculate moving avg / variance
            let alpha = self.config.rtt_moving_avg_new_weight;

            let mean = rtt_millis * alpha + prev * (1.0 - alpha);
            self.moving_mean_rtt_millis = Some(mean);

            let s = (mean - rtt_millis).powi(2);
            self.moving_variance_rtt_millis_squared = s * alpha + self.moving_variance_rtt_millis_squared * (1.0 - alpha);
        }
        else {
            // first RTT response
            self.moving_mean_rtt_millis = Some(rtt_millis);
            self.moving_variance_rtt_millis_squared = 0.0;
        }
    }

    const MAX_PHI: f64 = 1e9;

    //TODO unit test
    fn phi(&self) -> f64 {
        let nanos_since_last_seen = Instant::now().duration_since(self.last_seen).as_nanos() as f64;

        let rtt_mean_nanos = self.moving_mean_rtt_millis.unwrap_or(0.0) * 1000000.0;

        let mut rtt_std_dev_millis = self.moving_variance_rtt_millis_squared.sqrt();
        let min_std_dev_millis = (self.config.rtt_min_std_dev.as_nanos() as f64) / 1000000.0;
        if rtt_std_dev_millis < min_std_dev_millis {
            rtt_std_dev_millis = min_std_dev_millis;
        }

        let millis_overdue = (
            nanos_since_last_seen
            - self.config.heartbeat_interval.as_nanos() as f64
            - self.config.heartbeat_grace_period.as_nanos() as f64
            - rtt_mean_nanos
        ) / 1000000.0;

        if millis_overdue < 0.0 {
            return 0.0;
        }

        fn gaussian(x: f64, sigma: f64) -> f64 {
            let exponent = -x.powi(2) / (2.0 * sigma.powi(2));
            let coefficient = 1.0 / (sigma * (2.0 * PI).sqrt());
            coefficient * exponent.exp()
        }

        let g = gaussian(millis_overdue, rtt_std_dev_millis);
        if g < 1.0 / Self::MAX_PHI {
            return Self::MAX_PHI;
        }
        1.0 / g
    }

    pub fn is_reachable(&self) -> bool {
        let phi = self.phi();
        let result = phi < self.config.reachability_phi_threshold;

        // trace!("reachability for {:?}: rtt={}ms, variance={}ms, phi={} -> {}",
        //     self.tracked_node,
        //     self.moving_mean_rtt.unwrap_or(0.0)/ 1000000.0,
        //     self.moving_variance_rtt / 1000000.0,
        //     phi,
        //     result,
        // );

        result
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
