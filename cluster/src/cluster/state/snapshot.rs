//! Immutable, point-in-time view of the cluster's world. Owns all reducer
//! logic that previously lived on `ClusterState`.
//!
//! Mutating reducers take `&mut self` and operate via `Arc::make_mut` on the
//! per-node `Arc<NodeState>` entries so that snapshots already published via
//! `ArcSwap` enjoy structural sharing: only the modified entries are cloned.
//!
//! Reducers are pure with respect to event emission: every method that can
//! produce `ClusterEvent`s returns them in a `Vec`; the actor loop is the
//! sole entity that publishes the next snapshot and then forwards events
//! to the `ClusterEventNotifier`.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tracing::{debug, info, trace, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{
    ClusterEvent, LeaderChangedData, NodeAddedData, NodeRemovedData, NodeStateChangedData,
    NodeUpdatedData, ReachabilityChangedData,
};
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision;
use crate::cluster::state::node_state::{MembershipState, NodeReachability, NodeState};
use crate::messaging::node_addr::NodeAddr;

/// Immutable, point-in-time view of the cluster state, with reducers.
///
/// Cheap to clone: per-node data is shared via `Arc<NodeState>` so that
/// updates to a single node do not force readers to copy the whole map.
#[derive(Clone, Debug)]
pub struct ClusterStateSnapshot {
    /// Address of this node.
    pub myself: NodeAddr,

    /// Shared cluster configuration.
    pub config: Arc<ClusterConfig>,

    /// Per-node state, keyed by node address. Each entry is wrapped in `Arc`
    /// so that updates to individual nodes can share unchanged entries with
    /// previous snapshots (structural sharing) instead of cloning the whole
    /// map. Mutating reducers use `Arc::make_mut` so newly-published
    /// snapshots clone only the changed entries.
    pub nodes: BTreeMap<NodeAddr, Arc<NodeState>>,

    /// Monotonic counter used as the source for `NodeReachability::counter_of_reporter`
    /// values written by this node.
    pub version_counter: u32,

    /// Last leader observed by this node, used to suppress duplicate
    /// `LeaderChanged` events.
    pub cached_old_leader: Option<NodeAddr>,
}

impl ClusterStateSnapshot {
    /// Build the initial snapshot for `myself`. Starts with `myself` in the
    /// `Joining` state, `version_counter = 0`, `cached_old_leader = None`,
    /// `snapshot_version = 0` (the handle bumps this to 1 on first publish).
    pub(crate) fn new(myself: NodeAddr, config: Arc<ClusterConfig>) -> ClusterStateSnapshot {
        let self_node = NodeState {
            addr: myself,
            membership_state: MembershipState::Joining,
            roles: config.roles.clone(),
            reachability: Default::default(),
            seen_by: BTreeSet::from_iter([myself]),
        };
        let nodes = BTreeMap::from_iter([(myself, Arc::new(self_node))]);
        ClusterStateSnapshot {
            myself,
            config,
            nodes,
            version_counter: 0,
            cached_old_leader: None,
        }
    }

    // ----------------- read helpers -----------------

    pub fn myself(&self) -> NodeAddr {
        self.myself
    }

    pub fn get_node_state(&self, addr: &NodeAddr) -> Option<&Arc<NodeState>> {
        self.nodes.get(addr)
    }

    pub fn node_states(&self) -> impl Iterator<Item = &Arc<NodeState>> {
        self.nodes.values()
    }

    pub fn is_converged(&self) -> bool {
        let n = self.num_convergence_nodes();
        self.nodes.values().all(|s| s.seen_by.len() == n)
    }

    fn num_convergence_nodes(&self) -> usize {
        self.nodes
            .values()
            .filter(|s| s.membership_state.is_gossip_partner())
            .count()
    }

    pub fn is_node_converged(&self, addr: NodeAddr) -> bool {
        match self.nodes.get(&addr) {
            Some(node) => node.seen_by.len() == self.num_convergence_nodes(),
            None => false,
        }
    }

    /// Pure read of the current leader. Does **not** emit `LeaderChanged`
    /// or mutate `cached_old_leader`; that happens exclusively inside
    /// [`Self::do_leader_actions`].
    pub fn get_leader(&self) -> Option<NodeAddr> {
        if !self.is_converged() {
            return None;
        }
        Self::calc_leader_candidate(self.config.as_ref(), self.nodes.values().map(|a| a.as_ref()))
            .map(|s| s.addr)
    }

    pub fn am_i_leader(&self) -> bool {
        self.get_leader() == Some(self.myself)
    }

    /// Public-by-crate helper used by downing strategies.
    pub(crate) fn calc_leader_candidate<'a>(
        config: &ClusterConfig,
        nodes: impl Iterator<Item = &'a NodeState>,
    ) -> Option<&'a NodeState> {
        nodes
            .filter(|s| s.is_leader_eligible(config))
            .min_by_key(|s| s.addr)
    }

    fn is_myself_still_gossipping(&self) -> bool {
        self.nodes
            .get(&self.myself)
            .map(|s| s.membership_state.is_gossip_partner())
            .unwrap_or(false)
    }

    // ----------------- reducers -----------------

    pub(crate) fn add_joiner(&mut self, addr: NodeAddr, roles: BTreeSet<String>) {
        debug!("adding joining node {:?}", addr);
        if self.nodes.contains_key(&addr) {
            return;
        }
        let node_state = NodeState {
            addr,
            membership_state: MembershipState::Joining,
            roles,
            reachability: Default::default(),
            seen_by: vec![self.myself].into_iter().collect(),
        };
        self.nodes.insert(addr, Arc::new(node_state));
    }

    pub(crate) fn promote_myself_to_weakly_up(&mut self) -> Vec<ClusterEvent> {
        if let Some(node) = self.nodes.get(&self.myself) {
            if node.membership_state == MembershipState::Joining {
                info!(
                    "promoting myself to 'weakly up' after configured timeout of {}ms",
                    self.config.weakly_up_after.unwrap().as_millis()
                );
                return self.promote_myself(MembershipState::WeaklyUp);
            }
        }
        Vec::new()
    }

    pub(crate) fn promote_myself(&mut self, new_state: MembershipState) -> Vec<ClusterEvent> {
        self.promote_node(self.myself, new_state)
    }

    pub(crate) fn promote_node(
        &mut self,
        addr: NodeAddr,
        new_state: MembershipState,
    ) -> Vec<ClusterEvent> {
        if let Some(arc) = self.nodes.get(&addr) {
            let mut state = (**arc).clone();
            state.membership_state = new_state;
            if self.is_myself_still_gossipping() {
                state.seen_by = [self.myself].into();
            }
            return self.merge_node_state(state);
        }
        Vec::new()
    }

    /// Apply a downing decision; returns `(downed_addrs, events)`.
    pub(crate) fn apply_downing_decision(
        &mut self,
        downing_decision: DowningStrategyDecision,
    ) -> (Vec<NodeAddr>, Vec<ClusterEvent>) {
        let predicate = match downing_decision {
            DowningStrategyDecision::DownUs => |s: &&Arc<NodeState>| s.is_reachable(),
            DowningStrategyDecision::DownThem => |s: &&Arc<NodeState>| !s.is_reachable(),
        };
        let to_be_downed: Vec<NodeAddr> = self
            .nodes
            .values()
            .filter(predicate)
            .map(|s| s.addr)
            .collect();

        let mut all_events = Vec::new();
        for &node in &to_be_downed {
            all_events.extend(self.promote_node(node, MembershipState::Down));
        }
        (to_be_downed, all_events)
    }

    /// Run periodic leader actions. **Only** path that emits `LeaderChanged`
    /// and updates `cached_old_leader`.
    pub(crate) fn do_leader_actions(&mut self) -> Vec<ClusterEvent> {
        use MembershipState::*;
        let mut all_events = Vec::new();

        let current_leader = self.get_leader();
        if let Some(new_leader) = current_leader {
            if self.cached_old_leader != Some(new_leader) {
                info!("new cluster leader: {:?}", new_leader);
                all_events.push(ClusterEvent::LeaderChanged(LeaderChangedData { new_leader }));
                self.cached_old_leader = Some(new_leader);
            }
        }

        if current_leader == Some(self.myself) {
            let mut to_be_promoted = Vec::new();
            for s in self.nodes.values() {
                let old_state = s.membership_state;
                if let Some(new_state) = match old_state {
                    Joining | WeaklyUp => Some(Up),
                    Leaving => Some(Exiting),
                    Exiting | Down => Some(Removed),
                    _ => None,
                } {
                    to_be_promoted.push((s.addr, new_state));
                }
            }
            for (addr, new_state) in to_be_promoted {
                debug!("leader action: promoting {:?} to {:?}", addr, new_state);
                all_events.extend(self.promote_node(addr, new_state));
            }
        }
        all_events
    }

    /// when a node is removed from gossip (i.e. it becomes 'Down' or 'Removed'), it needs to be
    ///  removed from all 'seen by' sets and its reachability observations must be cleaned up
    fn on_node_removed_from_gossip(&mut self, addr: &NodeAddr) {
        for s in self.nodes.values_mut() {
            let s_mut = Arc::make_mut(s);
            s_mut.seen_by.remove(addr);
            s_mut.reachability.remove(addr);
        }
    }

    pub(crate) fn merge_node_state(&mut self, node_state: NodeState) -> Vec<ClusterEvent> {
        let _is_converged_before = self.is_converged();
        let is_myself_still_gossipping = self.is_myself_still_gossipping();
        let myself = self.myself;

        let mut events = Vec::new();

        match self.nodes.entry(node_state.addr) {
            Entry::Occupied(mut e) => {
                trace!(
                    "merging external node state {:?} into existing state {:?}",
                    node_state,
                    e.get()
                );
                let existing = Arc::make_mut(e.get_mut());
                let old_state = existing.membership_state;
                let was_reachable = existing.is_reachable();
                let was_self_changed = existing.merge(&node_state);
                if is_myself_still_gossipping {
                    existing.seen_by.insert(myself);
                }
                let snapshot_ref: &NodeState = e.get();
                events.extend(Self::derive_state_changed_events(
                    Some(old_state),
                    was_reachable,
                    snapshot_ref,
                    was_self_changed,
                ));
                if !node_state.membership_state.is_gossip_partner() {
                    debug!(
                        "node {:?} is not a gossip partner any more, removing from all 'seen by' sets",
                        node_state.addr
                    );
                    self.on_node_removed_from_gossip(&node_state.addr);
                }
            }
            Entry::Vacant(e) => {
                trace!(
                    "merging external node state for {:?}: registering previously unknown node locally",
                    node_state.addr
                );
                let mut node_state = node_state;
                if is_myself_still_gossipping {
                    node_state.seen_by.insert(myself);
                }
                events.extend(Self::derive_state_changed_events(
                    None, true, &node_state, true,
                ));
                let new_membership_state = node_state.membership_state;
                let addr = node_state.addr;
                e.insert(Arc::new(node_state));
                if !new_membership_state.is_gossip_partner() {
                    self.on_node_removed_from_gossip(&addr);
                }
            }
        }

        if !_is_converged_before && self.is_converged() {
            debug!("cluster state fully converged");
        }

        events
    }

    fn derive_state_changed_events(
        old_state: Option<MembershipState>,
        was_reachable: bool,
        s: &NodeState,
        was_self_modified: bool,
    ) -> Vec<ClusterEvent> {
        let mut events = Vec::new();
        if !was_self_modified {
            return events;
        }
        events.push(ClusterEvent::NodeUpdated(NodeUpdatedData { addr: s.addr }));
        if old_state.is_none() {
            events.push(ClusterEvent::NodeAdded(NodeAddedData {
                addr: s.addr,
                state: s.membership_state,
            }));
        }
        if let Some(old_state) = old_state {
            if old_state != s.membership_state {
                events.push(ClusterEvent::NodeStateChanged(NodeStateChangedData {
                    addr: s.addr,
                    old_state,
                    new_state: s.membership_state,
                }));
                if s.membership_state == MembershipState::Removed {
                    events.push(ClusterEvent::NodeRemoved(NodeRemovedData { addr: s.addr }));
                }
            }
        }
        if was_reachable != s.is_reachable() {
            events.push(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                addr: s.addr,
                old_is_reachable: was_reachable,
                new_is_reachable: !was_reachable,
            }));
        }
        events
    }

    pub(crate) fn update_reachability_from_myself(
        &mut self,
        reachability: &BTreeMap<NodeAddr, bool>,
    ) -> Vec<ClusterEvent> {
        trace!("updating reachability from myself to {:?}", reachability);
        let myself = self.myself;
        let is_myself_still_gossipping = self.is_myself_still_gossipping();
        let mut lazy_version_counter = LazyCounterVersion::new(self);
        let mut all_events: Vec<ClusterEvent> = Vec::new();

        {
            let mut updated_nodes = Vec::new();
            let mut reachability_changed_nodes = Vec::new();

            for s in self
                .nodes
                .values_mut()
                .filter(|s| !reachability.contains_key(&s.addr))
            {
                let was_reachable = s.is_reachable();
                if s.reachability.get(&myself).is_some() {
                    let s_mut = Arc::make_mut(s);
                    if let Some(r) = s_mut.reachability.get_mut(&myself) {
                        trace!("node now has no reachability from myself: {:?}", s_mut.addr);
                        if !r.is_reachable {
                            r.is_reachable = true;
                            r.counter_of_reporter = lazy_version_counter.get_version();

                            s_mut.seen_by.clear();
                            if is_myself_still_gossipping {
                                s_mut.seen_by.insert(myself);
                            }
                            updated_nodes.push(s_mut.addr);
                        }
                        if was_reachable != s_mut.is_reachable() {
                            reachability_changed_nodes.push(s_mut.addr);
                        }
                    }
                }
            }

            for addr in updated_nodes {
                all_events.push(ClusterEvent::NodeUpdated(NodeUpdatedData { addr }));
            }
            for addr in reachability_changed_nodes {
                all_events.push(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                    addr,
                    old_is_reachable: false,
                    new_is_reachable: true,
                }));
            }
        }

        for (&addr, &reachable) in reachability {
            if let Some(node_arc) = self.nodes.get_mut(&addr) {
                let node = Arc::make_mut(node_arc);
                let old_is_reachable = node.is_reachable();
                trace!(
                    "pre-existing reachability {} for node {:?} (from myself)",
                    old_is_reachable,
                    addr
                );

                let was_updated = match node.reachability.entry(myself) {
                    Entry::Occupied(mut e) => {
                        if e.get().is_reachable != reachable {
                            e.get_mut().is_reachable = reachable;
                            e.get_mut().counter_of_reporter = lazy_version_counter.get_version();
                            true
                        } else {
                            false
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(NodeReachability {
                            counter_of_reporter: lazy_version_counter.get_version(),
                            is_reachable: reachable,
                        });
                        true
                    }
                };
                let new_is_reachable = node.is_reachable();

                if was_updated {
                    node.seen_by.clear();
                    if is_myself_still_gossipping {
                        node.seen_by.insert(myself);
                    }
                    all_events.push(ClusterEvent::NodeUpdated(NodeUpdatedData { addr }));
                }
                if old_is_reachable != new_is_reachable {
                    all_events.push(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                        addr,
                        old_is_reachable,
                        new_is_reachable,
                    }));
                }
            } else {
                warn!(
                    "heartbeat data for node {:?} which is not part of the cluster's known state - ignoring",
                    addr
                );
            }
        }

        lazy_version_counter.finalize(self);
        all_events
    }
}

/// Bookkeeping for [`ClusterStateSnapshot::update_reachability_from_myself`]:
/// allocates a fresh `version_counter` value only if at least one mutation
/// observed it.
pub(crate) struct LazyCounterVersion {
    version_counter: u32,
    has_change: bool,
}

impl LazyCounterVersion {
    pub fn new(snap: &ClusterStateSnapshot) -> LazyCounterVersion {
        LazyCounterVersion {
            version_counter: snap.version_counter + 1,
            has_change: false,
        }
    }

    pub fn get_version(&mut self) -> u32 {
        self.has_change = true;
        self.version_counter
    }

    pub fn finalize(self, snap: &mut ClusterStateSnapshot) {
        if self.has_change {
            snap.version_counter = self.version_counter;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::node_state::MembershipState::{Joining, Up};
    use crate::node_state;
    use crate::test_util::node::test_node_addr_from_number;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    use rstest::rstest;

    use DowningStrategyDecision::*;
    use MembershipState::*;

    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::*;
    use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision;
    use crate::cluster::state::node_state::{MembershipState, NodeReachability, NodeState};
    use crate::cluster::state::snapshot::ClusterStateSnapshot;
    use crate::messaging::node_addr::NodeAddr;
    use crate::test_util::event::*;

    fn new_snap(myself: NodeAddr, nodes: Vec<NodeState>) -> ClusterStateSnapshot {
        let mut result = ClusterStateSnapshot::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr, None)));
        for n in nodes {
            result.nodes.insert(n.addr, Arc::new(n));
        }
        result
    }

    #[test]
    fn test_new() {
        let self_addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let myself = NodeAddr { unique: 0, socket_addr: self_addr };
        let config = Arc::new(ClusterConfig::new(self_addr, None));
        let snap = ClusterStateSnapshot::new(myself, config);

        let node_state = NodeState {
            addr: myself,
            membership_state: MembershipState::Joining,
            roles: BTreeSet::default(),
            reachability: BTreeMap::default(),
            seen_by: BTreeSet::from([myself]),
        };

        assert_eq!(snap.myself(), myself);
        assert_eq!(snap.version_counter, 0);
        assert_eq!(snap.cached_old_leader, None);
        assert_eq!(snap.nodes.len(), 1);
        assert_eq!(**snap.nodes.get(&myself).unwrap(), node_state);
    }

    #[test]
    fn test_add_joiner() {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, vec![]);

        let joiner_addr = test_node_addr_from_number(5);
        snap.add_joiner(joiner_addr, ["a".to_string()].into());

        assert_eq!(
            **snap.nodes.get(&joiner_addr).unwrap(),
            NodeState {
                addr: joiner_addr,
                membership_state: Joining,
                roles: ["a".to_string()].into(),
                reachability: Default::default(),
                seen_by: [myself].into(),
            }
        );
    }

    #[test]
    fn test_add_joiner_pre_existing() {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, vec![]);
        let joiner_addr = test_node_addr_from_number(5);
        let pre_existing_state = NodeState {
            addr: joiner_addr,
            membership_state: Up,
            roles: ["b".to_string()].into(),
            reachability: Default::default(),
            seen_by: [myself, test_node_addr_from_number(2)].into(),
        };
        snap.nodes.insert(joiner_addr, Arc::new(pre_existing_state.clone()));
        snap.add_joiner(joiner_addr, ["a".to_string()].into());

        assert_eq!(**snap.nodes.get(&joiner_addr).unwrap(), pre_existing_state);
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::weakly_up(vec![node_state!(1[]:WeaklyUp->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, WeaklyUp, Up)])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![])]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1])], node_state!(1[]:Leaving->[]@[1]), vec![])]
    fn test_promote_myself_to_up(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, nodes);

        let got = snap.promote_myself(MembershipState::Up);
        assert_eq!(**snap.nodes.get(&myself).unwrap(), expected);
        assert_eq!(got, events);
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:Down->[]@[]), vec![test_updated_evt(1), test_state_evt(1, Joining, Down)])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Down->[]@[]), vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::down(vec![node_state!(1[]:Down->[]@[])], node_state!(1[]:Down->[]@[]), vec![])]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[])], node_state!(1[]:Removed->[]@[]), vec![])]
    fn test_promote_myself_to_down(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, nodes);
        let got = snap.promote_myself(MembershipState::Down);
        assert_eq!(**snap.nodes.get(&myself).unwrap(), expected);
        assert_eq!(got, events);
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:WeaklyUp->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, Joining, WeaklyUp)])]
    #[case::weakly_up(vec![node_state!(1[]:WeaklyUp->[]@[1])], node_state!(1[]:WeaklyUp->[]@[1]), vec![])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![])]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1])], node_state!(1[]:Leaving->[]@[1]), vec![])]
    fn test_promote_myself_to_weakly_up(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        // weakly_up requires config.weakly_up_after to be Some when the
        // promotion branch is taken; provide it so the info!() call has a value.
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.weakly_up_after = Some(std::time::Duration::from_millis(100));
        let mut snap = ClusterStateSnapshot::new(myself, Arc::new(config));
        for n in nodes {
            snap.nodes.insert(n.addr, Arc::new(n));
        }

        let got = snap.promote_myself_to_weakly_up();
        assert_eq!(**snap.nodes.get(&myself).unwrap(), expected);
        assert_eq!(got, events);
    }

    #[rstest]
    #[case::joining_up_1(vec![node_state!(1[]:Joining->[]@[1])], 1, Up, Some(node_state!(1[]:Up->[]@[1])), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::joining_up_2(vec![node_state!(1[]:Joining->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Up, Some(node_state!(1[]:Up->[]@[1])), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::smaller_state(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Joining, Some(node_state!(1[]:Up->[]@[1,2])), vec![])]
    #[case::same_state(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Up, Some(node_state!(1[]:Up->[]@[1,2])), vec![])]
    #[case::missing_node(vec![node_state!(1[]:Up->[]@[1,2])], 2, Up, None, vec![])]
    fn test_promote_node(#[case] nodes: Vec<NodeState>, #[case] addr: u16, #[case] to_state: MembershipState, #[case] new_state: Option<NodeState>, #[case] events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, nodes);
        let addr = test_node_addr_from_number(addr);
        let got = snap.promote_node(addr, to_state);

        assert_eq!(snap.nodes.get(&addr).map(|a| (**a).clone()), new_state);
        assert_eq!(got, events);
    }

    #[rstest]
    #[case::single_reg_us(vec![node_state!(1[]:Up->[]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::single_reg_them(vec![node_state!(1[]:Up->[]@[1])], DownThem, vec![], vec![node_state!(1[]:Up->[]@[1])], vec![])]
    #[case::two_reg_us(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], DownUs, vec![1,2], vec![node_state!(1[]:Down->[]@[]), node_state!(2[]:Down->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down), test_updated_evt(2), test_state_evt(2, Up, Down)])]
    #[case::two_reg_them(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], DownThem, vec![], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![])]
    #[case::split_us(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[]@[]), node_state!(2[]:Up->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::split_them(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownThem, vec![2], vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Down->[1:false@5]@[1])], vec![test_updated_evt(2), test_state_evt(2, Up, Down)])]
    #[case::explicit_us(vec![node_state!(1[]:Up->[2:true@6]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[2:true@6]@[]), node_state!(2[]:Up->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::explicit_them(vec![node_state!(1[]:Up->[2:true@6]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownThem, vec![2], vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Down->[1:false@5]@[1])], vec![test_updated_evt(2), test_state_evt(2, Up, Down)])]
    #[case::two_unreachable_us(vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], DownUs, vec![], vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], vec![])]
    #[case::two_unreachable_them(vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], DownThem, vec![1,2], vec![node_state!(1[]:Down->[]@[]), node_state!(2[]:Down->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down), test_updated_evt(2), test_state_evt(2, Up, Down)])]
    fn test_apply_downing_decision(
        #[case] initial_nodes: Vec<NodeState>,
        #[case] downing_strategy_decision: DowningStrategyDecision,
        #[case] expected_downed_nodes: Vec<u16>,
        #[case] expected_state: Vec<NodeState>,
        #[case] expected_events: Vec<ClusterEvent>,
    ) {
        assert_eq!(initial_nodes.len(), expected_state.len());
        let expected_downed_nodes = expected_downed_nodes
            .into_iter()
            .map(test_node_addr_from_number)
            .collect::<Vec<_>>();

        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, initial_nodes);

        let (downed_nodes, events) = snap.apply_downing_decision(downing_strategy_decision);
        assert_eq!(downed_nodes, expected_downed_nodes);
        for exp in expected_state {
            assert_eq!(**snap.nodes.get(&exp.addr).unwrap(), exp);
        }
        assert_eq!(events, expected_events);
    }

    #[rstest]
    #[case::simple_2(vec![node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::simple_3(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Up->[]@[1,3])], true)]
    #[case::simple_3_incomplete(vec![node_state!(2[]:Up->[]@[1,3]), node_state!(3[]:Up->[]@[1,3])], false)]
    #[case::joining(vec![node_state!(2[]:Joining->[]@[1,2])], true)]
    #[case::weakly_up(vec![node_state!(2[]:WeaklyUp->[]@[1,2])], true)]
    #[case::leaving(vec![node_state!(2[]:Leaving->[]@[1,2])], true)]
    #[case::exiting(vec![node_state!(2[]:Exiting->[]@[1,2])], true)]
    #[case::down(vec![node_state!(2[]:Down->[]@[1])], true)]
    #[case::removed(vec![node_state!(2[]:Removed->[]@[1])], true)]
    #[case::other_joining(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Joining->[]@[1])], true)]
    #[case::other_weakly_up(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:WeaklyUp->[]@[1])], true)]
    #[case::other_up(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:WeaklyUp->[]@[1])], true)]
    #[case::other_leaving(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Leaving->[]@[1])], true)]
    #[case::other_exiting(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Exiting->[]@[1])], true)]
    #[case::other_down(vec![node_state!(2[]:Up->[]@[1,2]), node_state!(3[]:Down->[]@[1])], true)]
    #[case::other_removed(vec![node_state!(2[]:Up->[]@[1,2]), node_state!(3[]:Removed->[]@[1])], true)]
    #[case::non_existing(vec![], false)]
    fn test_is_node_converged(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let myself = test_node_addr_from_number(1);
        let snap = new_snap(myself, nodes);
        assert_eq!(snap.is_node_converged(test_node_addr_from_number(2)), expected);
    }

    #[rstest]
    #[case::empty(vec![], true)]
    #[case::converged_1(vec![node_state!(1[]:Up->[]@[1])], true)]
    #[case::converged_2(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::converged_3(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true)]
    #[case::joining_counts(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::joining_negative_1(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[2])], false)]
    #[case::joining_negative_2(vec![node_state!(1[]:Joining->[]@[2]),node_state!(2[]:Up->[]@[2])], false)]
    #[case::down(vec![node_state!(1[]:Down->[]@[2]),node_state!(2[]:Up->[]@[2])], true)]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::exiting(vec![node_state!(1[]:Exiting->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[2]),node_state!(2[]:Up->[]@[2])], true)]
    fn test_is_converged(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let myself = test_node_addr_from_number(1);
        let snap = new_snap(myself, nodes);
        assert_eq!(snap.is_converged(), expected);
    }

    #[rstest]
    #[case::empty(vec![], true, None, None, false)]
    #[case::regular_2(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true, Some(1), None, true)]
    #[case::regular_3(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(1), None, true)]
    #[case::regular_3_same_leader(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(1), Some(1), false)]
    #[case::regular_3_new_leader(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(1), Some(2), true)]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(2), None, true)]
    #[case::joining_same_leader(vec![node_state!(1[]:Joining->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(2), Some(2), false)]
    #[case::joining_new_leader(vec![node_state!(1[]:Joining->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(2), Some(3), true)]
    #[case::not_converged_1(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], false, Some(1), None, false)]
    #[case::not_converged_2(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], false, Some(2), None, false)]
    #[case::down(vec![node_state!(1[]:Down->[]@[2,3]),node_state!(2[]:Up->[]@[2,3]),node_state!(3[]:Up->[]@[2,3])], true, Some(2), None, true)]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(1), None, true)]
    #[case::leaving_negative(vec![node_state!(1[]:Leaving->[]@[2,3]),node_state!(2[]:Up->[]@[2,3]),node_state!(3[]:Up->[]@[2,3])], false, Some(1), None, false)]
    #[case::exiting(vec![node_state!(1[]:Exiting->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true, Some(1), None, true)]
    #[case::exiting_negative(vec![node_state!(1[]:Exiting->[]@[2,3]),node_state!(2[]:Up->[]@[2,3]),node_state!(3[]:Up->[]@[2,3])], false, Some(1), None, false)]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[2,3]),node_state!(2[]:Up->[]@[2,3]),node_state!(3[]:Up->[]@[2,3])], true, Some(2), None, true)]
    fn test_get_leader(#[case] nodes: Vec<NodeState>, #[case] is_converged: bool, #[case] leader_candidate: Option<u16>, #[case] prev_leader: Option<u16>, #[case] is_event_expected: bool) {
        let leader_candidate = leader_candidate.map(test_node_addr_from_number);

        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, nodes);
        snap.cached_old_leader = prev_leader.map(test_node_addr_from_number);

        let candidate = ClusterStateSnapshot::calc_leader_candidate(
            snap.config.as_ref(),
            snap.nodes.values().map(|a| a.as_ref()),
        )
            .map(|s| s.addr);
        assert_eq!(candidate, leader_candidate);
        assert_eq!(snap.is_converged(), is_converged);
        assert_eq!(snap.get_leader(), if is_converged { leader_candidate } else { None });

        let events = snap.do_leader_actions();
        if is_event_expected {
            let got = events.iter().any(|e| {
                matches!(e, ClusterEvent::LeaderChanged(d) if d.new_leader == leader_candidate.unwrap())
            });
            assert!(got, "expected LeaderChanged event; got {:?}", events);
        }
    }

    #[rstest]
    #[case::empty(vec![], false)]
    #[case::only(vec![node_state!(1[]:Up->[]@[1])], true)]
    #[case::regular_2(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], false)]
    #[case::not_converged(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1])], false)]
    fn test_am_i_leader(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let myself = test_node_addr_from_number(1);
        let snap = new_snap(myself, nodes);
        assert_eq!(snap.am_i_leader(), expected);
    }

    #[rstest]
    #[case::stable_nop(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![])]
    #[case::new_leader_nop(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], None, vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![test_leader_evt(1)])]
    #[case::joining(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Joining->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Joining, Up)])]
    #[case::joining_several(vec![node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Joining->[]@[1,2,3]), node_state!(3[]:Joining->[]@[1,2,3])],
                            Some(1),
                            vec![node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Up->[]@[1]), node_state!(3[]:Up->[]@[1])],
                            vec![test_updated_evt(2), test_state_evt(2, Joining, Up), test_updated_evt(3), test_state_evt(3, Joining, Up)])]
    #[case::joining_not_converged(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Joining->[]@[1])], Some(1), vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Joining->[]@[1])], vec![])]
    #[case::weakly_up(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:WeaklyUp->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, WeaklyUp, Up)])]
    #[case::leaving(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Leaving->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Exiting->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Leaving, Exiting)])]
    #[case::exiting(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Exiting->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Removed->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Exiting, Removed), test_removed_evt(2)])]
    #[case::down(vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Down->[]@[1])], Some(1), vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Removed->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Down, Removed), test_removed_evt(2)])]
    #[case::not_leader(vec![node_state!(0[]:Up->[]@[0,1,2]), node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Joining->[]@[1,2,3])],
                       None,
                       vec![node_state!(0[]:Up->[]@[0,1,2]), node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Joining->[]@[1,2,3])],
                       vec![test_leader_evt(0)])]
    fn test_do_leader_actions(#[case] initial: Vec<NodeState>, #[case] prev_leader: Option<u16>, #[case] expected: Vec<NodeState>, #[case] expected_events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, initial);
        snap.cached_old_leader = prev_leader.map(test_node_addr_from_number);

        let events = snap.do_leader_actions();

        for s in expected {
            assert_eq!(**snap.nodes.get(&s.addr).unwrap(), s);
        }
        assert_eq!(events, expected_events);
    }

    #[rstest]
    #[case::add(None, node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Joining->[]@[1,3]), vec![test_updated_evt(2), test_added_evt(2, Joining)])]
    #[case::same(Some(node_state!(2[]:Joining->[]@[1,3,4])), node_state!(2[]:Joining->[]@[1,3,4]), node_state!(2[]:Joining->[]@[1,3,4]), vec![])]
    #[case::same_merge_seen_by(Some(node_state!(2[]:Joining->[]@[1,4])), node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Joining->[]@[1,3,4]), vec![])]
    #[case::older(Some(node_state!(2[]:Up->[]@[1,4])), node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Up->[]@[1,4]), vec![])]
    #[case::newer(Some(node_state!(2[]:Joining->[]@[1,4])), node_state!(2[]:Up->[]@[3]), node_state!(2[]:Up->[]@[1,3]), vec![test_updated_evt(2), test_state_evt(2, Joining, Up)])]
    #[case::become_unreachable(Some(node_state!(2[]:Up->[]@[1,4])), node_state!(2[]:Up->[5:false@6]@[3]), node_state!(2[]:Up->[5:false@6]@[1,3]), vec![test_updated_evt(2), test_reachability_evt(2, false)])]
    #[case::become_reachable(Some(node_state!(2[]:Up->[5:false@6]@[1,4])), node_state!(2[]:Up->[5:true@9]@[3]), node_state!(2[]:Up->[5:true@9]@[1,3]), vec![test_updated_evt(2), test_reachability_evt(2, true)])]
    #[case::stay_unreachable(Some(node_state!(2[]:Up->[4:false@8]@[1,4])), node_state!(2[]:Up->[5:false@6]@[3]), node_state!(2[]:Up->[4:false@8,5:false@6]@[1]), vec![test_updated_evt(2)])]
    #[case::star_reachable(Some(node_state!(2[]:Up->[4:true@8]@[1,4])), node_state!(2[]:Up->[5:true@9]@[3]), node_state!(2[]:Up->[4:true@8,5:true@9]@[1]), vec![test_updated_evt(2)])]
    fn test_merge_node_state(#[case] prev: Option<NodeState>, #[case] new_state: NodeState, #[case] expected: NodeState, #[case] expected_events: Vec<ClusterEvent>) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, vec![]);
        if let Some(prev) = prev {
            snap.nodes.insert(prev.addr, Arc::new(prev));
        }

        let events = snap.merge_node_state(new_state);
        assert_eq!(**snap.nodes.get(&expected.addr).unwrap(), expected);
        assert_eq!(events, expected_events);
    }

    #[rstest]
    #[case(Down, true)]
    #[case(Down, false)]
    #[case(Removed, true)]
    #[case(Removed, false)]
    fn test_merge_node_state_remove_from_gossip(#[case] terminal_state: MembershipState, #[case] present_before_merge: bool) {
        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, vec![]);

        let _ = snap.merge_node_state(NodeState {
            addr: test_node_addr_from_number(2),
            membership_state: Up,
            roles: Default::default(),
            reachability: BTreeMap::from([(
                test_node_addr_from_number(3),
                NodeReachability { counter_of_reporter: 5, is_reachable: false },
            )]),
            seen_by: [test_node_addr_from_number(1), test_node_addr_from_number(3)].into(),
        });

        if present_before_merge {
            let _ = snap.merge_node_state(NodeState {
                addr: test_node_addr_from_number(3),
                membership_state: Up,
                roles: Default::default(),
                reachability: Default::default(),
                seen_by: [test_node_addr_from_number(1)].into(),
            });
        }

        assert_eq!(
            snap.nodes.get(&test_node_addr_from_number(2)).unwrap().seen_by,
            BTreeSet::from([test_node_addr_from_number(1), test_node_addr_from_number(3)])
        );
        assert_eq!(
            snap.nodes.get(&test_node_addr_from_number(2)).unwrap().reachability.get(&test_node_addr_from_number(3)),
            Some(&NodeReachability { counter_of_reporter: 5, is_reachable: false }),
        );

        let _ = snap.merge_node_state(NodeState {
            addr: test_node_addr_from_number(3),
            membership_state: terminal_state,
            roles: Default::default(),
            reachability: Default::default(),
            seen_by: [test_node_addr_from_number(1)].into(),
        });

        assert_eq!(
            snap.nodes.get(&test_node_addr_from_number(2)).unwrap().seen_by,
            BTreeSet::from([test_node_addr_from_number(1)])
        );
        assert_eq!(
            snap.nodes.get(&test_node_addr_from_number(2)).unwrap().reachability.get(&test_node_addr_from_number(3)),
            None,
            "node 3's reachability observation should be removed when node 3 leaves gossip",
        );
    }

    #[rstest]
    #[case::empty(vec![], vec![], vec![], 8, vec![])]
    #[case::same_1_true(vec![(2,Some(true))], vec![(2,true)], vec![(2,(5,true))], 8, vec![])]
    #[case::same_1_false(vec![(2,Some(false))], vec![(2,false)], vec![(2,(5,false))], 8, vec![])]
    #[case::same_2(vec![(2,Some(true)),(3,Some(false))], vec![(2,true),(3,false)], vec![(2,(5,true)),(3,(5,false))], 8, vec![])]
    #[case::update_1_true(vec![(2,Some(true))], vec![(2,false)], vec![(2,(9,false))], 9, vec![test_updated_evt(2), test_reachability_evt(2,false)])]
    #[case::update_1_false(vec![(2,Some(false))], vec![(2,true)], vec![(2,(9,true))], 9, vec![test_updated_evt(2), test_reachability_evt(2,true)])]
    #[case::update_2(vec![(2,Some(true)),(3,Some(false))], vec![(2,false),(3,true)], vec![(2,(9,false)),(3,(9,true))], 9, vec![test_updated_evt(2), test_reachability_evt(2,false), test_updated_evt(3), test_reachability_evt(3,true)])]
    #[case::add_0(vec![(3,None)], vec![(3,false)], vec![(3,(9,false))], 9, vec![test_updated_evt(3), test_reachability_evt(3,false)])]
    #[case::add_0_true(vec![(3,None)], vec![(3,true)], vec![(3,(9,true))], 9, vec![test_updated_evt(3)])]
    #[case::add_1(vec![(2,Some(false)),(3,None)], vec![(2,false),(3,false)], vec![(2,(5,false)),(3,(9,false))], 9, vec![test_updated_evt(3), test_reachability_evt(3,false)])]
    #[case::add_1_true(vec![(2,Some(false)),(3,None)], vec![(2,false),(3,true)], vec![(2,(5,false)),(3,(9,true))], 9, vec![test_updated_evt(3)])]
    #[case::remove_0(vec![(3,Some(false))], vec![], vec![(3,(9,true))], 9, vec![test_updated_evt(3), test_reachability_evt(3,true)])]
    #[case::remove_0_true(vec![(3,Some(true))], vec![], vec![(3,(5,true))], 8, vec![])]
    #[case::remove_1(vec![(2,Some(false)),(3,Some(false))], vec![(2,false)], vec![(2,(5,false)),(3,(9,true))], 9, vec![test_updated_evt(3), test_reachability_evt(3,true)])]
    #[case::remove_1_true(vec![(2,Some(false)),(3,Some(true))], vec![(2,false)], vec![(2,(5,false)),(3,(5,true))], 8, vec![])]
    #[case::heartbeat_without_node(vec![], vec![(3,false)], vec![], 8, vec![])]
    fn test_update_current_reachability(
        #[case] old_reachability: Vec<(u16, Option<bool>)>,
        #[case] new_reachability: Vec<(u16, bool)>,
        #[case] expected_reachability: Vec<(u16, (u32, bool))>,
        #[case] expected_version_counter: u32,
        #[case] expected_events: Vec<ClusterEvent>,
    ) {
        assert_eq!(old_reachability.len(), expected_reachability.len());

        let new_reachability = new_reachability
            .into_iter()
            .map(|(k, v)| (test_node_addr_from_number(k), v))
            .collect::<BTreeMap<_, _>>();

        let myself = test_node_addr_from_number(1);
        let mut snap = new_snap(myself, vec![]);
        snap.version_counter = 8;

        for (k, v) in old_reachability {
            let node_state = match (k, v) {
                (2, Some(true)) => node_state!(2[]:Up->[1:true@5,99:true@111]@[1,99]),
                (2, Some(false)) => node_state!(2[]:Up->[1:false@5,99:true@111]@[1,99]),
                (2, None) => node_state!(2[]:Up->[99:true@111]@[1,99]),
                (3, Some(true)) => node_state!(3[]:Up->[1:true@5,99:true@111]@[1,99]),
                (3, Some(false)) => node_state!(3[]:Up->[1:false@5,99:true@111]@[1,99]),
                (3, None) => node_state!(3[]:Up->[99:true@111]@[1,99]),
                _ => panic!(),
            };
            snap.nodes.insert(node_state.addr, Arc::new(node_state));
        }
        let unrelated_node = node_state!(98[]:Up->[99:true@7]@[1,99]);
        snap.nodes.insert(unrelated_node.addr, Arc::new(unrelated_node.clone()));

        let initial_nodes: Vec<NodeAddr> = snap.nodes.keys().cloned().collect();

        let events = snap.update_reachability_from_myself(&new_reachability);

        for (addr, (counter_of_reporter, is_reachable)) in expected_reachability {
            let node = snap.nodes.get(&test_node_addr_from_number(addr)).unwrap();
            let node_reachability = NodeReachability { counter_of_reporter, is_reachable };
            assert_eq!(
                &node.reachability,
                &[
                    (myself, node_reachability),
                    (test_node_addr_from_number(99), NodeReachability { counter_of_reporter: 111, is_reachable: true }),
                ]
                    .into()
            );
        }

        assert_eq!(snap.nodes.keys().cloned().collect::<Vec<_>>(), initial_nodes);
        assert_eq!(snap.version_counter, expected_version_counter);

        assert_eq!(**snap.nodes.get(&unrelated_node.addr).unwrap(), unrelated_node);
        assert_eq!(events, expected_events);
    }

    #[test]
    fn test_lazy_counter_version() {
        let self_addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let myself = NodeAddr {
            unique: 0,
            socket_addr: self_addr,
        };
        let config = Arc::new(ClusterConfig::new(self_addr, None));
        let mut snap = ClusterStateSnapshot::new(myself, config);
        snap.version_counter = 24;

        let counter = LazyCounterVersion::new(&snap);
        counter.finalize(&mut snap);
        assert_eq!(24, snap.version_counter);

        let mut counter = LazyCounterVersion::new(&snap);
        assert_eq!(25, counter.get_version());
        assert_eq!(24, snap.version_counter);
        counter.finalize(&mut snap);
        assert_eq!(25, snap.version_counter);
    }
}
