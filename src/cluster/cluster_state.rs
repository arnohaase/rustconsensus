use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::warn;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{ClusterEvent, ClusterEventNotifier, LeaderChangedData, NodeAddedData, NodeStateChangedData, NodeUpdatedData, ReachabilityChangedData};
use crate::messaging::node_addr::NodeAddr;
use crate::util::crdt::{Crdt, CrdtOrdering};

pub struct ClusterState {
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    nodes_with_state: FxHashMap<NodeAddr, NodeState>,
    event_notifier: Arc<ClusterEventNotifier>,
    version_counter: u32,
    /// we track the 'leader' even if there is no convergence (e.g. if it is unreachable) for convenience
    ///  of applications built on top of the cluster. Leader actions of the cluster are performed only
    ///  when convergence is reached.
    leader: Option<NodeAddr>,
}
impl ClusterState {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_event_queue: Arc<ClusterEventNotifier>) -> ClusterState {
        ClusterState {
            myself,
            config,
            nodes_with_state: Default::default(),
            event_notifier: cluster_event_queue,
            version_counter: 0,
            leader: None,
        }
    }

    pub fn myself(&self) -> NodeAddr {
        self.myself
    }

    pub fn node_states(&self) -> impl Iterator<Item=&NodeState> {
        self.nodes_with_state.values()
    }

    pub fn get_node_state(&self, addr: &NodeAddr) -> Option<&NodeState> {
        self.nodes_with_state.get(addr)
    }

    //TODO unit test
    /// returns the node that is the leader in the current topology once state converges (which
    ///  can only happen if all nodes are reachable)
    async fn recalc_leader_candidate(&mut self) {
        //TODO does this require more sophisticated handling? Give preference to some states
        // over others? Or does the timestamp of joining take care of that well enough?

        let new_leader = self.nodes_with_state.values()
            .filter(|s| s.membership_state.is_leader_eligible())
            .filter(|s| {
                if let Some(leader_eligible_roles) = &self.config.leader_eligible_roles {
                    leader_eligible_roles.iter()
                        .any(|role| s.roles.contains(role))
                }
                else {
                    true
                }
            })
            .map(|s| s.addr)
            .min();

        if new_leader != self.leader {
            self.send_event(ClusterEvent::LeaderChanged(LeaderChangedData {
                old_leader: self.leader,
                new_leader,
            })).await;
            self.leader = new_leader;
        }
    }

    //TODO logging / handling if there is no leader candidate (e.g. because no node has one of the leader roles)

    pub fn get_leader(&self) -> Option<NodeAddr> {
        self.leader
    }

    pub fn am_i_leader(&self) -> bool {
        self.leader == Some(self.myself)
    }


    pub fn is_converged(&self) -> bool {
        let num_convergence_nodes = self.nodes_with_state.values()
            .filter(|s| s.membership_state.is_gossip_partner())
            .count();

        self.nodes_with_state.values()
            .all(|s| s.seen_by.len() == num_convergence_nodes)
    }

    /// This is the internal handler for all changes to a given node state. It updates the 'seen by'
    ///  set and sends change events.
    async fn state_changed(myself: NodeAddr, old_state: Option<MembershipState>, s: &mut NodeState, crdt_ordering: CrdtOrdering, other_seen_by: &FxHashSet<NodeAddr>, event_notifier: Arc<ClusterEventNotifier>) {
        use CrdtOrdering::*;

        if crdt_ordering == Equal || crdt_ordering == SelfWasBigger {
            s.seen_by.insert(myself);
            return;
        }

        s.seen_by.clear();
        s.seen_by.insert(myself);
        if crdt_ordering == OtherWasBigger {
            for &addr in other_seen_by {
                s.seen_by.insert(addr);
            }
        }

        event_notifier.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr: s.addr })).await;
        if let Some(old_state) = old_state {
            if old_state != s.membership_state {
                event_notifier.send_event(ClusterEvent::NodeStateChanged(NodeStateChangedData {
                    addr: s.addr,
                    old_state,
                    new_state: s.membership_state,
                })).await;
            }
        }
        else {
            event_notifier.send_event(ClusterEvent::NodeAdded(NodeAddedData {
                addr: s.addr,
                state: s.membership_state
            })).await;
        }
    }

    /// This function is meant to be called at regular intervals on all nodes - it checks who is
    ///  currently the leader, ensures convergence and then performs leader actions if this
    ///  is actually the leader node
    pub async fn leader_actions(&mut self) {
        use MembershipState::*;

        self.recalc_leader_candidate().await;

        if self.am_i_leader() && self.is_converged() {
            for s in self.nodes_with_state.values_mut() {
                let old_state = s.membership_state;
                if let Some(new_state) = match old_state {
                    Joining | WeaklyUp => Some(Up),
                    Leaving => Some(Exiting),
                    Exiting | Down => Some(Removed),
                    _ => None
                } {
                    s.membership_state = new_state;
                    Self::state_changed(self.myself, Some(old_state), s, CrdtOrdering::NeitherWasBigger, &Default::default(), self.event_notifier.clone()).await;
                }
            }

            //TODO unreachable -> Down --> split brain handling etc.
        }
    }

    pub async fn merge_node_state(&mut self, mut state: NodeState) {
        let addr = state.addr;

        match self.nodes_with_state.entry(state.addr) {
            Entry::Occupied(mut e) => {
                let old_state = e.get().membership_state;
                let crdt_ordering = e.get_mut().merge(&state);
                Self::state_changed(self.myself, Some(old_state), e.get_mut(), crdt_ordering, &state.seen_by, self.event_notifier.clone()).await;
            }
            Entry::Vacant(e) => {
                let other_seen_by = state.seen_by;
                state.seen_by = Default::default();

                Self::state_changed(self.myself, None, &mut state, CrdtOrdering::OtherWasBigger, &other_seen_by, self.event_notifier.clone()).await;

                e.insert(state.clone());
            }
        }
    }

    async fn send_event(&self, event: ClusterEvent) {
        self.event_notifier.send_event(event).await;
    }

    //TODO unit test
    pub async fn update_current_reachability(&mut self, reachability: &FxHashMap<NodeAddr, bool>) {
        let mut lazy_version_counter = LazyCounterVersion::new(self);

        {
            let mut updated_nodes = Vec::new();
            let mut reachablility_changed_nodes = Vec::new();

            for s in self.nodes_with_state.values_mut()
                .filter(|s| !reachability.contains_key(&s.addr))
            {
                // mark nodes as 'reachable' that were previously tracked for reachability by this node
                //  but are not tracked anymore (due to a node joining or unreachable nodes in between
                //  becoming reachable)
                if let Some(r) = s.reachability.get_mut(&self.myself) {
                    if !r.is_reachable {
                        r.is_reachable = true;
                        r.counter_of_reporter = lazy_version_counter.get_version();

                        //TODO refactor - extract shared code with the loop below

                        s.seen_by.clear();
                        s.seen_by.insert(self.myself);

                        updated_nodes.push(s.addr);
                    }

                    if s.is_reachable() {
                        // it was not reachable previously, so this means the reachability changed
                        reachablility_changed_nodes.push(s.addr);
                    }
                }
            }

            for addr in updated_nodes {
                self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr })).await;
            }
            for addr in reachablility_changed_nodes {
                self.send_event(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                    addr,
                    old_is_reachable: false,
                    new_is_reachable: true,
                })).await;
            }
        }

        for (&addr, &reachable) in reachability {
            if let Some(node) = self.nodes_with_state.get_mut(&addr) {
                let old_is_reachable = node.is_reachable();

                let was_updated = match node.reachability.entry(self.myself) {
                    Entry::Occupied(mut e) => {
                        if e.get().is_reachable != reachable {
                            e.get_mut().is_reachable = reachable;
                            e.get_mut().counter_of_reporter = lazy_version_counter.get_version();
                            true
                        }
                        else {
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
                    // we changed the node's reachability information as seen from self, so there
                    //  is a new version of the node's data that was not seen by any other nodes
                    //  yet and that must be spread by gossip
                    node.seen_by.clear();
                    node.seen_by.insert(self.myself);

                    self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr })).await;
                }

                if old_is_reachable != new_is_reachable {
                    self.send_event(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                        addr,
                        old_is_reachable,
                        new_is_reachable,
                    })).await;
                }
            }
            else {
                warn!("reachability data for node {:?} which is not part of the cluster's known state - ignoring", addr);
            }
        }

        lazy_version_counter.finalize(self);
    }
}

struct LazyCounterVersion {
    version_counter: u32,
    has_change: bool,
}
impl LazyCounterVersion {
    pub fn new(cluster_state: &ClusterState) -> LazyCounterVersion {
        LazyCounterVersion {
            version_counter: cluster_state.version_counter + 1,
            has_change: false,
        }
    }

    pub fn get_version(&mut self) -> u32 {
        self.has_change = true;
        self.version_counter
    }

    pub fn finalize(self, cluster_state: &mut ClusterState) {
        if self.has_change {
            cluster_state.version_counter = self.version_counter;
        }
    }
}


#[derive(Clone, Debug)]
pub struct NodeState {
    pub addr: NodeAddr,
    pub membership_state: MembershipState,
    pub roles: FxHashSet<String>,
    pub reachability: FxHashMap<NodeAddr, NodeReachability>, //TODO when to clean up 'is_reachable == true' entries
    pub seen_by: FxHashSet<NodeAddr>,
}
impl NodeState {
    //TODO unit test
    pub fn is_reachable(&self) -> bool {
        self.reachability.values()
            .all(|r| r.is_reachable)
    }

    pub fn merge(&mut self, other: &NodeState) -> CrdtOrdering {
        assert_eq!(self.addr, other.addr);

        let mut result = CrdtOrdering::Equal;

        let membership_state_result = self.membership_state.merge_from(&other.membership_state);
        result = result.merge(membership_state_result);

        let roles_result = self.roles.merge_from(&other.roles);
        if roles_result != CrdtOrdering::Equal {
            warn!("different roles when merging gossip state for node {:?} - merged and proceeding, but this is a bug", self.addr);
        }
        result = result.merge(roles_result);

        for (addr, r_self) in self.reachability.iter_mut() {
            if let Some(r_other) = other.reachability.get(addr) {
                match Ord::cmp(&r_self.counter_of_reporter, &r_other.counter_of_reporter) {
                    Ordering::Less => {
                        r_self.counter_of_reporter = r_other.counter_of_reporter;
                        r_self.is_reachable = r_other.is_reachable;
                        result.merge(CrdtOrdering::OtherWasBigger);
                    }
                    Ordering::Equal => {
                        if r_self.is_reachable != r_other.is_reachable {
                            warn!("gossip inconsistency for reachability of node {:?} as seen from {:?}@{}: ", self.addr, addr, r_self.counter_of_reporter);
                        }
                    }
                    Ordering::Greater => {
                        result.merge(CrdtOrdering::SelfWasBigger);
                    }
                }
            }
            else {
                result.merge(CrdtOrdering::SelfWasBigger);
            }
        }

        let reporters_only_in_other = other.reachability.iter()
            .filter(|(a, r)| !self.reachability.contains_key(a))
            .map(|(a, r)| (*a, r.clone()))
            .collect::<Vec<_>>();

        for (addr, r) in reporters_only_in_other {
            let _ = self.reachability.insert(addr, r);
            result.merge(CrdtOrdering::OtherWasBigger);
        }

        result
    }
}

#[derive(Clone, Debug, Hash)]
pub struct NodeReachability {
    /// a node reporting a change in reachability for a node attaches a strictly monotonous
    ///  counter so that reachability can be merged in a coordination-free fashion
    pub counter_of_reporter: u32,
    /// only `reachable=false` is really of interest, reachability being the default. But storing
    ///  reachability is necessary to spread that information by gossip.
    pub is_reachable: bool,
}

/// see https://doc.akka.io/docs/akka/current/typed/cluster-membership.html
#[repr(u8)]
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
pub enum MembershipState {
    /// A node has registered its wish to join the _cluster, starting dissemination of that wish
    ///  through gossip - but the leader has not yet transitions the node to 'up' (after gossip
    ///  convergence was reached)
    Joining = 1,
    /// todo
    WeaklyUp = 2,
    /// The regular state for a node that is 'up and running', a full member of the _cluster. Note
    ///  that reachability (or lack thereof) is orthogonal to states, so a node can be 'up' but
    ///  (temporarily) unreachable.
    Up = 3,
    /// A node transitions to 'Leaving' when it starts to leave the _cluster (typically as part of
    ///  its shutdown). Nodes in state 'leaving' are still full members of the _cluster, but this
    ///  state allows 'higher-up' components built on top of the _cluster to prepare for a node
    ///  leaving the _cluster in a graceful manner (e.g resharding), i.e. without any gap in
    ///  operation.
    Leaving = 4,
    /// Once all preparations for a node leaving the _cluster are completed (i.e. confirmed by
    ///  all registered components on the leader node), the leader transitions a node to 'Exiting'.
    ///  An Exiting node is basically not part of the _cluster anymore, and this is a transient
    ///  state for reaching gossip consensus before the leader moves the node to 'Removed'
    Exiting = 5,
    /// 'Down' is not part of a node's regular lifecycle, but is assigned to unreachable nodes
    ///  algorithmically once some threshold of unreachability is passed; the details are intricate.
    ///
    /// In terms of a node's state CRDT, the transition 'Down' is irreversible, and once there
    ///  is consensus over a node being 'down', it is automatically propagated to 'Removed' by the
    ///  leader.
    ///
    /// Note that 'down' nodes are excluded from heartbeat and gossip - they are essentially written
    ///  of, and this state is just part of writing them out of the books.
    Down = 6,
    /// This is a tombstone state: Once there is consensus that a node 'Removed', it can and should
    ///  be removed from internal tracking data structures: It ceases to exist for all intents and
    ///  purposes, and no messages (gossip, heartbeat or otherwise) should be sent to it. Its
    ///  process is likely terminated.
    Removed = 7,
}
impl MembershipState {

    pub fn is_gossip_partner(&self) -> bool {
        todo!()
    }

    pub fn is_leader_eligible(&self) -> bool {
        use MembershipState::*;

        match self {
            // NB: We allow a leader to be 'Exiting' to allow promotion to 'Removed' reliably during shutdown
            Joining | WeaklyUp | Up | Leaving | Exiting => true,
            Down | Removed => false,
        }
    }
}
impl Crdt for MembershipState {
    fn merge_from(&mut self, other: &MembershipState) -> CrdtOrdering {
        match Ord::cmp(self, other) {
            Ordering::Equal => CrdtOrdering::Equal,
            Ordering::Less => {
                *self = *other;
                CrdtOrdering::OtherWasBigger
            }
            Ordering::Greater => {
                CrdtOrdering::SelfWasBigger
            }
        }
    }
}
