use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::sync::{broadcast, RwLock};
use tokio::{select, spawn, time};
use tracing::{debug, info, instrument, trace, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{ClusterEvent, ClusterEventNotifier, LeaderChangedData, NodeAddedData, NodeStateChangedData, NodeUpdatedData, ReachabilityChangedData};
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision;
use crate::messaging::node_addr::NodeAddr;
use crate::util::crdt::{Crdt, CrdtOrdering};

pub async fn run_administrative_tasks_loop(config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, mut events: broadcast::Receiver<ClusterEvent>) {
    let mut leader_action_ticks = time::interval(config.leader_action_interval);

    //TODO documentation
    if let Some(weakly_up_after) = config.weakly_up_after {
        let cluster_state = cluster_state.clone();
        spawn(async move {
            time::sleep(weakly_up_after).await;
            cluster_state.write().await
                .promote_myself_to_weakly_up().await;
        });
    }

    loop {
        select! {
            _ = leader_action_ticks.tick() => {
                debug!("running periodic leader actions");
                cluster_state.write().await
                    .do_leader_actions().await
            }
            evt = events.recv() => {
                if let Ok(ClusterEvent::NodeStateChanged(data)) = evt {
                    if data.new_state.is_terminal() {
                        info!("Shutting down this cluster node because it reached terminal state {:?}", data.new_state);
                        return;
                    }
                }
            }
        }
    }
}

pub struct ClusterState {
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    nodes_with_state: BTreeMap<NodeAddr, NodeState>,
    event_notifier: Arc<ClusterEventNotifier>,
    version_counter: u32,
    /// we track the 'leader' even if there is no convergence (e.g. if it is unreachable) for convenience
    ///  of applications built on top of the cluster. Leader actions of the cluster are performed only
    ///  when convergence is reached.
    cached_old_leader: Option<NodeAddr>,
}
impl ClusterState {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_event_queue: Arc<ClusterEventNotifier>) -> ClusterState {
        let nodes_with_state = BTreeMap::from_iter([(myself, NodeState {
            addr: myself,
            membership_state: MembershipState::Joining,
            roles: config.roles.clone(),
            reachability: Default::default(),
            seen_by: BTreeSet::from_iter([myself]),
        })]);

        ClusterState {
            myself,
            config,
            nodes_with_state,
            event_notifier: cluster_event_queue,
            version_counter: 0,
            cached_old_leader: None,
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

    pub fn add_joiner(&mut self, addr: NodeAddr, roles: BTreeSet<String>) {
        debug!("adding joining node {:?}", addr);

        let node_state = NodeState {
            addr,
            membership_state: MembershipState::Joining,
            roles,
            reachability: Default::default(),
            seen_by: vec![self.myself].into_iter().collect(),
        };

        if !self.nodes_with_state.contains_key(&addr) {
            self.nodes_with_state.insert(addr, node_state);
        }
    }

    pub async fn promote_myself_to_up(&mut self) {
        self.promote_myself(MembershipState::Up).await
    }

    pub async fn promote_myself_to_down(&mut self) {
        self.promote_myself(MembershipState::Down).await
    }

    async fn promote_myself_to_weakly_up(&mut self) {
        if let Some(node) = self.get_node_state(&self.myself) {
            if node.membership_state == MembershipState::Joining {
                info!("promoting myself to 'weakly up' after configured timeout of {}ms", self.config.weakly_up_after.unwrap().as_millis());
                self.promote_myself(MembershipState::WeaklyUp).await
            }
        }
    }

    async fn promote_myself(&mut self, new_state: MembershipState) {
        self.promote_node(self.myself, new_state).await
    }

    async fn promote_node(&mut self, addr: NodeAddr, new_state: MembershipState) {
        if let Some(state) = self.get_node_state(&addr) {
            let mut state = state.clone();
            state.membership_state = new_state;
            if self.is_myself_still_gossipping() {
                state.seen_by = [self.myself].into();
            }
            self.merge_node_state(state).await;
        }
    }

    /// apply the downing decision to the cluster state's internal data structures, returning the
    ///  downed nodes so a downing message can be sent to them as a best effort heuristic in the
    ///  face of network unreliability
    pub async fn apply_downing_decision(&mut self, downing_decision: DowningStrategyDecision) -> Vec<NodeAddr> {
        let predicate = match downing_decision {
            DowningStrategyDecision::DownUs => |s: &&NodeState| s.is_reachable(),
            DowningStrategyDecision::DownThem => |s: &&NodeState| !s.is_reachable(),
        };
        let to_be_downed = self.node_states()
            .filter(predicate)
            .map(|s| s.addr)
            .collect::<Vec<_>>();

        for &node in &to_be_downed {
            self.promote_node(node, MembershipState::Down).await;
        }

        to_be_downed
        //TODO shut down this node if state is 'down' or 'Removed' + special handling if this is the last node
    }

    /// extracted as API for use by [super::heartbeat::downing_strategy::DowningStrategy] implementations
    pub fn calc_leader_candidate<'a>(config: &ClusterConfig, nodes: impl Iterator<Item = &'a NodeState>) -> Option<&'a NodeState> {
        nodes
            .filter(|s| s.is_leader_eligible(config))
            .min_by_key(|s| s.addr)
    }

    /// returns the current leader, or None if there is no leader at the moment (e.g. because of
    ///  unreachable nodes that prevent convergence)
    pub fn get_leader(&mut self) -> Option<NodeAddr> {
        if !self.is_converged() {
            return None;
        }

        if let Some(new_leader_state) = Self::calc_leader_candidate(self.config.as_ref(), self.node_states()) {
            let new_leader = new_leader_state.addr;
            if self.cached_old_leader != Some(new_leader) {
                info!("new cluster leader: {:?}", new_leader);
                self.send_event(ClusterEvent::LeaderChanged(LeaderChangedData { new_leader }));
                self.cached_old_leader = Some(new_leader);
            }
            Some(new_leader)
        }
        else {
            None
        }
    }

    pub fn am_i_leader(&mut self) -> bool {
        self.get_leader() == Some(self.myself)
    }


    fn num_convergence_nodes(&self) -> usize {
        self.nodes_with_state.values()
            .filter(|s| s.membership_state.is_gossip_partner())
            .count()
    }

    pub fn is_node_converged(&self, addr: NodeAddr) -> bool {
        if let Some(node) = self.get_node_state(&addr) {
            node.seen_by.len() == self.num_convergence_nodes()
        }
        else {
            false
        }
    }

    pub fn is_converged(&self) -> bool {
        let num_convergence_nodes = self.num_convergence_nodes();
        self.nodes_with_state.values()
            .all(|s| s.seen_by.len() == num_convergence_nodes)
    }

    /// This is the internal handler for all changes to a given node state. It updates the 'seen by'
    ///  set and sends change events.
    async fn send_state_changed_events(old_state: Option<MembershipState>, s: &mut NodeState, was_self_modified: bool, event_notifier: Arc<ClusterEventNotifier>) {
        if !was_self_modified {
            return;
        }

        event_notifier.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr: s.addr }));
        if let Some(old_state) = old_state {
            if old_state != s.membership_state {
                event_notifier.send_event(ClusterEvent::NodeStateChanged(NodeStateChangedData {
                    addr: s.addr,
                    old_state,
                    new_state: s.membership_state,
                }));
            }
        }
        else {
            event_notifier.send_event(ClusterEvent::NodeAdded(NodeAddedData {
                addr: s.addr,
                state: s.membership_state
            }));
        }
    }

    /// This function is meant to be called at regular intervals on all nodes - it checks who is
    ///  currently the leader, ensures convergence and then performs leader actions if this
    ///  is actually the leader node
    async fn do_leader_actions(&mut self) {
        use MembershipState::*;

        if self.am_i_leader() {
            let mut to_be_promoted= Vec::new();
            for s in self.nodes_with_state.values_mut() {
                let old_state = s.membership_state;
                if let Some(new_state) = match old_state {
                    Joining | WeaklyUp => Some(Up),
                    Leaving => Some(Exiting), //TODO grace period
                    Exiting | Down => Some(Removed),
                    _ => None
                } {
                    to_be_promoted.push((s.addr, new_state));
                }
            }

            for (addr, new_state) in to_be_promoted {
                debug!("leader action: promoting {:?} to {:?}", addr, new_state);
                self.promote_node(addr, new_state).await;
            }
        }
    }

    /// when a node is removed from gossip (i.e. it becomes 'Down' or 'Removed'), it needs to be
    ///  removed from all 'seen by' sets
    fn on_node_removed_from_gossip(&mut self, addr: &NodeAddr) {
        for s in self.nodes_with_state.values_mut() {
            s.seen_by.remove(addr);
        }
    }

    fn is_myself_still_gossipping(&self) -> bool {
        self.get_node_state(&self.myself).unwrap()
            .membership_state
            .is_gossip_partner()
    }

    pub async fn merge_node_state(&mut self, node_state: NodeState) {
        let is_converged_before = self.is_converged();
        let is_myself_still_gossipping = self.is_myself_still_gossipping();

        match self.nodes_with_state.entry(node_state.addr) {
            Entry::Occupied(mut e) => {
                trace!("merging external node state {:?} into existing state {:?}", node_state, e.get());
                let old_state = e.get().membership_state;
                let was_self_changed = e.get_mut().merge(&node_state);
                if is_myself_still_gossipping {
                    e.get_mut().seen_by.insert(self.myself);
                }
                Self::send_state_changed_events(Some(old_state), e.get_mut(), was_self_changed, self.event_notifier.clone()).await;
                if !node_state.membership_state.is_gossip_partner() {
                    debug!("node {:?} is not a gossip partner any more, removing from all 'seen by' sets", node_state.addr);
                    self.on_node_removed_from_gossip(&node_state.addr);
                }
            }
            Entry::Vacant(e) => {
                trace!("merging external node state for {:?}: registering previously unknown node locally", node_state.addr);
                let mut node_state = node_state;
                if is_myself_still_gossipping {
                    node_state.seen_by.insert(self.myself);
                }

                Self::send_state_changed_events(None, &mut node_state, true, self.event_notifier.clone()).await;

                let new_membership_state = node_state.membership_state;
                let addr = node_state.addr;
                e.insert(node_state);
                if !new_membership_state.is_gossip_partner() {
                    self.on_node_removed_from_gossip(&addr);
                }
            }
        }

        if ! is_converged_before {
            if self.is_converged() {
                debug!("cluster state fully converged");
                //TODO send an event for full convergence?
            }
        }
    }

    fn send_event(&self, event: ClusterEvent) {
        self.event_notifier.send_event(event);
    }

    pub async fn update_current_reachability(&mut self, reachability: &BTreeMap<NodeAddr, bool>) {
        trace!("updating reachability from myself to {:?}", reachability);
        let mut lazy_version_counter = LazyCounterVersion::new(self);
        let is_myself_still_gossipping = self.is_myself_still_gossipping();

        {
            let mut updated_nodes = Vec::new();
            let mut reachablility_changed_nodes = Vec::new();

            for s in self.nodes_with_state.values_mut()
                .filter(|s| !reachability.contains_key(&s.addr))
            {
                let was_reachable = s.is_reachable();

                // mark nodes as 'reachable' that were previously tracked for heartbeat by this node
                //  but are not tracked anymore (due to a node joining or unreachable nodes in between
                //  becoming reachable)
                if let Some(r) = s.reachability.get_mut(&self.myself) {
                    trace!("node now has no reachability from myself: {:?}", s.addr);
                    if !r.is_reachable {
                        r.is_reachable = true;
                        r.counter_of_reporter = lazy_version_counter.get_version();

                        //TODO refactor - extract shared code with the loop below

                        s.seen_by.clear();
                        if is_myself_still_gossipping {
                            s.seen_by.insert(self.myself);
                        }

                        updated_nodes.push(s.addr);
                    }

                    if was_reachable != s.is_reachable() {
                        // it was not reachable previously, so this means the heartbeat changed
                        reachablility_changed_nodes.push(s.addr);
                    }
                }
            }

            for addr in updated_nodes {
                self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr }));
            }
            for addr in reachablility_changed_nodes {
                // removing reachability information can only change reachability from false to true
                self.send_event(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                    addr,
                    old_is_reachable: false,
                    new_is_reachable: true,
                }));
            }
        }

        for (&addr, &reachable) in reachability {
            if let Some(node) = self.nodes_with_state.get_mut(&addr) {
                let old_is_reachable = node.is_reachable();

                trace!("pre-existing reachability {} for node {:?} (from myself)", old_is_reachable, addr);

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
                    // we changed the node's heartbeat information as seen from self, so there
                    //  is a new version of the node's data that was not seen by any other nodes
                    //  yet and that must be spread by gossip
                    node.seen_by.clear();
                    if is_myself_still_gossipping {
                        node.seen_by.insert(self.myself);
                    }

                    self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr }));
                }

                if old_is_reachable != new_is_reachable {
                    self.send_event(ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
                        addr,
                        old_is_reachable,
                        new_is_reachable,
                    }));
                }
            }
            else {
                warn!("heartbeat data for node {:?} which is not part of the cluster's known state - ignoring", addr);
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


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeState {
    pub addr: NodeAddr,
    pub membership_state: MembershipState,
    pub roles: BTreeSet<String>,
    pub reachability: BTreeMap<NodeAddr, NodeReachability>, //TODO clean up 'is_reachable == true' entries
    pub seen_by: BTreeSet<NodeAddr>,
}
impl NodeState {
    pub fn is_reachable(&self) -> bool {
        self.reachability.values()
            .all(|r| r.is_reachable)
    }

    #[instrument(level = "trace", skip(config)) ]
    pub fn is_leader_eligible(&self, config: &ClusterConfig) -> bool {
        use MembershipState::*;
        let is_membership_eligible = match self.membership_state {
            // we require a leader to be at least 'Up' to simplify logic for initial discovery: All
            //  nodes become 'Joining' right away on startup, and depending on the discovery strategy
            //  may or may not promote themselves to 'Up'. That logic becomes more involved if e.g.
            //  single nodes becomes full-blown clusters on their own, promoting themselves to 'Up'.
            Joining | WeaklyUp => false,
            // NB: We allow a leader to be 'Exiting' to allow promotion to 'Removed' reliably during shutdown
            Up | Leaving | Exiting => true,
            Down | Removed => false,
        };
        if !is_membership_eligible {
            trace!("state is not membership eligible -> false");
            return false;
        }

        if let Some(leader_eligible_roles) = &config.leader_eligible_roles {
            let result = leader_eligible_roles.iter()
                .any(|role| self.roles.contains(role));
            trace!("checking role eligibility: {}", result);
            result
        }
        else {
            trace!("state is membership eligible -> true");
            true
        }
    }

    /// returns a flag whether 'self' was modified in any way
    #[must_use]
    pub fn merge(&mut self, other: &NodeState) -> bool {
        assert_eq!(self.addr, other.addr);

        let mut result_ordering = CrdtOrdering::Equal;

        let membership_state_result = self.membership_state.merge_from(&other.membership_state);
        result_ordering = result_ordering.merge(membership_state_result);

        let roles_result = self.roles.merge_from(&other.roles);
        if roles_result != CrdtOrdering::Equal {
            warn!("different roles when merging gossip state for node {:?} - merged and proceeding, but this is a bug somewhere in the distributed system", self.addr);
        }
        result_ordering = result_ordering.merge(roles_result);

        for (addr, r_self) in self.reachability.iter_mut() {
            if let Some(r_other) = other.reachability.get(addr) {
                match Ord::cmp(&r_self.counter_of_reporter, &r_other.counter_of_reporter) {
                    Ordering::Less => {
                        r_self.counter_of_reporter = r_other.counter_of_reporter;
                        r_self.is_reachable = r_other.is_reachable;
                        result_ordering = result_ordering.merge(CrdtOrdering::OtherWasBigger);
                    }
                    Ordering::Equal => {
                        if r_self.is_reachable != r_other.is_reachable {
                            warn!("gossip inconsistency for heartbeat of node {:?} as seen from {:?}@{}: ", self.addr, addr, r_self.counter_of_reporter);
                            r_self.is_reachable = false; // to facilitate eventual self-healing
                            result_ordering = CrdtOrdering::NeitherWasBigger;
                        }
                    }
                    Ordering::Greater => {
                        result_ordering = result_ordering.merge(CrdtOrdering::SelfWasBigger);
                    }
                }
            }
            else {
                result_ordering = result_ordering.merge(CrdtOrdering::SelfWasBigger);
            }
        }

        let reporters_only_in_other = other.reachability.iter()
            .filter(|(a, _)| !self.reachability.contains_key(a))
            .map(|(a, r)| (*a, r.clone()))
            .collect::<Vec<_>>();

        for (addr, r) in reporters_only_in_other {
            self.reachability.insert(addr, r);
            result_ordering = result_ordering.merge(CrdtOrdering::OtherWasBigger);
        }

        match result_ordering {
            CrdtOrdering::SelfWasBigger => {
            }
            CrdtOrdering::Equal => {
                for &s in &other.seen_by {
                    self.seen_by.insert(s);
                }
            }
            CrdtOrdering::OtherWasBigger => {
                self.seen_by = other.seen_by.clone();
            }
            CrdtOrdering::NeitherWasBigger => {
                self.seen_by.clear();
            }
        }

        result_ordering.was_self_modified()
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct NodeReachability {
    /// a node reporting a change in heartbeat for a node attaches a strictly monotonous
    ///  counter so that heartbeat can be merged in a coordination-free fashion
    pub counter_of_reporter: u32,
    /// only `reachable=false` is really of interest, heartbeat being the default. But storing
    ///  heartbeat is necessary to spread that information by gossip.
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
    ///  that heartbeat (or lack thereof) is orthogonal to states, so a node can be 'up' but
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
        use MembershipState::*;

        match self {
            Joining | WeaklyUp | Up | Leaving | Exiting => true,
            Down | Removed => false,
        }
    }

    pub fn is_terminal(&self) -> bool {
        use MembershipState::*;

        match self {
            Down | Removed => true,
            Joining | WeaklyUp | Up | Leaving | Exiting => false,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    use rstest::rstest;
    use rustc_hash::FxHashSet;
    use tokio::runtime::Runtime;

    use DowningStrategyDecision::*;
    use MembershipState::*;

    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::*;
    use crate::messaging::node_addr::NodeAddr;
    use crate::node_state;
    use crate::test_util::event::*;
    use crate::test_util::node::*;

    use super::*;

    #[test]
    fn test_new() {
        let self_addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let myself = NodeAddr::from(self_addr);
        let config = Arc::new(ClusterConfig::new(self_addr));
        let cluster_event_queue = Arc::new(ClusterEventNotifier::new());

        let cluster_state = ClusterState::new(
            myself,
            config,
            cluster_event_queue,
        );

        let node_state = NodeState {
            addr: myself,
            membership_state: MembershipState::Joining,
            roles: BTreeSet::default(),
            reachability: BTreeMap::default(),
            seen_by: BTreeSet::from([myself]),
        };


        assert_eq!(cluster_state.myself(), myself);
        assert_eq!(cluster_state.version_counter, 0);
        assert_eq!(cluster_state.cached_old_leader, None);
        assert_eq!(
            cluster_state.nodes_with_state,
            BTreeMap::from([(myself, node_state.clone())]),
        );
        assert_eq!(
            cluster_state.node_states()
                .collect::<Vec<_>>(),
            vec![&node_state],
        );
        assert_eq!(
            cluster_state.get_node_state(&myself),
            Some(&node_state),
        );
    }

    #[test]
    fn test_add_joiner() {
        let myself = test_node_addr_from_number(1);
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));

        let joiner_addr = test_node_addr_from_number(5);
        cluster_state.add_joiner(joiner_addr, ["a".to_string()].into());

        assert_eq!(cluster_state.get_node_state(&joiner_addr), Some(&NodeState {
            addr: joiner_addr,
            membership_state: Joining,
            roles: ["a".to_string()].into(),
            reachability: Default::default(),
            seen_by: [myself].into(),
        }));
    }

    #[test]
    fn test_add_joiner_pre_existing() {
        let myself = test_node_addr_from_number(1);
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));

        let joiner_addr = test_node_addr_from_number(5);

        let pre_existing_state = NodeState {
            addr: joiner_addr,
            membership_state: Up,
            roles: ["b".to_string()].into(),
            reachability: Default::default(),
            seen_by: [myself, test_node_addr_from_number(2)].into(),
        };

        cluster_state.nodes_with_state.insert(joiner_addr, pre_existing_state.clone());

        cluster_state.add_joiner(joiner_addr, ["a".to_string()].into());

        assert_eq!(cluster_state.get_node_state(&joiner_addr), Some(&pre_existing_state));
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::weakly_up(vec![node_state!(1[]:WeaklyUp->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, WeaklyUp, Up)])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![])]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1])], node_state!(1[]:Leaving->[]@[1]), vec![])]
    fn test_promote_myself_to_up(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            for n in nodes {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            cluster_state.promote_myself_to_up().await;

            assert_eq!(cluster_state.get_node_state(&myself), Some(&expected));
            for expected in events {
                let actual = event_subscriber.try_recv().unwrap();
                assert_eq!(actual, expected);
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:Down->[]@[]), vec![test_updated_evt(1), test_state_evt(1, Joining, Down)])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Down->[]@[]), vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::down(vec![node_state!(1[]:Down->[]@[])], node_state!(1[]:Down->[]@[]), vec![])]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[])], node_state!(1[]:Removed->[]@[]), vec![])]
    fn test_promote_myself_to_down(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            for n in nodes {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            cluster_state.promote_myself_to_down().await;

            assert_eq!(cluster_state.get_node_state(&myself), Some(&expected));
            for expected in events {
                let actual = event_subscriber.try_recv().unwrap();
                assert_eq!(actual, expected);
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], node_state!(1[]:WeaklyUp->[]@[1]), vec![test_updated_evt(1), test_state_evt(1, Joining, WeaklyUp)])]
    #[case::weakly_up(vec![node_state!(1[]:WeaklyUp->[]@[1])], node_state!(1[]:WeaklyUp->[]@[1]), vec![])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], node_state!(1[]:Up->[]@[1]), vec![])]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1])], node_state!(1[]:Leaving->[]@[1]), vec![])]
    fn test_promote_myself_to_weakly_up(#[case] nodes: Vec<NodeState>, #[case] expected: NodeState, #[case] events: Vec<ClusterEvent>) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            for n in nodes {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            cluster_state.promote_myself_to_weakly_up().await;

            assert_eq!(cluster_state.get_node_state(&myself), Some(&expected));
            for expected in events {
                let actual = event_subscriber.try_recv().unwrap();
                assert_eq!(actual, expected);
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::joining_up_1(vec![node_state!(1[]:Joining->[]@[1])], 1, Up, Some(node_state!(1[]:Up->[]@[1])), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::joining_up_2(vec![node_state!(1[]:Joining->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Up, Some(node_state!(1[]:Up->[]@[1])), vec![test_updated_evt(1), test_state_evt(1, Joining, Up)])]
    #[case::smaller_state(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Joining, Some(node_state!(1[]:Up->[]@[1,2])), vec![])]
    #[case::same_state(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], 1, Up, Some(node_state!(1[]:Up->[]@[1,2])), vec![])]
    #[case::missing_node(vec![node_state!(1[]:Up->[]@[1,2])], 2, Up, None, vec![])]
    fn test_promote_node(#[case] nodes: Vec<NodeState>, #[case] addr: u16, #[case] to_state: MembershipState, #[case] new_state: Option<NodeState>, #[case] events: Vec<ClusterEvent>) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            for n in nodes {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            let addr = test_node_addr_from_number(addr);

            cluster_state.promote_node(addr, to_state).await;

            assert_eq!(cluster_state.get_node_state(&addr), new_state.as_ref());
            for expected in events {
                let actual = event_subscriber.try_recv().unwrap();
                assert_eq!(actual, expected);
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::single_reg_us(vec![node_state!(1[]:Up->[]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::single_reg_them(vec![node_state!(1[]:Up->[]@[1])], DownThem, vec![], vec![node_state!(1[]:Up->[]@[1])], vec![])]
    #[case::two_reg_us(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], DownUs, vec![1,2], vec![node_state!(1[]:Down->[]@[]), node_state!(2[]:Down->[]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down), test_updated_evt(2), test_state_evt(2, Up, Down)])]
    #[case::two_reg_them(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], DownThem, vec![], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![])]
    #[case::split_us(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[]@[]), node_state!(2[]:Up->[1:false@5]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::split_them(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownThem, vec![2], vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Down->[1:false@5]@[1])], vec![test_updated_evt(2), test_state_evt(2, Up, Down)])]
    #[case::explicit_us(vec![node_state!(1[]:Up->[2:true@6]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownUs, vec![1], vec![node_state!(1[]:Down->[2:true@6]@[]), node_state!(2[]:Up->[1:false@5]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down)])]
    #[case::explicit_them(vec![node_state!(1[]:Up->[2:true@6]@[1,2]), node_state!(2[]:Up->[1:false@5]@[1])], DownThem, vec![2], vec![node_state!(1[]:Up->[2:true@6]@[1]), node_state!(2[]:Down->[1:false@5]@[1])], vec![test_updated_evt(2), test_state_evt(2, Up, Down)])]
    // somewhat pathological corner case - more to check robustness than anything else
    #[case::two_unreachable_us(vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], DownUs, vec![], vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], vec![])]
    #[case::two_unreachable_them(vec![node_state!(1[]:Up->[2:false@3]@[1,2]), node_state!(2[]:Up->[1:false@4]@[1,2])], DownThem, vec![1,2], vec![node_state!(1[]:Down->[2:false@3]@[]), node_state!(2[]:Down->[1:false@4]@[])], vec![test_updated_evt(1), test_state_evt(1, Up, Down), test_updated_evt(2), test_state_evt(2, Up, Down)])]
    fn test_apply_downing_decision(
        #[case] initial_nodes: Vec<NodeState>,
        #[case] downing_strategy_decision: DowningStrategyDecision,
        #[case] expected_downed_nodes: Vec<u16>,
        #[case] expected_state: Vec<NodeState>,
        #[case] expected_events: Vec<ClusterEvent>,
    ) {
        assert_eq!(initial_nodes.len(), expected_state.len());

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let expected_downed_nodes = expected_downed_nodes.into_iter()
                .map(|n| test_node_addr_from_number(n))
                .collect::<Vec<_>>();

            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            for n in initial_nodes {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            let downed_nodes = cluster_state.apply_downing_decision(downing_strategy_decision).await;
            assert_eq!(downed_nodes, expected_downed_nodes);

            for exp in expected_state {
                assert_eq!(cluster_state.get_node_state(&exp.addr), Some(&exp));
            }

            for exp in expected_events {
                let act = event_subscriber.try_recv().unwrap();
                assert_eq!(act, exp);
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::simple_2(vec![node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::simple_3(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Up->[]@[1,3])], true)]
    #[case::simple_3_incomplete(vec![node_state!(2[]:Up->[]@[1,3]), node_state!(3[]:Up->[]@[1,3])], false)]
    #[case::joining  (vec![node_state!(2[]:Joining-> []@[1,2])], true)]
    #[case::weakly_up(vec![node_state!(2[]:WeaklyUp->[]@[1,2])], true)]
    #[case::leaving  (vec![node_state!(2[]:Leaving-> []@[1,2])], true)]
    #[case::exiting  (vec![node_state!(2[]:Exiting-> []@[1,2])], true)]
    #[case::down     (vec![node_state!(2[]:Down->    []@[1])], true)] //NB: 'down' node cannot see itself
    #[case::removed  (vec![node_state!(2[]:Removed-> []@[1])], true)] //NB: 'removed' node cannot see itself
    #[case::other_joining  (vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Joining-> []@[1])], true)]
    #[case::other_weakly_up(vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:WeaklyUp->[]@[1])], true)]
    #[case::other_up       (vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:WeaklyUp->[]@[1])], true)]
    #[case::other_leaving  (vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Leaving-> []@[1])], true)]
    #[case::other_exiting  (vec![node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:Exiting-> []@[1])], true)]
    #[case::other_down     (vec![node_state!(2[]:Up->[]@[1,2]),   node_state!(3[]:Down->    []@[1])], true)] //NB: 'down' node cannot see
    #[case::other_removed  (vec![node_state!(2[]:Up->[]@[1,2]),   node_state!(3[]:Removed-> []@[1])], true)] //NB: 'removed' node cannot see
    #[case::non_existing(vec![], false)] // should not happen, robustness corner case test
    fn test_is_node_converged(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let myself = test_node_addr_from_number(1);
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
        for n in nodes {
            cluster_state.nodes_with_state.insert(n.addr, n);
        }
        assert_eq!(cluster_state.is_node_converged(test_node_addr_from_number(2)), expected);
    }

    #[rstest]
    #[case::empty(vec![], true)]
    #[case::converged_1(vec![node_state!(1[]:Up->[]@[1])], true)]
    #[case::converged_2(vec![node_state!(1[]:Up->[]@[1,2]),node_state!(2[]:Up->[]@[1,2]),], true)]
    #[case::converged_3(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], true)]
    #[case::joining_counts(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::joining_negative_1(vec![node_state!(1[]:Joining->[]@[1,2]),node_state!(2[]:Up->[]@[2])], false)]
    #[case::joining_negative_2(vec![node_state!(1[]:Joining->[]@[2]),node_state!(2[]:Up->[]@[2])], false)]
    #[case::down(vec![node_state!(1[]:Down->[]@[2]),node_state!(2[]:Up->[]@[2])], true)]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::exiting(vec![node_state!(1[]:Exiting->[]@[1,2]),node_state!(2[]:Up->[]@[1,2])], true)]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[2]),node_state!(2[]:Up->[]@[2])], true)]
    //TODO how to handle 'seen by' by nodes that are not registered (yet)? Filter them out in ClusterState? Defer convergence? Can they become part of the cluster's lore without existing?
    fn test_is_converged(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let myself = test_node_addr_from_number(1);
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
        for n in nodes {
            cluster_state.nodes_with_state.insert(n.addr, n);
        }

        assert_eq!(cluster_state.is_converged(), expected);
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
        let leader_candidate = leader_candidate.map(|n| test_node_addr_from_number(n));

        let myself = test_node_addr_from_number(1);
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
        for n in nodes {
            cluster_state.nodes_with_state.insert(n.addr, n);
        }
        cluster_state.cached_old_leader = prev_leader.map(|n| test_node_addr_from_number(n));

        let mut event_subscriber = cluster_state.event_notifier.subscribe();

        assert_eq!(
            ClusterState::calc_leader_candidate(cluster_state.config.as_ref(), cluster_state.node_states())
                .map(|s| s.addr),
            leader_candidate,
        );
        assert_eq!(cluster_state.is_converged(), is_converged);
        assert_eq!(cluster_state.get_leader(), if is_converged { leader_candidate } else { None });

        if is_event_expected {
            assert_eq!(
                event_subscriber.try_recv().unwrap(),
                ClusterEvent::LeaderChanged(LeaderChangedData {
                    new_leader: leader_candidate.unwrap()
                })
            );
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
        let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
        for n in nodes {
            cluster_state.nodes_with_state.insert(n.addr, n);
        }
        assert_eq!(cluster_state.am_i_leader(), expected);
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
    #[case::exiting(vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Exiting->[]@[1,2])], Some(1), vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Removed->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Exiting, Removed)])]
    #[case::down(vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Down->[]@[1])], Some(1), vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Removed->[]@[1])], vec![test_updated_evt(2), test_state_evt(2, Down, Removed)])]
    #[case::not_leader(vec![node_state!(0[]:Up->[]@[0,1,2]), node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Joining->[]@[1,2,3])],
                       None,
                       vec![node_state!(0[]:Up->[]@[0,1,2]), node_state!(1[]:Up->[]@[1,2,3]), node_state!(2[]:Joining->[]@[1,2,3])],
                       vec![test_leader_evt(0)])]
    fn test_do_leader_actions(#[case] initial: Vec<NodeState>, #[case] prev_leader: Option<u16>, #[case] expected: Vec<NodeState>, #[case] expected_events: Vec<ClusterEvent>) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            cluster_state.nodes_with_state.clear();
            for n in initial {
                cluster_state.nodes_with_state.insert(n.addr, n);
            }
            cluster_state.cached_old_leader = prev_leader.map(|n| test_node_addr_from_number(n));
            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            cluster_state.do_leader_actions().await;

            for s in expected {
                assert_eq!(cluster_state.nodes_with_state.get(&s.addr), Some(&s));
            }

            for exp in expected_events {
                let act = event_subscriber.try_recv().unwrap();
                trace!("received event {:?}", act);
                assert_eq!(act, exp);
            }

            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case::add(None, node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Joining->[]@[1,3]), true, false, true)]
    #[case::same(Some(node_state!(2[]:Joining->[]@[1,3,4])), node_state!(2[]:Joining->[]@[1,3,4]), node_state!(2[]:Joining->[]@[1,3,4]), false, false, false)]
    #[case::same_merge_seen_by(Some(node_state!(2[]:Joining->[]@[1,4])), node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Joining->[]@[1,3,4]), false, false, false)]
    #[case::older(Some(node_state!(2[]:Up->[]@[1,4])), node_state!(2[]:Joining->[]@[3]), node_state!(2[]:Up->[]@[1,4]), false, false, false)]
    #[case::newer(Some(node_state!(2[]:Joining->[]@[1,4])), node_state!(2[]:Up->[]@[3]), node_state!(2[]:Up->[]@[1,3]), false, true, true)]
    fn test_merge_node_state(#[case] prev: Option<NodeState>, #[case] new_state: NodeState, #[case] expected: NodeState, #[case] is_add: bool, #[case] is_state_change: bool, #[case] is_update: bool) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));

            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            if let Some(prev) = prev.clone() {
                cluster_state.nodes_with_state.insert(prev.addr, prev);
            }

            cluster_state.merge_node_state(new_state).await;

            assert_eq!(cluster_state.get_node_state(&expected.addr), Some(&expected));

            if is_update {
                let evt = event_subscriber.try_recv().unwrap();
                assert_eq!(evt, ClusterEvent::NodeUpdated(NodeUpdatedData { addr: expected.addr }));
            }
            if is_state_change {
                let evt = event_subscriber.try_recv().unwrap();
                assert_eq!(evt, ClusterEvent::NodeStateChanged(NodeStateChangedData { addr: expected.addr, old_state: prev.unwrap().membership_state, new_state: expected.membership_state }));
            }
            if is_add {
                let evt = event_subscriber.try_recv().unwrap();
                assert_eq!(evt, ClusterEvent::NodeAdded(NodeAddedData { addr: expected.addr, state: expected.membership_state }));
            }
            assert!(event_subscriber.is_empty());
        });
    }

    #[rstest]
    #[case(Down, true)]
    #[case(Down, false)]
    #[case(Removed, true)]
    #[case(Removed, false)]
    fn test_merge_node_state_remove_from_gossip(#[case] terminal_state: MembershipState, #[case] present_before_merge: bool) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));

            // node 3 is in node 2's 'seen by' set
            cluster_state.merge_node_state(NodeState {
                addr: test_node_addr_from_number(2),
                membership_state: Up,
                roles: Default::default(),
                reachability: Default::default(),
                seen_by: [test_node_addr_from_number(1), test_node_addr_from_number(3)].into(),
            }).await;

            if present_before_merge {
                // initial data for node 3: up
                cluster_state.merge_node_state(NodeState {
                    addr: test_node_addr_from_number(3),
                    membership_state: Up,
                    roles: Default::default(),
                    reachability: Default::default(),
                    seen_by: [test_node_addr_from_number(1)].into(),
                }).await;
            }

            assert_eq!(
                cluster_state.nodes_with_state.get(&test_node_addr_from_number(2))
                    .unwrap()
                    .seen_by,
                BTreeSet::from([test_node_addr_from_number(1), test_node_addr_from_number(3)])
            );

            // promote node 3 to terminal state -> should remove it from node 2's 'seen by' set
            cluster_state.merge_node_state(NodeState {
                addr: test_node_addr_from_number(3),
                membership_state: terminal_state,
                roles: Default::default(),
                reachability: Default::default(),
                seen_by: [test_node_addr_from_number(1)].into(),
            }).await;

            assert_eq!(
                cluster_state.nodes_with_state.get(&test_node_addr_from_number(2))
                    .unwrap()
                    .seen_by,
                BTreeSet::from([test_node_addr_from_number(1)])
            );
        })
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
        #[case] old_reachability: Vec<(u16,Option<bool>)>,
        #[case] new_reachability: Vec<(u16,bool)>,
        #[case] expected_reachability: Vec<(u16,(u32,bool))>,
        #[case] expected_version_counter: u32,
        #[case] expected_events: Vec<ClusterEvent>,
    ) {
        assert_eq!(old_reachability.len(), expected_reachability.len());

        let new_reachability = new_reachability.into_iter()
            .map(|(k,v)| (test_node_addr_from_number(k), v))
            .collect::<BTreeMap<_,_>>();

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let mut cluster_state = ClusterState::new(myself, Arc::new(ClusterConfig::new(myself.socket_addr)), Arc::new(ClusterEventNotifier::new()));
            cluster_state.version_counter = 8;

            let mut event_subscriber = cluster_state.event_notifier.subscribe();

            // set up initial state: add nodes for 'old reachability'
            for (k,v) in old_reachability {
                let node_state = match (k,v) {
                    (2,Some(true)) => node_state!(2[]:Up->[1:true@5,99:true@111]@[1,99]),
                    (2,Some(false)) => node_state!(2[]:Up->[1:false@5,99:true@111]@[1,99]),
                    (2,None) => node_state!(2[]:Up->[99:true@111]@[1,99]),
                    (3,Some(true)) => node_state!(3[]:Up->[1:true@5,99:true@111]@[1,99]),
                    (3,Some(false)) => node_state!(3[]:Up->[1:false@5,99:true@111]@[1,99]),
                    (3,None) => node_state!(3[]:Up->[99:true@111]@[1,99]),
                    _ => panic!(),
                };
                cluster_state.nodes_with_state
                    .insert(node_state.addr, node_state);
            }
            // set up initial state: add a node unrelated to reachability
            let unrelated_node = node_state!(98[]:Up->[99:true@7]@[1,99]);
            cluster_state.nodes_with_state
                .insert(unrelated_node.addr, unrelated_node.clone());

            let initial_nodes: Vec<NodeAddr> = cluster_state.nodes_with_state.keys().cloned().collect();

            // execute code under test
            cluster_state.update_current_reachability(&new_reachability).await;

            for (addr, (counter_of_reporter, is_reachable)) in expected_reachability {
                let node = cluster_state.nodes_with_state.get(&test_node_addr_from_number(addr)).unwrap();
                let node_reachability = NodeReachability {
                    counter_of_reporter,
                    is_reachable,
                };
                assert_eq!(&node.reachability, &[
                    (myself, node_reachability),
                    (test_node_addr_from_number(99), NodeReachability { counter_of_reporter: 111, is_reachable: true }),
                ].into());
            }

            assert_eq!(cluster_state.nodes_with_state.keys().cloned().collect::<Vec<_>>(), initial_nodes);
            assert_eq!(cluster_state.version_counter, expected_version_counter);

            // verify that the unrelated node was left untouched
            assert_eq!(cluster_state.nodes_with_state.get(&unrelated_node.addr), Some(&unrelated_node));

            for exp in expected_events {
                let act = event_subscriber.try_recv().unwrap();
                trace!("received event {:?}", act);
                assert_eq!(act, exp);
            }

            assert!(event_subscriber.is_empty());
        });
    }

    #[test]
    fn test_lazy_counter_version() {
        let self_addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let myself = NodeAddr::from(self_addr);
        let config = Arc::new(ClusterConfig::new(self_addr));
        let cluster_event_queue = Arc::new(ClusterEventNotifier::new());

        let mut cluster_state = ClusterState::new(
            myself,
            config,
            cluster_event_queue,
        );
        cluster_state.version_counter = 24;

        let counter = LazyCounterVersion::new(&cluster_state);
        counter.finalize(&mut cluster_state);
        assert_eq!(24, cluster_state.version_counter);

        let mut counter = LazyCounterVersion::new(&cluster_state);
        assert_eq!(25, counter.get_version());
        assert_eq!(24, cluster_state.version_counter);
        counter.finalize(&mut cluster_state);
        assert_eq!(25, cluster_state.version_counter);
    }

    #[rstest]
    #[case::two_reachable     (node_state!(1['a']:Up->[2:true@7,  3:true@5 ]@[1,2,3]), true)]
    #[case::one_reachable     (node_state!(1['a']:Up->[2:true@7            ]@[1,2,3]), true)]
    #[case::one_unreachable   (node_state!(1['a']:Up->[2:false@7           ]@[1,2,3]), false)]
    #[case::first_unreachable (node_state!(1['a']:Up->[2:false@7, 3:true@5 ]@[1,2,3]), false)]
    #[case::second_unreachable(node_state!(1['a']:Up->[2:true@7,  3:false@5]@[1,2,3]), false)]
    #[case::both_unreachable  (node_state!(1['a']:Up->[2:false@7, 3:false@5]@[1,2,3]), false)]
    #[case::empty_reachability(node_state!(1['a']:Up->[                    ]@[1,2,3]), true)]
    fn test_node_state_is_reachable(#[case] node_state: NodeState, #[case] expected: bool) {
        assert_eq!(node_state.is_reachable(), expected);
    }

    #[rstest]
    #[case::joining(node_state!(1[]:Joining->[]@[1]), None, false)]
    #[case::weakly_up(node_state!(1[]:WeaklyUp->[]@[1]), None, false)]
    #[case::up(node_state!(1[]:Up->[]@[1]), None, true)]
    #[case::leaving(node_state!(1[]:Leaving->[]@[1]), None, true)]
    #[case::exiting(node_state!(1[]:Exiting->[]@[1]), None, true)]
    #[case::down(node_state!(1[]:Down->[]@[1]), None, false)]
    #[case::removed(node_state!(1[]:Removed->[]@[1]), None, false)]
    #[case::missing_role(node_state!(1[]:Up->[]@[1]), Some(vec!["l"]), false)]
    #[case::missing_role_2(node_state!(1[]:Up->[]@[1]), Some(vec!["l1", "l2"]), false)]
    #[case::wrong_role(node_state!(1["o"]:Up->[]@[1]), Some(vec!["l"]), false)]
    #[case::wrong_role_2(node_state!(1["o"]:Up->[]@[1]), Some(vec!["l1", "l2"]), false)]
    #[case::matching_role(node_state!(1["l"]:Up->[]@[1]), Some(vec!["l"]), true)]
    #[case::matching_role_2(node_state!(1["l1"]:Up->[]@[1]), Some(vec!["l1", "l2"]), true)]
    #[case::matching_role_and_other(node_state!(1["l", "o"]:Up->[]@[1]), Some(vec!["l"]), true)]
    #[case::matching_role_and_other_2(node_state!(1["l1", "o"]:Up->[]@[1]), Some(vec!["l1", "l2"]), true)]
    #[case::ignore_reachability(node_state!(1[]:Up->[2:false@5]@[1]), None, true)]
    fn test_node_state_is_leader_eligible(#[case] node_state: NodeState, #[case] required_roles: Option<Vec<&str>>, #[case] expected: bool) {
        let mut config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr);
        if let Some(required_roles) = required_roles {
            let leader_eligible_roles = required_roles.iter()
                .map(|r| r.to_string())
                .collect::<FxHashSet<_>>();
            config.leader_eligible_roles = Some(leader_eligible_roles);
        }

        assert_eq!(node_state.is_leader_eligible(&config), expected);
    }

    #[rstest]
    #[case::equal(node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[1,2]), false)]
    #[case::equal_merge_seen_by(node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[3]), node_state!(1["a"]:Up->[2:true@7]@[1,2,3]), false)]
    #[case::joining_up(node_state!(1[]:Joining->[]@[1,2]), node_state!(1[]:Up->[]@[3]), node_state!(1[]:Up->[]@[3]), true)]
    #[case::up_joining(node_state!(1[]:Up->[]@[1,2]), node_state!(1[]:Joining->[]@[3]), node_state!(1[]:Up->[]@[1,2]), false)]
    #[case::roles(node_state!(1["a"]:Up->[]@[1,2]), node_state!(1["b"]:Up->[]@[3]), node_state!(1["a","b"]:Up->[]@[]), true)]
    #[case::reachability_added_node(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:true@5,3:false@6]@[3]), node_state!(1[]:Up->[2:true@5,3:false@6]@[3]), true)]
    #[case::reachability_missing_node(node_state!(1[]:Up->[2:true@5,3:false@9]@[1,2]), node_state!(1[]:Up->[2:true@5]@[3]), node_state!(1[]:Up->[2:true@5,3:false@9]@[1,2]), false)]
    #[case::reachability_higher_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@6]@[3]), node_state!(1[]:Up->[2:false@6]@[3]), true)]
    #[case::reachability_lower_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@4]@[3]), node_state!(1[]:Up->[2:true@5]@[1,2]), false)]
    #[case::reachability_same_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@5]@[3]), node_state!(1[]:Up->[2:false@5]@[]), true)]
    #[case::reachability_same_version2(node_state!(1[]:Up->[2:false@5]@[1,2]), node_state!(1[]:Up->[2:true@5]@[3]), node_state!(1[]:Up->[2:false@5]@[]), true)]
    #[case::reachability_higher_lower_version(node_state!(1[]:Up->[2:false@5,3:false@5]@[1,2]), node_state!(1[]:Up->[2:true@4,3:true@6]@[3]), node_state!(1[]:Up->[2:false@5,3:true@6]@[]), true)]
    #[case::reachability_added_missing(node_state!(1[]:Up->[2:false@5]@[1,2]), node_state!(1[]:Up->[3:true@6]@[3]), node_state!(1[]:Up->[2:false@5,3:true@6]@[]), true)]
    #[case::state_and_reachability(node_state!(1[]:Up->[]@[1,2]), node_state!(1[]:Joining->[3:true@6]@[3]), node_state!(1[]:Up->[3:true@6]@[]), true)]
    #[case::state_and_roles(node_state!(1[]:Up->[]@[1,2]), node_state!(1["a"]:Joining->[]@[3]), node_state!(1["a"]:Up->[]@[]), true)]
    #[case::roles_and_reachability(node_state!(1["a"]:Up->[]@[1,2]), node_state!(1[]:Up->[3:true@6]@[3]), node_state!(1["a"]:Up->[3:true@6]@[]), true)]
    fn test_node_state_merge(#[case] mut first: NodeState, #[case] second: NodeState, #[case] expected_merged: NodeState, #[case] expected_was_self_changed: bool) {
        let was_changed = first.merge(&second);
        assert_eq!(was_changed, expected_was_self_changed);
        assert_eq!(first, expected_merged);
    }

    #[rstest]
    #[case::joining(Joining, true)]
    #[case::weakly_up(WeaklyUp, true)]
    #[case::up(Up, true)]
    #[case::leaving(Leaving, true)]
    #[case::exiting(Exiting, true)]
    #[case::removed(Removed, false)]
    #[case::down(Down, false)]
    fn test_membership_state_is_gossip_partner(#[case] membership_state: MembershipState, #[case] expected: bool) {
        assert_eq!(membership_state.is_gossip_partner(), expected);
    }

    #[rstest]
    #[case::joining(Joining, false)]
    #[case::weakly_up(WeaklyUp, false)]
    #[case::up(Up, false)]
    #[case::leaving(Leaving, false)]
    #[case::exiting(Exiting, false)]
    #[case::removed(Removed, true)]
    #[case::down(Down, true)]
    fn test_membership_state_is_terminal(#[case] membership_state: MembershipState, #[case] expected: bool) {
        assert_eq!(membership_state.is_terminal(), expected);
    }
}
