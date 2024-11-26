use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;
use std::sync::Arc;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::{select, spawn, time};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, trace, warn};

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
    leader: Option<NodeAddr>,
}
impl ClusterState {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_event_queue: Arc<ClusterEventNotifier>) -> ClusterState {
        let nodes_with_state = BTreeMap::from_iter([(myself, NodeState {
            addr: myself,
            membership_state: MembershipState::Joining,
            roles: config.roles.clone(),
            reachability: Default::default(), //TODO is a node reachable from itself?
            seen_by: BTreeSet::from_iter([myself]),
        })]);

        ClusterState {
            myself,
            config,
            nodes_with_state,
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

    pub fn add_joiner(&mut self, addr: NodeAddr, roles: BTreeSet<String>) {
        debug!("adding joining node {:?}", addr);

        let node_state = NodeState {
            addr,
            membership_state: MembershipState::Joining,
            roles,
            reachability: Default::default(),
            seen_by: vec![self.myself].into_iter().collect(),
        };

        self.nodes_with_state.insert(addr, node_state);
    }

    pub async fn promote_myself_to_up(&mut self) {
        self.promote_myself(MembershipState::Up).await
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
            self.merge_node_state(state).await;
        }
    }

    pub async fn on_stable_unreachable_set(&mut self, downing_decision: DowningStrategyDecision) {
        let predicate = match downing_decision {
            DowningStrategyDecision::DownUs => |s: &&NodeState| s.is_reachable(),
            DowningStrategyDecision::DownThem => |s: &&NodeState| !s.is_reachable(),
        };
        let to_be_downed = self.node_states()
            .filter(predicate)
            .map(|s| s.addr)
            .collect::<Vec<_>>();

        for node in to_be_downed {
            self.promote_node(node, MembershipState::Down).await;
        }
        //TODO shut down this node if state is 'down' or 'Removed' + special handling if this is the last node
    }

    /// returns the node that is the leader in the current topology once state converges (which
    ///  can only happen if all nodes are reachable)
    async fn update_leader_candidate(&mut self) {
        //TODO does this require more sophisticated handling? Give preference to some states
        // over others? Or does the timestamp of joining take care of that well enough?

        let new_leader = Self::calc_leader_candidate(self.config.as_ref(), self.nodes_with_state.values())
            .map(|s| s.addr);

        if new_leader != self.leader {
            if let Some(l) = new_leader {
                info!("new cluster leader: {:?}", l);
            }
            self.send_event(ClusterEvent::LeaderChanged(LeaderChangedData {
                old_leader: self.leader,
                new_leader,
            }));
            self.leader = new_leader;
        }
    }

    pub fn calc_leader_candidate<'a>(config: &ClusterConfig, nodes: impl Iterator<Item = &'a NodeState>) -> Option<&'a NodeState> {
        nodes
            .filter(|s| s.membership_state.is_leader_eligible())
            .filter(|s| {
                if let Some(leader_eligible_roles) = &config.leader_eligible_roles {
                    leader_eligible_roles.iter()
                        .any(|role| s.roles.contains(role))
                }
                else {
                    true
                }
            })
            .min_by_key(|s| s.addr)
    }

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
    async fn state_changed(old_state: Option<MembershipState>, s: &mut NodeState, was_self_modified: bool, event_notifier: Arc<ClusterEventNotifier>) {
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

        self.update_leader_candidate().await;

        if self.am_i_leader() && self.is_converged() {
            let mut nodes_removed_from_gossip = Vec::new();

            for s in self.nodes_with_state.values_mut() {
                let old_state = s.membership_state;
                if let Some(new_state) = match old_state {
                    Joining | WeaklyUp => Some(Up),
                    Leaving => Some(Exiting),
                    Exiting | Down => Some(Removed),
                    _ => None
                } {
                    debug!("leader action: promoting {:?} to {:?}", s.addr, new_state);
                    s.membership_state = new_state;
                    s.seen_by.clear();
                    s.seen_by.insert(self.myself);
                    Self::state_changed(Some(old_state), s, true, self.event_notifier.clone()).await;
                    if old_state.is_gossip_partner() && !new_state.is_gossip_partner() {
                        nodes_removed_from_gossip.push(s.addr);
                    }
                }
            }

            for addr in nodes_removed_from_gossip {
                self.on_node_removed_from_gossip(&addr);
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

    pub async fn merge_node_state(&mut self, mut state: NodeState) {
        let is_converged_before = self.is_converged();

        match self.nodes_with_state.entry(state.addr) {
            Entry::Occupied(mut e) => {
                trace!("merging external node state for {:?} into existing state", state.addr);
                let old_state = e.get().membership_state;
                let was_self_changed = e.get_mut().merge(&state);
                Self::state_changed(Some(old_state), e.get_mut(), was_self_changed, self.event_notifier.clone()).await;
                if old_state.is_gossip_partner() && !state.membership_state.is_gossip_partner() {
                    self.on_node_removed_from_gossip(&state.addr);
                }
            }
            Entry::Vacant(e) => {
                trace!("merging external node state for {:?}: registering previously unknown node locally", state.addr);
                state.seen_by = Default::default();

                Self::state_changed(None, &mut state, true, self.event_notifier.clone()).await;

                let new_state = state.membership_state;
                let addr = state.addr;
                e.insert(state);
                if !new_state.is_gossip_partner() {
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
        let mut lazy_version_counter = LazyCounterVersion::new(self);

        {
            let mut updated_nodes = Vec::new();
            let mut reachablility_changed_nodes = Vec::new();

            for s in self.nodes_with_state.values_mut()
                .filter(|s| !reachability.contains_key(&s.addr))
            {
                // mark nodes as 'reachable' that were previously tracked for heartbeat by this node
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
                        // it was not reachable previously, so this means the heartbeat changed
                        reachablility_changed_nodes.push(s.addr);
                    }
                }
            }

            for addr in updated_nodes {
                self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr }));
            }
            for addr in reachablility_changed_nodes {
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
                    node.seen_by.insert(self.myself);

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

    /// returns a flag whether 'self' was modified in any way
    #[must_use]
    pub fn merge(&mut self, other: &NodeState) -> bool {
        assert_eq!(self.addr, other.addr);

        let mut result_ordering = CrdtOrdering::Equal;

        let membership_state_result = self.membership_state.merge_from(&other.membership_state);
        result_ordering = result_ordering.merge(membership_state_result);

        let roles_result = self.roles.merge_from(&other.roles);
        if roles_result != CrdtOrdering::Equal {
            warn!("different roles when merging gossip state for node {:?} - merged and proceeding, but this is a bug", self.addr);
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
                self.seen_by.insert(self.addr);
            }
            CrdtOrdering::NeitherWasBigger => {
                self.seen_by.clear();
                self.seen_by.insert(self.addr);
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

    pub fn is_leader_eligible(&self) -> bool {
        use MembershipState::*;

        match self {
            // we require a leader to be at least 'Up' to simplify logic for initial discovery: All
            //  nodes become 'Joining' right away on startup, and depending on the discovery strategy
            //  may or may not promote themselves to 'Up'. That logic becomes more involved if e.g.
            //  single nodes becomes full-blown clusters on their own, promoting themselves to 'Up'.
            Joining | WeaklyUp => false,
            // NB: We allow a leader to be 'Exiting' to allow promotion to 'Removed' reliably during shutdown
            Up | Leaving | Exiting => true,
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
mod test {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    use rstest::rstest;

    use MembershipState::*;
    use CrdtOrdering::*;

    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::messaging::node_addr::NodeAddr;
    use crate::node_state;

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
        assert_eq!(cluster_state.leader, None);
        assert_eq!(cluster_state.get_leader(), None);
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
        todo!()
    }

    #[test]
    fn test_promote_myself_to_up() {
        todo!()
    }

    #[test]
    fn test_promote_myself_to_weakly_up() {
        todo!()
    }

    #[test]
    fn test_promote_node() {
        todo!()
    }

    #[test]
    fn test_on_stable_unreachable_set() {
        todo!()
    }

    #[test]
    fn test_promote_calc_leader_candidate() {
        todo!()
    }

    #[test]
    fn test_promote_update_leader_candidate() {
        todo!()
    }

    #[test]
    fn test_get_leader() {
        todo!()
    }

    #[test]
    fn test_am_i_leader() {
        todo!()
    }

    #[test]
    fn test_is_converged() {
        todo!()
    }

    #[test]
    fn test_state_changed() {
        todo!()
    }

    #[test]
    fn test_do_leade_actions() {
        todo!()
    }

    #[test]
    fn test_merge_node_state() {
        todo!()
    }

    #[test]
    fn test_update_current_reachability() {
        todo!()
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
    #[case::equal(node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[1,2]), false)]
    #[case::equal_merge_seen_by(node_state!(1["a"]:Up->[2:true@7]@[1,2]), node_state!(1["a"]:Up->[2:true@7]@[3]), node_state!(1["a"]:Up->[2:true@7]@[1,2,3]), false)]
    #[case::joining_up(node_state!(1[]:Joining->[]@[1,2]), node_state!(1[]:Up->[]@[3]), node_state!(1[]:Up->[]@[1,3]), true)]
    #[case::up_joining(node_state!(1[]:Up->[]@[1,2]), node_state!(1[]:Joining->[]@[3]), node_state!(1[]:Up->[]@[1,2]), false)]
    #[case::roles(node_state!(1["a"]:Up->[]@[1,2]), node_state!(1["b"]:Up->[]@[3]), node_state!(1["a","b"]:Up->[]@[1]), true)]
    #[case::reachability_added_node(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:true@5,3:false@6]@[3]), node_state!(1[]:Up->[2:true@5,3:false@6]@[1,3]), true)]
    #[case::reachability_missing_node(node_state!(1[]:Up->[2:true@5,3:false@9]@[1,2]), node_state!(1[]:Up->[2:true@5]@[3]), node_state!(1[]:Up->[2:true@5,3:false@9]@[1,2]), false)]
    #[case::reachability_higher_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@6]@[3]), node_state!(1[]:Up->[2:false@6]@[1,3]), true)]
    #[case::reachability_lower_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@4]@[3]), node_state!(1[]:Up->[2:true@5]@[1,2]), false)]
    #[case::reachability_same_version(node_state!(1[]:Up->[2:true@5]@[1,2]), node_state!(1[]:Up->[2:false@5]@[3]), node_state!(1[]:Up->[2:false@5]@[1]), true)]
    #[case::reachability_same_version2(node_state!(1[]:Up->[2:false@5]@[1,2]), node_state!(1[]:Up->[2:true@5]@[3]), node_state!(1[]:Up->[2:false@5]@[1]), true)]
    #[case::reachability_higher_lower_version(node_state!(1[]:Up->[2:false@5,3:false@5]@[1,2]), node_state!(1[]:Up->[2:true@4,3:true@6]@[3]), node_state!(1[]:Up->[2:false@5,3:true@6]@[1]), true)]
    #[case::reachability_added_missing(node_state!(1[]:Up->[2:false@5]@[1,2]), node_state!(1[]:Up->[3:true@6]@[3]), node_state!(1[]:Up->[2:false@5,3:true@6]@[1]), true)]
    #[case::state_and_reachability(node_state!(1[]:Up->[]@[1,2]), node_state!(1[]:Joining->[3:true@6]@[3]), node_state!(1[]:Up->[3:true@6]@[1]), true)]
    #[case::state_and_roles(node_state!(1[]:Up->[]@[1,2]), node_state!(1["a"]:Joining->[]@[3]), node_state!(1["a"]:Up->[]@[1]), true)]
    #[case::roles_and_reachability(node_state!(1["a"]:Up->[]@[1,2]), node_state!(1[]:Up->[3:true@6]@[3]), node_state!(1["a"]:Up->[3:true@6]@[1]), true)]
    fn test_node_state_merge(#[case] mut first: NodeState, #[case] second: NodeState, #[case] expected_merged: NodeState, #[case] expected_was_self_changed: bool) {
        let ordering = first.merge(&second);
        assert_eq!(ordering, expected_was_self_changed);
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
    #[case::up(Up, true)]
    #[case::leaving(Leaving, true)]
    #[case::exiting(Exiting, true)]
    #[case::removed(Removed, false)]
    #[case::down(Down, false)]
    fn test_membership_state_is_leader_eligible(#[case] membership_state: MembershipState, #[case] expected: bool) {
        assert_eq!(membership_state.is_leader_eligible(), expected);
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
