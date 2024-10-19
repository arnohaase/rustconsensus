use std::collections::hash_map::Entry;
use std::sync::Arc;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;
use tracing::{error, warn};
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{ClusterEvent, NodeAddedData, NodeStateChangedData, NodeUpdatedData, ReachabilityChangedData};
use crate::messaging::node_addr::NodeAddr;


pub struct ClusterState { //todo  move to ../cluster_state?
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    nodes_with_state: FxHashMap<NodeAddr, NodeState>,
    cluster_event_queue: mpsc::Sender<ClusterEvent>,
    version_counter: u32,
}
impl ClusterState {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_event_queue: mpsc::Sender<ClusterEvent>) -> ClusterState {
        ClusterState {
            myself,
            config,
            nodes_with_state: Default::default(),
            cluster_event_queue,
            version_counter: 0,
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

    /// returns the node that is the leader in the current topology once state converges (which
    ///  can only happen if all nodes are reachable)
    pub fn get_leader_candidate(&self) -> Option<NodeAddr> {
        self.nodes_with_state.values()
            .filter(|s| s.membership_state.is_leader_eligible())
            .map(|s| s.addr)
            .min()
    }

    pub fn is_converged(&self) -> bool {
        let num_convergence_nodes = self.nodes_with_state.values()
            .filter(|s| s.membership_state.is_gossip_partner())
            .count();

        self.nodes_with_state.values()
            .all(|s| s.seen_by.len() == num_convergence_nodes)
    }

    //TODO API for accessing state

    pub async fn merge_node_state(&mut self, state: NodeState) {
        //TODO events, notifications
        let addr = state.addr;

        match self.nodes_with_state.entry(state.addr) {
            Entry::Occupied(mut e) => {
                let old_state = e.get().membership_state;
                let new_state = state.membership_state;
                let was_changed = e.get_mut().merge(state);

                if was_changed {
                    self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr })).await;
                    self.send_event(ClusterEvent::NodeStateChanged(NodeStateChangedData {
                        addr,
                        old_state,
                        new_state,
                    })).await;
                }
            }
            Entry::Vacant(e) => {
                e.insert(state.clone());

                self.send_event(ClusterEvent::NodeUpdated(NodeUpdatedData { addr })).await;
                self.send_event(ClusterEvent::NodeAdded(NodeAddedData {
                    addr,
                    state: state.membership_state,
                })).await;
            }
        }
    }

    async fn send_event(&self, event: ClusterEvent) {
        if let Err(e) = self.cluster_event_queue.send(event).await {
            error!("error sending cluster event (queue overflow?): {}", e);
        }
    }

    fn next_counter(&mut self) -> u32 {
        self.version_counter += 1;
        self.version_counter
    }

    //TODO unit test
    pub async fn update_current_reachability(&mut self, reachability: &FxHashMap<NodeAddr, bool>) {
        //TODO handle added / removed reachability keys since last call


        let has_change = reachability.iter()
            .any(|(addr, &reachable)| match self.nodes_with_state.get(addr) {
                None => true,
                Some(s) => {
                    match s.reachability.get(&self.myself) {
                        None => true,
                        Some(r) => r.is_reachable != reachable,
                    }
                }
            });

        if has_change {
            let new_counter_version = self.next_counter();

            for (&addr, &reachable) in reachability {
                if let Some(node) = self.nodes_with_state.get_mut(&addr) {
                    let old_is_reachable = node.is_reachable();

                    let was_updated = match node.reachability.entry(self.myself) {
                        Entry::Occupied(mut e) => {
                            if e.get().is_reachable != reachable {
                                e.get_mut().is_reachable = reachable;
                                e.get_mut().counter_of_reporter = new_counter_version;
                                true
                            }
                            else {
                                false
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(NodeReachability {
                                counter_of_reporter: new_counter_version,
                                is_reachable: reachable,
                            });
                            true
                        }
                    };
                    let new_is_reachable = node.is_reachable();

                    if was_updated {
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
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeState {
    pub addr: NodeAddr,
    pub membership_state: MembershipState,
    //TODO roles
    pub reachability: FxHashMap<NodeAddr, NodeReachability>, //TODO when to clean up 'is_reachable == true' entries
    pub seen_by: FxHashSet<NodeAddr>,
}
impl NodeState {
    //TODO unit test
    pub fn is_reachable(&self) -> bool {
        self.reachability.values()
            .all(|r| r.is_reachable)
    }

    /// returns true iff self wos modified
    pub fn merge(&mut self, other: NodeState) -> bool {
        todo!()
    }
}

#[derive(Clone, Debug)]
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
        todo!()
    }
}
