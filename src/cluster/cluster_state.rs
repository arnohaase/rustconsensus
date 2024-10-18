use std::sync::Arc;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use rustc_hash::{FxHashMap, FxHashSet};
use crate::cluster::cluster_config::ClusterConfig;
use crate::messaging::node_addr::NodeAddr;


pub struct ClusterState { //todo  move to ../cluster_state?
    myself: NodeAddr,
    config: Arc<ClusterConfig>,
    nodes_with_state: FxHashMap<NodeAddr, NodeState>,
    // unreachable_set: UnreachableSet, //TODO ???
}
impl ClusterState {
    pub fn myself(&self) -> NodeAddr {
        self.myself
    }

    pub fn node_states(&self) -> impl Iterator<Item=&NodeState> {
        self.nodes_with_state.values()
    }

    pub fn get_node_state(&self, addr: &NodeAddr) -> Option<&NodeState> {
        self.nodes_with_state.get(addr)
    }

    pub fn merge_node_state(&mut self, state: &NodeState) {
        todo!()
    }

    pub fn update_current_reachability(&mut self, reachability: &FxHashMap<NodeAddr, bool>) {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct NodeState {
    pub addr: NodeAddr,
    pub membership_state: MembershipState,
    pub reachability: FxHashMap<NodeAddr, NodeReachability>, //TODO when to clean up 'is_reachable == true' entries
    pub seen_by: FxHashSet<NodeAddr>,
}
impl NodeState {
    pub fn is_reachable(&self) -> bool {
        //TODO unit test
        self.reachability.values()
            .all(|r| r.is_reachable)
    }
}

#[derive(Clone, Debug)]
pub struct NodeReachability {
    /// a node reporting a change in reachability for a node attaches a strictly monotonous
    ///  counter so that reachability can be merged in a coordination-free fashion
    counter_of_reporter: u64,
    /// only `reachable=false` is really of interest, reachability being the default. But storing
    ///  reachability is necessary to spread that information by gossip.
    is_reachable: bool,
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
}
