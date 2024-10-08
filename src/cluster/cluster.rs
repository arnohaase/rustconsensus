use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::Deref;

use rand::distributions::{Distribution, WeightedIndex};
use rand::thread_rng;
use rustc_hash::FxHashSet;

use crate::msg::node_addr::NodeAddr;
use crate::util::crdt::{Crdt, CrdtOrdering};

//TODO concurrent initial startup -> how to detect and merge disjoint clusters
//TODO garbage collection for 'removed' tombstones

//TODO active part - who drives this
//TODO notifications for all kinds of changes

//TODO handle reconnect by a node that is still marked as 'unreachable' or 'down'

//TODO heartbeat -> if one node is isolated, how does it know if only its partners are
//          unreachable, or the entire rest of the cluster?
//TODO should every node track the leader's heartbeat?

//TODO when the topology changes, 'unreachable' may not be the detecting node's responsibility any more

#[derive(Eq, PartialEq, Clone, Debug, Hash)]
struct OrderedNodeAddr(NodeAddr);
impl Deref for OrderedNodeAddr {
    type Target = NodeAddr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartialOrd for OrderedNodeAddr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedNodeAddr {
    //TODO unit test
    fn cmp(&self, other: &Self) -> Ordering {
        match self.0.unique.cmp(&other.0.unique) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.0.addr.cmp(&other.0.addr)
        }
    }
}


pub struct Cluster {
    members: BTreeMap<OrderedNodeAddr, NodeMembershipState>,
}
impl Cluster {
    /// Merge a single node's state into the overall membership state. The returned ordering
    ///  refers to this specific node's state.
    //TODO unit test
    pub fn merge_node(&mut self, other: &NodeMembershipState) -> CrdtOrdering {
        let result = match self.members.entry(OrderedNodeAddr(other.node_addr)) {
            Entry::Occupied(mut e) => {
                e.get_mut().merge_from(other)
            }
            Entry::Vacant(e) => {
                e.insert(other.clone());
                CrdtOrdering::OtherWasBigger
            }
        };

        if !other.state.is_active() {
            // Sanitize: remove the newly inactive node from all 'seen by' sets.
            // NB: This does not affect the returned ordering because someone sending a node
            //      as inactive should not have this inactive node in its 'seen by' sets
            //      anyway
            for s in self.members.values_mut() {
                let _ = s.seen_by.remove(&other.node_addr.addr);
            }
        }

        result
    }

    /// NB: There are typically more active than inactive nodes, so materializing the inactive nodes
    ///  is more efficient than materializing the active ones.
    //TODO unit test
    fn inactive_members(&self) -> FxHashSet<NodeAddr> {
        self.members.iter()
            .filter(|(_, s)| !s.state.is_active())
            .map(|(addr, _)| (**addr).clone())
            .collect()
    }

    fn num_convergence_nodes(&self) -> usize {
        self.members.len() - self.inactive_members().len()
    }

    /// Checks if there is gossip convergence from this node's perspective, i.e. this node's
    ///  perspective on the cluster has been confirmed by all other nodes.
    //TODO unit test
    pub fn is_converged(&self) -> bool {
        let num_convergence_nodes = self.num_convergence_nodes();

        //TODO comparing the sizes is simpler and more efficient than comparing sets - but is it robust?
        self.members.values()
            .all(|s| s.seen_by.len() == num_convergence_nodes)
    }

    /// A leader only becomes active based on gossip convergence, so leader resolution only has
    ///  to be well-defined based on gossip convergence.
    ///
    /// In that case, the oldest node in 'up' state is used, or failing that, the oldest active
    ///  node.
    //TODO graceful handling of cluster shutdown - what if all nodes move to 'removed', or 'joining' / 'weakly up'?
    //TODO unit test
    pub fn leader(&self) -> Option<NodeAddr> {
        if self.is_converged() {
            if let Some(up_member) = self.members.iter()
                .find(|(_, v)| v.state == NodeState::Up)
                .map(|(addr, _)| **addr)
            {
                return Some(up_member);
            }

            self.members.iter()
                .find(|(_, v)| v.state.is_leader_eligible())
                .map(|(addr, _)| **addr)
        }
        else {
            None
        }
    }

    /// Pick nodes for a new round of gossip by random, giving more weight to nodes that have not
    ///  yet converged. Nodes are picked randomly for each new round of gossip.
    //TODO unit test
    pub (in crate::cluster) fn new_gossip_partners(&self) -> Vec<NodeAddr> {
        const NUM_GOSSIP_PARTNERS: usize = 5; //TODO config

        let num_convergence_nodes = self.num_convergence_nodes();

        let (candidates, weights) = self.members.iter()
            .filter(|(_, s)| s.state.is_active())
            .map(|(addr, s)| (**addr, if s.seen_by.len() == num_convergence_nodes { 1u32 } else { 4u32 })) //TODO configurable weights etc.
            .collect::<(Vec<_>, Vec<_>)>();

        if candidates.len() <= NUM_GOSSIP_PARTNERS {
            return candidates;
        }

        let mut weights = WeightedIndex::new(&weights).unwrap();
        let mut rng = thread_rng();

        let mut result = Vec::with_capacity(NUM_GOSSIP_PARTNERS);

        for _ in 0..NUM_GOSSIP_PARTNERS {
            let index = weights.sample(&mut rng);
            result.push(candidates[index]);
            weights.update_weights(&[(index, &0u32)]).unwrap()
        }

        result
    }

    pub (in crate::cluster) fn new_gossip_message(&self, for_node: &NodeAddr) -> String {



        todo!()
    }

    pub fn hash_for_equality_check(&self, nonce: u32) -> u64 {
        //NB: spurious hash collision is not a problem - the nonce is to prevent it being permanent

        todo!()
    }
}
impl Crdt for Cluster {
    //TODO unit test
    fn merge_from(&mut self, other: &Self) -> CrdtOrdering {
        let all_orderings = other.members.values()
            .map(|other| {
                self.merge_node(other)
            });
        CrdtOrdering::merge_all(all_orderings)
            .unwrap_or(if self.members.is_empty() {CrdtOrdering::Equal} else { CrdtOrdering::SelfWasBigger })
    }
}

#[derive(Debug, Clone)]
pub struct NodeMembershipState {
    node_addr: NodeAddr,
    state: NodeState,
    seen_by: FxHashSet<SocketAddr>,
}
impl Crdt for NodeMembershipState {
    //TODO unit test
    fn merge_from(&mut self, other: &NodeMembershipState) -> CrdtOrdering {
        match self.state.merge_from(&other.state) {
            CrdtOrdering::Equal => {
                self.seen_by.merge_from(&other.seen_by)
            }
            CrdtOrdering::SelfWasBigger => {
                CrdtOrdering::SelfWasBigger
            }
            CrdtOrdering::OtherWasBigger => {
                *self = other.clone(); //TODO is 'other' available for passing in by-value anyway? then we could avoid this clone
                CrdtOrdering::OtherWasBigger
            }
            CrdtOrdering::NeitherWasBigger => {
                panic!("states are strictly ordered")
            },
        }
    }
}


/// see https://doc.akka.io/docs/akka/current/typed/cluster-membership.html
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum NodeState {
    /// A node has registered its wish to join the cluster, starting dissemination of that wish
    ///  through gossip - but the leader has not yet transitions the node to 'up' (after gossip
    ///  convergence was reached)
    Joining,
    /// todo
    WeaklyUp,
    /// The regular state for a node that is 'up and running', a full member of the cluster. Note
    ///  that reachability (or lack thereof) is orthogonal to states, so a node can be 'up' but
    ///  (temporarily) unreachable.
    Up,
    /// A node transitions to 'Leaving' when it starts to leave the cluster (typically as part of
    ///  its shutdown). Nodes in state 'leaving' are still full members of the cluster, but this
    ///  state allows 'higher-up' components built on top of the cluster to prepare for a node
    ///  leaving the cluster in a graceful manner (e.g resharding), i.e. without any gap in
    ///  operation.
    Leaving,
    /// Once all preparations for a node leaving the cluster are completed (i.e. confirmed by
    ///  all registered components on the leader node), the leader transitions a node to 'Exiting'.
    ///  An Exiting node is basically not part of the cluster anymore, and this is a transient
    ///  state for reaching gossip consensus before the leader moves the node to 'Removed'
    Exiting,
    /// 'Down' is not part of a node's regular lifecycle, but is assigned to unreachable nodes
    ///  algorithmically once some threshold of unreachability is passed; the details are intricate.
    ///
    /// In terms of a node's state CRDT, the transition 'Down' is irreversible, and once there
    ///  is consensus over a node being 'down', it is automatically propagated to 'Removed' by the
    ///  leader.
    ///
    /// Note that 'down' nodes are excluded from heartbeat and gossip - they are essentially written
    ///  of, and this state is just part of writing them out of the books.
    Down,
    /// This is a tombstone state: Once there is consensus that a node 'Removed', it can and should
    ///  be removed from internal tracking data structures: It ceases to exist for all intents and
    ///  purposes, and no messages (gossip, heartbeat or otherwise) should be sent to it. Its
    ///  process is likely terminated.
    Removed,
}
impl NodeState {
    /// Active nodes are the ones participating in gossip. Inactive nodes may be gossipped about,
    ///  but other nodes do not send them gossip messages, and they are ignored for gossip
    ///  convergence.
    pub fn is_active(&self) -> bool {
        match *self {
            NodeState::Down | NodeState::Removed => false,
            _ => true
        }
    }

    pub fn is_leader_eligible(&self) -> bool {
        match *self {
            NodeState::Down | NodeState::Removed => false,
            NodeState::Joining | NodeState::WeaklyUp => false,
            _ => true
        }
    }
}

impl Crdt for NodeState {
    fn merge_from(&mut self, other: &NodeState) -> CrdtOrdering {
        match Ord::cmp(self, other) {
            Ordering::Less => {
                *self = *other;
                CrdtOrdering::OtherWasBigger
            }
            Ordering::Equal => {
                CrdtOrdering::Equal
            }
            Ordering::Greater => {
                CrdtOrdering::SelfWasBigger
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use CrdtOrdering::*;
    use NodeState::*;

    use super::*;

    #[rstest]
    #[case(Joining, true)]
    #[case(WeaklyUp, true)]
    #[case(Up, true)]
    #[case(Leaving, true)]
    #[case(Exiting, true)]
    #[case(Down, false)]
    #[case(Removed, false)]
    fn test_node_state_is_active(#[case] state: NodeState, #[case] expected: bool) {
        assert_eq!(state.is_active(), expected);
    }

    #[rstest]
    #[case(Joining, false)]
    #[case(WeaklyUp, false)]
    #[case(Up, true)]
    #[case(Leaving, true)]
    #[case(Exiting, true)]
    #[case(Down, false)]
    #[case(Removed, false)]
    fn test_node_state_is_leader_eligible(#[case] state: NodeState, #[case] expected: bool) {
        assert_eq!(state.is_leader_eligible(), expected);
    }

    #[rstest]
    #[case(Joining,  Joining,  Joining,   Equal)]
    #[case(Joining,  WeaklyUp, WeaklyUp,  OtherWasBigger)]
    #[case(Joining,  Up,       Up,        OtherWasBigger)]
    #[case(Joining,  Leaving,  Leaving,   OtherWasBigger)]
    #[case(Joining,  Exiting,  Exiting,   OtherWasBigger)]
    #[case(Joining,  Down,     Down,      OtherWasBigger)]
    #[case(Joining,  Removed,  Removed,   OtherWasBigger)]
    #[case(WeaklyUp,  Joining,  WeaklyUp, SelfWasBigger)]
    #[case(WeaklyUp,  WeaklyUp, WeaklyUp, Equal)]
    #[case(WeaklyUp,  Up,       Up,       OtherWasBigger)]
    #[case(WeaklyUp,  Leaving,  Leaving,  OtherWasBigger)]
    #[case(WeaklyUp,  Exiting,  Exiting,  OtherWasBigger)]
    #[case(WeaklyUp,  Down,     Down,     OtherWasBigger)]
    #[case(WeaklyUp,  Removed,  Removed,  OtherWasBigger)]
    #[case(Up,        Joining,  Up,       SelfWasBigger)]
    #[case(Up,        WeaklyUp, Up,       SelfWasBigger)]
    #[case(Up,        Up,       Up,       Equal)]
    #[case(Up,        Leaving,  Leaving,  OtherWasBigger)]
    #[case(Up,        Exiting,  Exiting,  OtherWasBigger)]
    #[case(Up,        Down,     Down,     OtherWasBigger)]
    #[case(Up,        Removed,  Removed,  OtherWasBigger)]
    #[case(Leaving,   Joining,  Leaving,  SelfWasBigger)]
    #[case(Leaving,   WeaklyUp, Leaving,  SelfWasBigger)]
    #[case(Leaving,   Up,       Leaving,  SelfWasBigger)]
    #[case(Leaving,   Leaving,  Leaving,  Equal)]
    #[case(Leaving,   Exiting,  Exiting,  OtherWasBigger)]
    #[case(Leaving,   Down,     Down,     OtherWasBigger)]
    #[case(Leaving,   Removed,  Removed,  OtherWasBigger)]
    #[case(Exiting,   Joining,  Exiting,  SelfWasBigger)]
    #[case(Exiting,   WeaklyUp, Exiting,  SelfWasBigger)]
    #[case(Exiting,   Up,       Exiting,  SelfWasBigger)]
    #[case(Exiting,   Leaving,  Exiting,  SelfWasBigger)]
    #[case(Exiting,   Exiting,  Exiting,  Equal)]
    #[case(Exiting,   Down,     Down,     OtherWasBigger)]
    #[case(Exiting,   Removed,  Removed,  OtherWasBigger)]
    #[case(Down,      Joining,  Down,     SelfWasBigger)]
    #[case(Down,      WeaklyUp, Down,     SelfWasBigger)]
    #[case(Down,      Up,       Down,     SelfWasBigger)]
    #[case(Down,      Leaving,  Down,     SelfWasBigger)]
    #[case(Down,      Exiting,  Down,     SelfWasBigger)]
    #[case(Down,      Down,     Down,     Equal)]
    #[case(Down,      Removed,  Removed,  OtherWasBigger)]
    #[case(Removed,   Joining,  Removed,  SelfWasBigger)]
    #[case(Removed,   WeaklyUp, Removed,  SelfWasBigger)]
    #[case(Removed,   Up,       Removed,  SelfWasBigger)]
    #[case(Removed,   Leaving,  Removed,  SelfWasBigger)]
    #[case(Removed,   Exiting,  Removed,  SelfWasBigger)]
    #[case(Removed,   Down,     Removed,  SelfWasBigger)]
    #[case(Removed,   Removed,  Removed,  Equal)]
    fn test_node_state_merge(#[case] mut a: NodeState, #[case] b: NodeState, #[case] expected_state: NodeState, #[case] expected_ordering: CrdtOrdering) {
        assert_eq!(a.merge_from(&b), expected_ordering);
        assert_eq!(a, expected_state);
    }
}
