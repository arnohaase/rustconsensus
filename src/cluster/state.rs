use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::util::crdt::{Crdt, CrdtOrdering};

//TODO concurrent initial startup -> how to detect and merge disjoint clusters
//TODO garbage collection for 'removed' tombstones


pub struct MembershipState {
    members: FxHashMap<SocketAddr, NodeMembershipState>,
}
impl MembershipState {
    /// Merge a single node's state into the overall membership state. The returned ordering
    ///  refers to this specific node's state.

    //TODO unit test
    pub fn merge_node(&mut self, addr: SocketAddr, other: &NodeMembershipState) -> CrdtOrdering {
        match self.members.entry(addr) {
            Entry::Occupied(mut e) => {
                e.get_mut().merge_from(other)
            }
            Entry::Vacant(e) => {
                e.insert(other.clone());
                CrdtOrdering::OtherWasBigger
            }
        }
    }
}
impl Crdt for MembershipState {
    //TODO unit test
    fn merge_from(&mut self, other: &Self) -> CrdtOrdering {
        let all_orderings = other.members.iter()
            .map(|(&addr, other)| {
                self.merge_node(addr, other)
            });
        CrdtOrdering::merge_all(all_orderings)
            .unwrap_or(if self.members.is_empty() {CrdtOrdering::Equal} else { CrdtOrdering::SelfWasBigger })
    }
}

//TODO unique part / NodeAddr?

#[derive(Debug, Clone)]
pub struct NodeMembershipState {
    state: NodeState,
    seen_by: FxHashSet<SocketAddr>,
}
impl Crdt for NodeMembershipState {
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
    Joining,
    WeaklyUp,
    Up,
    Leaving,
    Exiting,
    Down,
    Removed,
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
    use NodeState::*;

    use super::*;
    use CrdtOrdering::*;

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
