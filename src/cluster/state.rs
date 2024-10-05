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
    //TODO unit test
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
                CrdtOrdering::OtherWasBigger
            }
        }
    }
}
