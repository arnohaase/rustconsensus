use crate::cluster::cluster_config::ClusterConfig;
use crate::messaging::node_addr::NodeAddr;
use crate::util::crdt::{Crdt, CrdtOrdering};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use tracing::{instrument, trace, warn};

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
        self.reachability.values().all(|r| r.is_reachable)
    }

    #[instrument(level = "trace", skip(config))]
    pub fn is_leader_eligible(&self, config: &ClusterConfig) -> bool {
        use MembershipState::*;
        let is_membership_eligible = match self.membership_state {
            Joining | WeaklyUp => false,
            Up | Leaving | Exiting => true,
            Down | Removed => false,
        };
        if !is_membership_eligible {
            trace!("state is not membership eligible -> false");
            return false;
        }

        if let Some(leader_eligible_roles) = &config.leader_eligible_roles {
            let result = leader_eligible_roles
                .iter()
                .any(|role| self.roles.contains(role));
            trace!("checking role eligibility: {}", result);
            result
        } else {
            trace!("state is membership eligible -> true");
            true
        }
    }

    /// returns a flag whether 'self' was modified in any way
    #[must_use]
    pub fn merge(&mut self, other: &NodeState) -> bool {
        let (merged, ordering) = self.merged_with(other);
        *self = merged;
        ordering.was_self_modified()
    }

    /// Pure CRDT merge: returns `(merged_state, ordering)` without mutating
    /// either input.
    #[must_use]
    pub fn merged_with(&self, other: &NodeState) -> (NodeState, CrdtOrdering) {
        assert_eq!(self.addr, other.addr);

        let mut merged = self.clone();
        let mut result_ordering = CrdtOrdering::Equal;

        let membership_state_result = merged.membership_state.merge_from(&other.membership_state);
        result_ordering = result_ordering.merge(membership_state_result);

        let roles_result = merged.roles.merge_from(&other.roles);
        if roles_result != CrdtOrdering::Equal {
            warn!("different roles when merging gossip state for node {:?} - merged and proceeding, but this is a bug somewhere in the distributed system", merged.addr);
        }
        result_ordering = result_ordering.merge(roles_result);

        for (addr, r_self) in merged.reachability.iter_mut() {
            if let Some(r_other) = other.reachability.get(addr) {
                match Ord::cmp(&r_self.counter_of_reporter, &r_other.counter_of_reporter) {
                    Ordering::Less => {
                        r_self.counter_of_reporter = r_other.counter_of_reporter;
                        r_self.is_reachable = r_other.is_reachable;
                        result_ordering = result_ordering.merge(CrdtOrdering::OtherWasBigger);
                    }
                    Ordering::Equal => {
                        if r_self.is_reachable != r_other.is_reachable {
                            warn!("gossip inconsistency for heartbeat of node {:?} as seen from {:?}@{}: ", merged.addr, addr, r_self.counter_of_reporter);
                            r_self.is_reachable = false;
                            result_ordering = CrdtOrdering::NeitherWasBigger;
                        }
                    }
                    Ordering::Greater => {
                        result_ordering = result_ordering.merge(CrdtOrdering::SelfWasBigger);
                    }
                }
            } else {
                result_ordering = result_ordering.merge(CrdtOrdering::SelfWasBigger);
            }
        }

        let reporters_only_in_other = other
            .reachability
            .iter()
            .filter(|(a, _)| !merged.reachability.contains_key(a))
            .map(|(a, r)| (*a, r.clone()))
            .collect::<Vec<_>>();

        for (addr, r) in reporters_only_in_other {
            merged.reachability.insert(addr, r);
            result_ordering = result_ordering.merge(CrdtOrdering::OtherWasBigger);
        }

        match result_ordering {
            CrdtOrdering::SelfWasBigger => {}
            CrdtOrdering::Equal => {
                for &s in &other.seen_by {
                    merged.seen_by.insert(s);
                }
            }
            CrdtOrdering::OtherWasBigger => {
                merged.seen_by = other.seen_by.clone();
            }
            CrdtOrdering::NeitherWasBigger => {
                merged.seen_by.clear();
            }
        }

        (merged, result_ordering)
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodeReachability {
    /// a node reporting a change in heartbeat for a node attaches a strictly monotonous
    ///  counter so that heartbeat can be merged in a coordination-free fashion
    pub counter_of_reporter: u32,
    /// only `reachable=false` is really of interest, heartbeat being the default. But storing
    ///  heartbeat is necessary to spread that information by gossip.
    pub is_reachable: bool,
}
impl Debug for NodeReachability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.is_reachable, self.counter_of_reporter)
    }
}

/// see https://doc.akka.io/docs/akka/current/typed/cluster-membership.html
#[repr(u8)]
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
pub enum MembershipState {
    Joining = 1,
    WeaklyUp = 2,
    Up = 3,
    Leaving = 4,
    Exiting = 5,
    Down = 6,
    Removed = 7,
}
impl MembershipState {
    pub fn is_gossip_partner(&self) -> bool {
        use MembershipState::*;
        matches!(self, Joining | WeaklyUp | Up | Leaving | Exiting)
    }

    pub fn is_terminal(&self) -> bool {
        use MembershipState::*;
        matches!(self, Down | Removed)
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
            Ordering::Greater => CrdtOrdering::SelfWasBigger,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::state::node_state::{MembershipState, NodeState};
    use crate::node_state;
    use crate::test_util::node::test_node_addr_from_number;
    use rstest::rstest;
    use rustc_hash::FxHashSet;
    use MembershipState::*;

    #[rstest]
    #[case::two_reachable(node_state!(1['a']:Up->[2:true@7,  3:true@5 ]@[1,2,3]), true)]
    #[case::one_reachable(node_state!(1['a']:Up->[2:true@7            ]@[1,2,3]), true)]
    #[case::one_unreachable(node_state!(1['a']:Up->[2:false@7           ]@[1,2,3]), false)]
    #[case::first_unreachable(node_state!(1['a']:Up->[2:false@7, 3:true@5 ]@[1,2,3]), false)]
    #[case::second_unreachable(node_state!(1['a']:Up->[2:true@7,  3:false@5]@[1,2,3]), false)]
    #[case::both_unreachable(node_state!(1['a']:Up->[2:false@7, 3:false@5]@[1,2,3]), false)]
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
        let mut config = ClusterConfig::new_for_test(test_node_addr_from_number(1).socket_addr);
        if let Some(required_roles) = required_roles {
            let leader_eligible_roles = required_roles.iter().map(|r| r.to_string()).collect::<FxHashSet<_>>();
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
        let first_before = first.clone();
        let second_before = second.clone();

        let (pure_merged, pure_ordering) = first.merged_with(&second);
        assert_eq!(first, first_before, "merged_with must not mutate self");
        assert_eq!(second, second_before, "merged_with must not mutate other");
        assert_eq!(pure_merged, expected_merged);
        assert_eq!(pure_ordering.was_self_modified(), expected_was_self_changed);

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
