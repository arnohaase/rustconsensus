use crate::cluster::cluster_state::NodeState;
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision::{DownThem, DownUs};
#[cfg(test)] use mockall::automock;
use std::fmt::Debug;
use std::net::SocketAddr;
use tracing::info;

/// A [DowningStrategy] decides which nodes should continue to run and which nodes should be
///  terminated (i.e. promoted to 'down'). It is called when the set of unreachable nodes has
///  been *stable* for a configured period.
///
/// ## Network Partitions
///
/// The reason for making this a configurable strategy is *network partitions*: Even if some nodes
///  become unreachable from this node, they can still be running and doing work - with this node
///  being unreachable as far as they are concerned. In such a situation, both partitions of the
///  network need to agree on (at most) one of the partitions that can continue, and terminate the
///  rest.
///
/// And they need to agree without communicating - they are unreachable to each other after all.
///  That is the purpose and the main constraint for [DowningStrategy] implementations: To make
///  consistent decisions without requiring coordination.
///
/// Note that the most common 'unreachable' scenario by far is a single node being killed without
///  going through its regular shutdown and deregistration sequence. All [DowningStrategy]
///  implementations must handle this case well, typically by downing the single killed node.
///
/// ## Decision results
///
/// As far as a downing strategy is concerned, there are only two groups of nodes:
///  * "us", i.e. the nodes that are reachable from here
///  * "them", i.e. the nodes unreachable from here
///
/// These groups survive or get downed as a whole, there is no good reason for a downing strategy
///  to distinguish between nodes in a group. So there are two possible decisions for a downing
///  strategy, **DownUs** and **DownThem**.
///
/// The only constraint on the decision is that at most one partition may vote **DownThem** in a
///  given scenario: That would lead to two separate clusters and could cause inconsistencies.
///
/// But it is perfectly valid for several or all partitions to vote **DownUs**, causing the entire
///  cluster to shut down if none of the partitions are big enough to continue on their own.
///
/// ## TODO documentation
///
/// in general: heuristic, no guarantee that 'joining' nodes agree
/// TODO filter to ignore nodes < Up?
///
/// 'leader survives' strategy - nodes may disagree on who the leader candidate is; scenario that
///  can't be better handled by seed node or role strategy?
///
/// on every node independently, not by the leader
///
/// inconsistent sets of nodes: no convergence, unreachability can happen during promotion
/// --> >= Up provides a particularly window of uncertainty because the change requires a leader
#[cfg_attr(test, automock)]
pub trait DowningStrategy: Debug + Send + Sync {
    fn decide(
        &self,
        node_states: &[NodeState],
    ) -> DowningStrategyDecision;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DowningStrategyDecision {
    DownUs,
    DownThem,
}


/// [QuorumOfSeedNodesStrategy] decides survival / downing based on the set of seed nodes that are
///  used for discovery: A partition needs to have more than half of the seed nodes in order to
///  survive.
///
/// The idea behind this is that new nodes joining the cluster will do so based on the globally
///  well-known seed nodes, so the surviving part of the cluster needs the seed nodes to be
///  discoverable.
#[derive(Debug)]
pub struct QuorumOfSeedNodesStrategy {
    pub seed_nodes: Vec<SocketAddr>,
}
impl DowningStrategy for QuorumOfSeedNodesStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_reachable_seed_nodes = node_states.iter()
            .filter(|s| !s.membership_state.is_terminal())
            .filter(|s| s.is_reachable())
            .filter(|s| self.seed_nodes.contains(&s.addr.socket_addr))
            .count();

        info!("{} of {} seed nodes are in the cluster, {} of them are reachable",
            node_states.iter()
                .filter(|s| self.seed_nodes.contains(&s.addr.socket_addr))
                .count(),
            self.seed_nodes.len(),
            num_reachable_seed_nodes,
        );

        quorum_decision(num_reachable_seed_nodes, self.seed_nodes.len())
    }
}

fn quorum_decision(actual: usize, total: usize) -> DowningStrategyDecision {
    if 2*actual > total {
        DownThem
    }
    else {
        DownUs
    }
}


/// [QuorumOfNodesStrategy] lets a partition survive if more than half the total
///  (non-terminal) nodes currently in the cluster are reachable.
///
/// NB: This can cause a small partition to survive if the rest becomes unreachable gradually rather
///  than all at once (which may or may not be the desired behavior).
#[derive(Debug)]
pub struct QuorumOfNodesStrategy {}
impl DowningStrategy for QuorumOfNodesStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_reachable_nodes = node_states.iter()
            .filter(|s| !s.membership_state.is_terminal())
            .filter(|s| s.is_reachable())
            .count();

        let num_all_nodes = node_states.iter()
            .filter(|s| !s.membership_state.is_terminal())
            .count();

        info!("{} of {} non-terminal nodes in the cluster are reachable",
            num_reachable_nodes,
            num_all_nodes);

        quorum_decision(num_reachable_nodes, num_all_nodes)
    }
}


/// [QuorumOfRoleStrategy] decides based on the quorum of nodes with a given role.
#[derive(Debug)]
pub struct QuorumOfRoleStrategy {
    pub required_role: String,
}
impl DowningStrategy for QuorumOfRoleStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_nodes_with_role = node_states.iter()
            .filter(|s| !s.membership_state.is_terminal())
            .filter(|s| s.roles.contains(&self.required_role))
            .count();

        let num_reachable_nodes = node_states.iter()
            .filter(|s| !s.membership_state.is_terminal())
            .filter(|s| s.roles.contains(&self.required_role))
            .filter(|s| s.is_reachable())
            .count();

        info!("{} of {} nodes in the cluster have role {}, {} of them are reachable",
            node_states.len(),
            self.required_role,
            num_nodes_with_role,
            num_reachable_nodes,
        );

        quorum_decision(num_reachable_nodes, num_nodes_with_role)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_state;
    use crate::cluster::cluster_state::*;
    use MembershipState::*;
    use crate::test_util::node::test_node_addr_from_number;
    use rstest::rstest;

    #[rstest]
    #[case::empty(vec![], DownUs)]
    #[case::all_reachable(vec![node_state!(1[]:Up->[]@[1,2,3]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[]@[1,2,3])], DownThem)]
    #[case::all_unreachable(vec![node_state!(1[]:Up->[4:false@2]@[1,2,3]),node_state!(2[]:Up->[4:false@2]@[1,2,3]),node_state!(3[]:Up->[4:false@2]@[1,2,3])], DownUs)]
    #[case::quorum_of_seed_nodes_reachable(vec![node_state!(1[]:Up->[]@[]),node_state!(2[]:Up->[]@[1,2,3]),node_state!(3[]:Up->[9:false@3]@[1,2,3]),node_state!(4[]:Up->[9:false@4]@[1,2,3]),node_state!(5[]:Up->[9:false@4]@[1,2,3])], DownThem)]
    #[case::quorum_of_seed_nodes_unreachable(vec![node_state!(1[]:Up->[]@[]),node_state!(2[]:Up->[9:false@2]@[1,2,3]),node_state!(3[]:Up->[9:false@8]@[1,2,3]),node_state!(4[]:Up->[]@[1,2,3]),node_state!(5[]:Up->[]@[1,2,3])], DownUs)]
    #[case::terminal_nodes(vec![node_state!(1[]:Up->[]@[]),node_state!(2[]:Down->[]@[1]),node_state!(3[]:Removed->[]@[1])], DownUs)]
    #[case::part_of_seed_nodes_present(vec![node_state!(1[]:Up->[]@[]),node_state!(2[]:Up->[]@[1,2,3])], DownThem)]
    #[case::one_seed_nodes_present(vec![node_state!(1[]:Up->[]@[])], DownUs)]
    fn test_quorum_of_seed_nodes_strategy(#[case] node_states: Vec<NodeState>, #[case] expected_decision: DowningStrategyDecision) {
        let seed_nodes = vec![
            test_node_addr_from_number(1).socket_addr,
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ];

        let strategy = QuorumOfSeedNodesStrategy { seed_nodes };
        let decision = strategy.decide(&node_states);
        assert_eq!(decision, expected_decision);
    }

    #[rstest]
    #[case::empty(0, 5, DownUs)]
    #[case::smallest(1, 5, DownUs)]
    #[case::small(2, 5, DownUs)]
    #[case::big(3, 5, DownThem)]
    #[case::biggest(4, 5, DownThem)]
    #[case::all(5, 5, DownThem)]
    #[case::half(3, 6, DownUs)] // corner case: we need *more* than half the nodes to survive (as a tie-breaker)
    fn test_quorum_decision(#[case] actual: usize, #[case] total: usize, #[case] expected: DowningStrategyDecision) {
        assert_eq!(quorum_decision(actual, total), expected);
    }

    #[rstest]
    #[case::empty(vec![], DownUs)]
    #[case::quorum_reachable(vec![node_state!(1[]:Up->[]@[1]),node_state!(2[]:Up->[]@[1]),node_state!(3[]:Up->[4:false@3]@[1])], DownThem)]
    #[case::quorum_unreachable(vec![node_state!(1[]:Up->[]@[1]),node_state!(2[]:Up->[4:false@5]@[1]),node_state!(3[]:Up->[4:false@3]@[1])], DownUs)]
    #[case::half_reachable(vec![node_state!(1[]:Up->[]@[1]),node_state!(2[]:Up->[4:false@5]@[1])], DownUs)] // corner case: we need *more* than half the nodes to be reachable
    #[case::reachable_despite_terminal_nodes(vec![node_state!(1[]:Up->[]@[1]),node_state!(2[]:Up->[]@[1]),node_state!(3[]:Up->[4:false@3]@[1]),node_state!(8[]:Down->[4:false@8]@[1]),node_state!(9[]:Removed->[4:false@5]@[1])], DownThem)]
    #[case::unreachable_despite_terminal_nodes(vec![node_state!(1[]:Up->[]@[1]),node_state!(2[]:Up->[4:false@3]@[1]),node_state!(3[]:Up->[4:false@3]@[1]),node_state!(8[]:Down->[]@[1]),node_state!(9[]:Removed->[]@[1])], DownUs)]
    fn test_quorum_of_nodes_strategy(#[case] node_states: Vec<NodeState>, #[case] expected: DowningStrategyDecision) {
        let strategy = QuorumOfNodesStrategy {};
        let decision = strategy.decide(&node_states);
        assert_eq!(decision, expected);
    }

    #[rstest]
    #[case::all(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[]@[]),node_state!(3["a"]:Up->[]@[])], DownThem)]
    #[case::none(vec![node_state!(1["a"]:Up->[4:false@9]@[]),node_state!(2["a"]:Up->[4:false@3]@[]),node_state!(3["a"]:Up->[4:false@3]@[])], DownUs)]
    #[case::quorum(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[]@[]),node_state!(3["a"]:Up->[4:false@8]@[])], DownThem)]
    #[case::no_quorum(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[4:false@8]@[]),node_state!(3["a"]:Up->[4:false@8]@[])], DownUs)]
    #[case::half(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[4:false@8]@[])], DownUs)] // corner case: we need *more than* half the nodes to survive
    #[case::additional_role_counts_for_quorum(vec![node_state!(1["a","b"]:Up->[]@[]),node_state!(2["a","b"]:Up->[]@[]),node_state!(3["a"]:Up->[4:false@8]@[])], DownThem)]
    #[case::additional_role_counts_against_quorum(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a","b"]:Up->[4:false@8]@[]),node_state!(3["a","b"]:Up->[4:false@8]@[])], DownUs)]
    #[case::quorum_roles_only(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[]@[]),node_state!(3["a"]:Up->[4:false@3]@[]),node_state!(4[]:Up->[4:false@3]@[]),node_state!(5[]:Up->[5:false@3]@[])], DownThem)]
    #[case::no_quorum_roles_only(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[4:false@4]@[]),node_state!(3["a"]:Up->[4:false@3]@[]),node_state!(4[]:Up->[]@[]),node_state!(5[]:Up->[]@[])], DownUs)]
    #[case::quorum_terminal_nodes(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[]@[]),node_state!(3["a"]:Up->[4:false@3]@[]),node_state!(4["a"]:Down->[4:false@3]@[]),node_state!(5["a"]:Removed->[5:false@3]@[])], DownThem)]
    #[case::no_quorum_terminal_nodes(vec![node_state!(1["a"]:Up->[]@[]),node_state!(2["a"]:Up->[4:false@4]@[]),node_state!(3["a"]:Up->[4:false@3]@[]),node_state!(4["a"]:Down->[]@[]),node_state!(5["a"]:Removed->[]@[])], DownUs)]
    fn test_quorum_of_role_strategy(#[case] node_states: Vec<NodeState>, #[case] expected: DowningStrategyDecision) {
        let strategy = QuorumOfRoleStrategy { required_role: "a".to_string() };
        let decision = strategy.decide(&node_states);
        assert_eq!(decision, expected);
    }
}