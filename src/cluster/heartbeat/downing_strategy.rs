use std::fmt::Debug;
use std::net::SocketAddr;
use tracing::info;
use crate::cluster::cluster_state::NodeState;
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision::{DownThem, DownUs};

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
/// on every node independently, not by the leader
///
/// inconsistent sets of nodes: no convergence, unreachability can happen during promotion
/// --> >= Up provides a particularly window of uncertainty because the change requires a leader
pub trait DowningStrategy: Debug + Send + Sync {
    fn decide(
        &self,
        node_states: &[NodeState],
    ) -> DowningStrategyDecision;
}

pub enum DowningStrategyDecision {
    DownUs,
    DownThem,
}


//TODO documentation, unit tests
#[derive(Debug)]
pub struct QuorumOfSeedNodesStrategy {
    pub seed_nodes: Vec<SocketAddr>,
}
impl DowningStrategy for QuorumOfSeedNodesStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_reachable_seed_nodes = node_states.iter()
            .filter(|s| s.is_reachable())
            .filter(|s| self.seed_nodes.contains(&s.addr.addr))
            .count();

        info!("{} of {} seed nodes are in the cluster, {} of them are reachable",
            num_reachable_seed_nodes,
            node_states.iter()
                .filter(|s| self.seed_nodes.contains(&s.addr.addr))
                .count(),
            self.seed_nodes.len());

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

//TODO documentation, unit tests
#[derive(Debug)]
pub struct QuorumOfNodesStrategy {}
impl DowningStrategy for QuorumOfNodesStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_reachable_nodes = node_states.iter()
            .filter(|s| s.is_reachable())
            .count();

        info!("{} of {} nodes in the cluster are reachable",
            num_reachable_nodes,
            node_states.len());

        quorum_decision(num_reachable_nodes, node_states.len())
    }
}


//TODO documentation, unit tests
#[derive(Debug)]
pub struct QuorumOfRoleStrategy {
    pub required_role: String,
}
impl DowningStrategy for QuorumOfRoleStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        let num_nodes_with_role = node_states.iter()
            .filter(|s| s.roles.contains(&self.required_role))
            .count();

        let num_reachable_nodes = node_states.iter()
            .filter(|s| s.roles.contains(&self.required_role))
            .filter(|s| s.is_reachable())
            .count();

        info!("{} of {} nodes in the cluster have role {}, {} of them are reachable",
            num_reachable_nodes,
            node_states.len(),
            self.required_role,
            num_nodes_with_role,
        );

        quorum_decision(num_reachable_nodes, num_nodes_with_role)
    }
}


//TODO documentation, unit test
#[derive(Debug)]
pub struct LeaderSurvivesStrategy {}
impl DowningStrategy for LeaderSurvivesStrategy {
    fn decide(&self, node_states: &[NodeState]) -> DowningStrategyDecision {
        //TODO role requirements for leader eligibility

        let opt_leader_candidate = node_states.iter()
            .find(|s| s.membership_state.is_leader_eligible());

        if let Some(l) = opt_leader_candidate {
            if l.is_reachable() {
                info!("leader candidate {:?} is reachable", l);
                return DownThem;
            }
            else {
                info!("leader candidate {:?} is not reachable", l);
                return DownUs;
            }
        }

        info!("no leader candidate in the cluster - downing us to be on the safe side");
        DownUs
    }
}

