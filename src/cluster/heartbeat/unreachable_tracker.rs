use std::sync::Arc;

use rustc_hash::FxHashSet;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::info;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::messaging::node_addr::NodeAddr;

pub struct UnreachableTracker {
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
    unreachable_nodes: FxHashSet<NodeAddr>,
    stability_period_handle: Option<JoinHandle<()>>,
    unstable_thrashing_timeout_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    downing_strategy: Arc<dyn DowningStrategy>,
}
impl UnreachableTracker {
    pub fn new(config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, downing_strategy: Arc<dyn DowningStrategy>) -> UnreachableTracker {
        UnreachableTracker {
            config,
            cluster_state,
            unreachable_nodes: FxHashSet::default(),
            stability_period_handle: None,
            unstable_thrashing_timeout_handle: Default::default(),
            downing_strategy,
        }
    }

    pub async fn update_reachability(&mut self, node: NodeAddr, is_reachable: bool) {
        let was_fully_reachable = self.unreachable_nodes.is_empty();

        let modified = if is_reachable {
            self.unreachable_nodes.remove(&node)
        }
        else {
            self.unreachable_nodes.insert(node)
        };

        if !modified {
            return;
        }

        // Handle min stability period before downing: We want to base our downing decision on
        //  the 'big picture' rather than some initial partial information. And since there is
        //  no meaningful concept of convergence while some nodes are unreachable, we approximate
        //  that by waiting for the unreachable set to remain the same for a while.

        if let Some(handle) = &self.stability_period_handle {
            // reachability changed, so previous stability timer is irrelevant
            handle.abort();
            self.stability_period_handle = None;
        }

        if !self.unreachable_nodes.is_empty() {
            let stability_period = self.config.stability_period_before_downing;
            let unstable_thrashing_timeout_handle = self.unstable_thrashing_timeout_handle.clone();
            let cluster_state = self.cluster_state.clone();
            let downing_strategy = self.downing_strategy.clone();

            let unreachable_nodes = self.unreachable_nodes.clone();

            // there are unreachable nodes, so we start a new timer for stability
            self.stability_period_handle = Some(tokio::spawn(async move {
                time::sleep(stability_period).await;

                info!("unreachble set {:?} remained stable for {:?}: deferring to downing strategy {:?} for a decision", unreachable_nodes, stability_period, downing_strategy);

                let mut lock = unstable_thrashing_timeout_handle.write().await;
                if let Some(handle) = lock.as_ref() {
                    handle.abort();
                    *lock = None;
                }

                // NB: There is a miniscule possibility that a change in reachability occurred but
                //  has not reached this place, but it's not worth checking for: The whole 'stability'
                //  thing is somewhat racy and heuristic anyway
                let mut cluster_state = cluster_state.write().await;

                let node_states = cluster_state.node_states().cloned().collect::<Vec<_>>();
                let downing_decision = downing_strategy.decide(&node_states);
                cluster_state.on_stable_unreachable_set(downing_decision).await;
            }));
        }

        // Handle general network instability: If reachability keeps changing, there is likely
        //  some underlying problem outside the cluster's control, and we shut down all nodes
        //  after a configured interval with unreachable nodes but no stability long enough to
        //  trigger the downing provider.

        if was_fully_reachable {
            let mut lock = self.unstable_thrashing_timeout_handle.write().await;
            if let Some(handle) = lock.as_ref() {
                handle.abort();
            }

            let timeout_period = self.config.unstable_thrashing_timeout;
            *lock = Some(tokio::spawn(async move {
                time::sleep(timeout_period).await;

                //TODO shut down cluster
            }));
        }
    }
}