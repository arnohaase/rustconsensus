use std::sync::Arc;
use rustc_hash::FxHashSet;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time;
use crate::cluster::cluster_config::ClusterConfig;

use crate::messaging::node_addr::NodeAddr;

pub struct UnreachableTracker {
    config: Arc<ClusterConfig>,
    unreachable_nodes: FxHashSet<NodeAddr>,
    stability_period_handle: Option<JoinHandle<()>>,
    unstable_thrashing_timeout_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}
impl UnreachableTracker {
    pub fn new(config: Arc<ClusterConfig>) -> UnreachableTracker {
        UnreachableTracker {
            config,
            unreachable_nodes: FxHashSet::default(),
            stability_period_handle: None,
            unstable_thrashing_timeout_handle: Default::default(),
        }
    }

    pub async fn update_reachability(&mut self, node: NodeAddr, is_reachable: bool) {
        let was_fully_reachable = self.unreachable_nodes.is_empty();

        let modified = if is_reachable {
            self.unreachable_nodes.insert(node)
        }
        else {
            self.unreachable_nodes.remove(&node)
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

            // there are unreachable nodes, so we start a new timer for stability
            self.stability_period_handle = Some(tokio::spawn(async move {
                time::sleep(stability_period).await;

                let mut lock = unstable_thrashing_timeout_handle.write().await;
                if let Some(handle) = lock.as_ref() {
                    handle.abort();
                    *lock = None;
                }

                //TODO trigger downing provider
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