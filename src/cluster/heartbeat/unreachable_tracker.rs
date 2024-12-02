use std::ops::DerefMut;
use std::sync::Arc;

use rustc_hash::FxHashSet;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{info, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::gossip_messages::GossipMessage;
use crate::cluster::heartbeat::downing_strategy::{DowningStrategy, DowningStrategyDecision};
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;

pub struct UnreachableTracker  {
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
    unreachable_nodes: FxHashSet<NodeAddr>,
    stability_period_handle: Option<JoinHandle<()>>,
    unstable_thrashing_timeout_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    downing_strategy: Arc<dyn DowningStrategy>,
}
impl  UnreachableTracker {
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

    pub async fn update_reachability(&mut self, node: NodeAddr, is_reachable: bool, messaging: Arc<dyn Messaging>) {
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
            let messaging = messaging.clone();

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
                Self::on_downing_decision(&mut cluster_state, downing_decision, messaging.as_ref()).await;
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
            let cluster_state = self.cluster_state.clone();
            let messaging = messaging.clone();

            *lock = Some(tokio::spawn(async move {
                time::sleep(timeout_period).await;

                // not canceled -> shut down the whole cluster
                warn!("unreachable for {:?} without reaching a stable configuration: shutting down the entire cluster", timeout_period);
                // we want the downing of all nodes to be atomic
                let mut cs_lock = cluster_state.write().await;
                Self::on_downing_decision(cs_lock.deref_mut(), DowningStrategyDecision::DownUs, messaging.as_ref()).await;
                Self::on_downing_decision(cs_lock.deref_mut(), DowningStrategyDecision::DownThem, messaging.as_ref()).await;
            }));
        }
    }

    async fn on_downing_decision(cluster_state: &mut ClusterState, downing_strategy_decision: DowningStrategyDecision, messaging: &dyn Messaging) {
        let downed_nodes = cluster_state.apply_downing_decision(downing_strategy_decision).await;
        for n in downed_nodes {
            // This is a best effort to notify all affected nodes of the downing decision.
            //  We cannot reach all nodes anyway, and there may be network problems, so this is
            //  *not* a reliable notification - but it may help in the face of problems
            let _ = messaging.send(n, &GossipMessage::DownYourself).await;
        }
    }
}