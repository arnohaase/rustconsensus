use std::sync::Arc;

use rustc_hash::FxHashSet;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterStateHandle;
use crate::cluster::gossip::gossip_messages::GossipMessage;
use crate::cluster::heartbeat::downing_strategy::{DowningStrategy, DowningStrategyDecision};
use crate::messaging::messaging::{MessageSender, MessageSenderExt};
use crate::messaging::node_addr::NodeAddr;


/// The [UnreachableTracker] picks up work when nodes are declared 'unreachable', either locally
///  by a reachability decider, or remotely via gossip. It tracks stability of overall node
///  reachability and invokes the [DowningStrategy] to resolve long-term unreachability.
pub struct UnreachableTracker  {
    config: Arc<ClusterConfig>,
    cluster_state: ClusterStateHandle,
    unreachable_nodes: FxHashSet<NodeAddr>,
    stability_period_handle: Option<JoinHandle<()>>,
    unstable_thrashing_timeout_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    downing_strategy: Arc<dyn DowningStrategy>,
}
impl  UnreachableTracker {
    pub fn new(config: Arc<ClusterConfig>, cluster_state: ClusterStateHandle, downing_strategy: Arc<dyn DowningStrategy>) -> UnreachableTracker {
        UnreachableTracker {
            config,
            cluster_state,
            unreachable_nodes: FxHashSet::default(),
            stability_period_handle: None,
            unstable_thrashing_timeout_handle: Default::default(),
            downing_strategy,
        }
    }

    pub async fn update_reachability<M: MessageSender>(&mut self, node: NodeAddr, is_reachable: bool, messaging: Arc<M>) {
        let was_fully_reachable = self.unreachable_nodes.is_empty();

        // start with the actual update - that's the simple part:
        let modified = if is_reachable {
            self.unreachable_nodes.remove(&node)
        }
        else {
            self.unreachable_nodes.insert(node)
        };

        if !modified {
            return;
        }

        if is_reachable {
            debug!("node {:?} became reachable", node)
        }
        else {
            debug!("node {:?} became unreachable", node)
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

                info!("unreachable set {:?} remained stable for {:?}: deferring to downing strategy {:?} for a decision", unreachable_nodes, stability_period, downing_strategy);

                let mut lock = unstable_thrashing_timeout_handle.write().await;
                if let Some(handle) = lock.as_ref() {
                    handle.abort();
                    *lock = None;
                }

                // NB: There is a miniscule possibility that a change in reachability occurred but
                //  has not reached this place, but it's not worth checking for: The whole 'stability'
                //  thing is somewhat racy and heuristic anyway
                //
                // Use a snapshot to feed the downing strategy and the actor
                // to apply the resulting decision. The actor processes the
                // decision as a single command, preserving atomicity of the
                // multi-node promotion sequence.
                let snap = cluster_state.snapshot();
                let node_states: Vec<_> = snap.node_states().map(|s| (**s).clone()).collect();
                let downing_decision = downing_strategy.decide(&node_states);
                Self::on_downing_decision(&cluster_state, downing_decision, messaging.as_ref()).await;
            }));
        }

        // Handle general network instability: If reachability keeps changing, there is likely
        //  some underlying problem outside the cluster's control, and we shut down all nodes
        //  after a configured interval with unreachable nodes but no stability long enough to
        //  trigger the downing provider.

        if was_fully_reachable {
            // reachable -> abort and remove thrashing timer
            let mut lock = self.unstable_thrashing_timeout_handle.write().await;
            if let Some(handle) = lock.as_ref() {
                handle.abort();
                *lock = None;
            }
        }
        else {
            // unreachable -> start thrashing timer if it does not run already
            let mut lock = self.unstable_thrashing_timeout_handle.write().await;
            if lock.is_none() {
                let timeout_period = self.config.unstable_thrashing_timeout;
                let cluster_state = self.cluster_state.clone();
                let messaging = messaging.clone();

                *lock = Some(tokio::spawn(async move {
                    time::sleep(timeout_period).await;

                    // not canceled -> shut down the whole cluster
                    warn!("unreachable for {:?} without reaching a stable configuration: shutting down the entire cluster", timeout_period);

                    // Atomicity here is preserved by the single-writer
                    // actor: each `on_downing_decision` call submits one
                    // `ApplyDowningDecision` command, and the actor
                    // processes commands strictly serially. DownThem runs
                    // before DownUs so that "them" still exists when the
                    // decision is computed.
                    Self::on_downing_decision(&cluster_state, DowningStrategyDecision::DownThem, messaging.as_ref()).await;
                    Self::on_downing_decision(&cluster_state, DowningStrategyDecision::DownUs, messaging.as_ref()).await;
                }));
            }
        }
    }

    async fn on_downing_decision<M: MessageSender>(cluster_state: &ClusterStateHandle, downing_strategy_decision: DowningStrategyDecision, messaging: &M) {
        info!("downing decision: {:?}", downing_strategy_decision);
        let downed_nodes = cluster_state.cmd_apply_downing_decision(downing_strategy_decision).await;
        info!("downing nodes {:?}", downed_nodes);
        for n in downed_nodes {
            // This is a best effort to notify all affected nodes of the downing decision.
            //  We cannot reach all nodes anyway, and there may be network problems, so this is
            //  *not* a reliable notification - but it may help in the face of problems
            if let Err(e) = messaging.send_low_latency(n, &GossipMessage::DownYourself).await {
                warn!("failed to send DownYourself to {:?}: {}", n, e);
            }
        }
    }

    /// Recovery path for `broadcast::error::RecvError::Lagged`: rebuild our
    /// view of the unreachable set from an authoritative snapshot (computed
    /// by the caller from `ClusterStateHandle::snapshot()`).
    ///
    /// We diff `target_unreachable` against our currently-tracked set and
    /// feed each flip through [`Self::update_reachability`] so that the
    /// stability and thrashing timers stay coherent. Callers must pass a
    /// snapshot-derived set so that `target_unreachable` reflects a
    /// consistent point-in-time view; mixing live reads here would
    /// re-introduce the very lock-scope bug the snapshot model exists to
    /// avoid.
    pub async fn resync_from_snapshot<M: MessageSender>(&mut self, target_unreachable: FxHashSet<NodeAddr>, messaging: Arc<M>) {
        // Nodes that we currently believe are unreachable but the snapshot
        // says are now reachable → feed as `is_reachable = true`.
        let now_reachable: Vec<NodeAddr> = self.unreachable_nodes
            .difference(&target_unreachable)
            .copied()
            .collect();
        for addr in now_reachable {
            self.update_reachability(addr, true, messaging.clone()).await;
        }
        // Nodes that the snapshot says are unreachable but we did not yet
        // know about → feed as `is_reachable = false`.
        let now_unreachable: Vec<NodeAddr> = target_unreachable
            .difference(&self.unreachable_nodes)
            .copied()
            .collect();
        for addr in now_unreachable {
            self.update_reachability(addr, false, messaging.clone()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_state::ClusterStateHandle;
    use crate::cluster::gossip::gossip_messages::GossipMessage;
    use crate::cluster::heartbeat::downing_strategy::{DowningStrategyDecision, MockDowningStrategy};
    use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
    use crate::cluster::state::node_state::MembershipState::{Down, Up};
    use crate::messaging::node_addr::NodeAddr;
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Builder;
    use tokio::time;

    fn handle_for_new(myself: NodeAddr, config: Arc<ClusterConfig>) -> ClusterStateHandle {
        ClusterStateHandle::new(myself, config)
    }

    #[tokio::test(start_paused = true)]
    async fn test_update_unreachable_set() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new_for_test(myself.socket_addr));
        let handle = handle_for_new(myself, config.clone());

        let downing_strategy = Arc::new(MockDowningStrategy::new());
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let mut tracker = UnreachableTracker::new(config, handle, downing_strategy);

        tracker.update_reachability(test_node_addr_from_number(2), true, messaging.clone()).await;
        assert!(tracker.unreachable_nodes.is_empty());

        tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
        assert_eq!(tracker.unreachable_nodes.iter().cloned().collect::<Vec<_>>(), vec![test_node_addr_from_number(2)]);

        tracker.update_reachability(test_node_addr_from_number(2), true, messaging.clone()).await;
        assert!(tracker.unreachable_nodes.is_empty());

        time::advance(Duration::from_secs(1000)).await;
    }

    #[rstest]
    #[case::them(DowningStrategyDecision::DownThem, vec![2])]
    #[case::us(DowningStrategyDecision::DownUs, vec![1,3])]
    fn test_downing(#[case] decision: DowningStrategyDecision, #[case] expected_downed_nodes: Vec<u16>) {
        let expected_downed_nodes = expected_downed_nodes.into_iter()
            .map(|n| test_node_addr_from_number(n))
            .collect::<Vec<_>>();

        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            time::pause();

            let myself = test_node_addr_from_number(1);
            let config = Arc::new(ClusterConfig::new_for_test(myself.socket_addr));
            let handle = handle_for_new(myself, config.clone());
            handle.cmd_merge_node_state(node_state!(2[]:Up->[3:false@9]@[1,2,3])).await;
            handle.cmd_merge_node_state(node_state!(3[]:Up->[]@[1,2,3])).await;
            handle.flush().await;

            let mut downing_strategy = MockDowningStrategy::new();
            downing_strategy.expect_decide()
                .once()
                .return_const(decision);
            let downing_strategy = Arc::new(downing_strategy);
            let messaging = Arc::new(TrackingMockMessageSender::new(myself));

            let mut tracker = UnreachableTracker::new(config, handle.clone(), downing_strategy);

            tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;

            // reachability changes reset timers
            time::sleep(Duration::from_secs(4)).await;
            tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;
            time::sleep(Duration::from_secs(4)).await;
            tracker.update_reachability(test_node_addr_from_number(3), true, messaging.clone()).await;
            time::sleep(Duration::from_secs(4)).await;
            // sending an update that does not change anything does not reset timers
            tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
            time::sleep(Duration::from_millis(1001)).await;

            handle.flush().await;
            for n in expected_downed_nodes {
                let membership_state = handle.snapshot()
                    .get_node_state(&n).unwrap()
                    .membership_state;
                assert_eq!(membership_state, Down);
                messaging.assert_message_sent(n, GossipMessage::DownYourself).await;
            }

            messaging.assert_no_remaining_messages().await;
        });
    }

    #[tokio::test(start_paused = true)]
    async fn test_unstable_shutdown() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new_for_test(myself.socket_addr));
        let handle = handle_for_new(myself, config.clone());
        handle.cmd_merge_node_state(node_state!(2[]:Up->[3:false@9]@[1,2,3])).await;
        handle.cmd_merge_node_state(node_state!(3[]:Up->[]@[1,2,3])).await;
        handle.flush().await;

        let downing_strategy = Arc::new(MockDowningStrategy::new());
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let mut tracker = UnreachableTracker::new(config, handle.clone(), downing_strategy);

        tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        // unstable timeout timer is restarted when all nodes become reachable again
        tracker.update_reachability(test_node_addr_from_number(2), true, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;

        // node 2 becomes and stays unreachable while node 3 flips between reachability and unreachability. This
        //  prevents the reachability tracker from become 'stable', triggering its 'thrashing' path that should
        //  shut down the entire cluster on a best-effort bases.
        tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), true, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), true, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;

        messaging.assert_no_remaining_messages().await;

        time::sleep(Duration::from_millis(4001)).await;

        messaging.assert_message_sent(test_node_addr_from_number(2), GossipMessage::DownYourself).await;
        messaging.assert_message_sent(test_node_addr_from_number(1), GossipMessage::DownYourself).await;
        messaging.assert_message_sent(test_node_addr_from_number(3), GossipMessage::DownYourself).await;
        messaging.assert_no_remaining_messages().await;

        handle.flush().await;
        let snap = handle.snapshot();
        let membership_state = snap
            .get_node_state(&test_node_addr_from_number(1)).unwrap()
            .membership_state;
        assert_eq!(membership_state, Down);

        let membership_state = snap
            .get_node_state(&test_node_addr_from_number(2)).unwrap()
            .membership_state;
        assert_eq!(membership_state, Down);

        let membership_state = snap
            .get_node_state(&test_node_addr_from_number(3)).unwrap()
            .membership_state;
        assert_eq!(membership_state, Down);
    }
}
