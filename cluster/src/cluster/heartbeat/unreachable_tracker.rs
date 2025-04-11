use std::ops::DerefMut;
use std::sync::Arc;

use rustc_hash::FxHashSet;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::gossip_messages::GossipMessage;
use crate::cluster::heartbeat::downing_strategy::{DowningStrategy, DowningStrategyDecision};
use crate::messaging::messaging::MessageSender;
use crate::messaging::node_addr::NodeAddr;


/// The [UnreachableTracker] picks up work when nodes are declared 'unreachable', either locally
///  by a reachability decider, or remotely via gossip. It tracks stability of overall node
///  reachability and invokes the [DowningStrategy] to resolve long-term unreachability.
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

                // we want the downing of all nodes to be atomic, so we keep the lock across both calls
                let mut cs_lock = cluster_state.write().await;
                Self::on_downing_decision(cs_lock.deref_mut(), DowningStrategyDecision::DownUs, messaging.as_ref()).await;
                Self::on_downing_decision(cs_lock.deref_mut(), DowningStrategyDecision::DownThem, messaging.as_ref()).await;
            }));
        }
    }

    async fn on_downing_decision<M: MessageSender>(cluster_state: &mut ClusterState, downing_strategy_decision: DowningStrategyDecision, messaging: &M) {
        info!("downing decision: {:?}", downing_strategy_decision);
        let downed_nodes = cluster_state.apply_downing_decision(downing_strategy_decision).await;
        info!("downing nodes {:?}", downed_nodes);
        for n in downed_nodes {
            // This is a best effort to notify all affected nodes of the downing decision.
            //  We cannot reach all nodes anyway, and there may be network problems, so this is
            //  *not* a reliable notification - but it may help in the face of problems
            messaging.send(n, &GossipMessage::DownYourself).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::cluster::cluster_state::MembershipState::Up;
    use crate::cluster::cluster_state::*;
    use crate::cluster::heartbeat::downing_strategy::{DowningStrategyDecision, MockDowningStrategy};
    use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;
    use std::sync::Arc;
    use std::time::Duration;
    use rstest::rstest;
    use tokio::runtime::Builder;
    use tokio::sync::RwLock;
    use tokio::time;
    use crate::cluster::gossip::gossip_messages::GossipMessage;

    #[tokio::test(start_paused = true)]
    async fn test_update_unreachable_set() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));

        let downing_strategy = Arc::new(MockDowningStrategy::new());
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let mut tracker = UnreachableTracker::new(config, cluster_state, downing_strategy);

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
            let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
            let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
            cluster_state.write().await
                .merge_node_state(node_state!(2[]:Up->[3:false@9]@[1,2,3])).await;
            cluster_state.write().await
                .merge_node_state(node_state!(3[]:Up->[]@[1,2,3])).await;

            let mut downing_strategy = MockDowningStrategy::new();
            downing_strategy.expect_decide()
                .once()
                .return_const(decision);
            let downing_strategy = Arc::new(downing_strategy);
            let messaging = Arc::new(TrackingMockMessageSender::new(myself));

            let mut tracker = UnreachableTracker::new(config, cluster_state.clone(), downing_strategy);

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

            for n in expected_downed_nodes {
                let membership_state = cluster_state.read().await
                    .get_node_state(&n).unwrap()
                    .membership_state;
                assert_eq!(membership_state, MembershipState::Down);
                messaging.assert_message_sent(n, GossipMessage::DownYourself).await;
            }

            messaging.assert_no_remaining_messages().await;
        });
    }

    #[tokio::test(start_paused = true)]
    async fn test_unstable_shutdown() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        cluster_state.write().await
            .merge_node_state(node_state!(2[]:Up->[3:false@9]@[1,2,3])).await;
        cluster_state.write().await
            .merge_node_state(node_state!(3[]:Up->[]@[1,2,3])).await;

        let downing_strategy = Arc::new(MockDowningStrategy::new());
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let mut tracker = UnreachableTracker::new(config, cluster_state.clone(), downing_strategy);

        tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        // unstable timeout timer is restarted when all nodes become reachable again
        tracker.update_reachability(test_node_addr_from_number(2), true, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;

        tracker.update_reachability(test_node_addr_from_number(2), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), true, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), false, messaging.clone()).await;
        time::sleep(Duration::from_secs(4)).await;
        tracker.update_reachability(test_node_addr_from_number(3), true, messaging.clone()).await;

        messaging.assert_no_remaining_messages().await;

        time::sleep(Duration::from_millis(4001)).await;

        messaging.assert_message_sent(test_node_addr_from_number(1), GossipMessage::DownYourself).await;
        messaging.assert_message_sent(test_node_addr_from_number(3), GossipMessage::DownYourself).await;
        messaging.assert_message_sent(test_node_addr_from_number(2), GossipMessage::DownYourself).await;
        messaging.assert_no_remaining_messages().await;

        let membership_state = cluster_state.read().await
            .get_node_state(&test_node_addr_from_number(1)).unwrap()
            .membership_state;
        assert_eq!(membership_state, MembershipState::Down);

        let membership_state = cluster_state.read().await
            .get_node_state(&test_node_addr_from_number(2)).unwrap()
            .membership_state;
        assert_eq!(membership_state, MembershipState::Down);

        let membership_state = cluster_state.read().await
            .get_node_state(&test_node_addr_from_number(3)).unwrap()
            .membership_state;
        assert_eq!(membership_state, MembershipState::Down);
    }
}
