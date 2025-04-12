use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use tokio::select;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::{ClusterState, NodeState};
use crate::cluster::join_messages::JoinMessage;
use crate::messaging::messaging::MessageSender;
use crate::messaging::node_addr::NodeAddr;

//TODO documentation
//TODO unit test

pub async fn run_discovery<M: MessageSender>(discovery_strategy: impl DiscoveryStrategy, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<M>) {
    match discovery_strategy.do_discovery(config, cluster_state, messaging).await {
        Ok(_) => {
            // sleep forever, i.e. until the cluster's regular loop terminates
            loop {
                sleep(Duration::from_secs(10)).await;
            }
        }
        Err(e) => {
            error!("discovery unsuccessful, shutting down: {}", e);
        }
    }
}


/// Strategy for joining an existing cluster on startup. This is more important than it looks at
///  first glance because we want to avoid a split into two clusters due to race conditions on
///  startup.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait DiscoveryStrategy {
    async fn do_discovery<M: MessageSender> (
        &self,
        config: Arc<ClusterConfig>,
        cluster_state: Arc<RwLock<ClusterState>>,
        messaging: Arc<M>,
    ) -> anyhow::Result<()>;
}


pub struct StartAsClusterDiscoveryStrategy {}
impl StartAsClusterDiscoveryStrategy {
    pub fn new() -> StartAsClusterDiscoveryStrategy {
        StartAsClusterDiscoveryStrategy {}
    }
}
#[async_trait]
impl DiscoveryStrategy for StartAsClusterDiscoveryStrategy {
    async fn do_discovery<M: MessageSender>(&self, _config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, _messaging: Arc<M>) -> anyhow::Result<()> {
        cluster_state.write().await
            .promote_myself_to_up().await;
        Ok(())
    }
}

/// This strategy means that there is a fixed list of seed nodes, which is the same across the cluster,
///  and which is used for discovery.
///
/// If 'myself' is one of the seed nodes, it may promote itself to 'Up' once a quorum of seed nodes
///  has joined (if it has the 'smallest' address).
///
/// If 'myself' is not one of the seed nodes, discovery is done once the join request is acknowledged
///  by one of the seed nodes.
pub struct SeedNodesStrategy {
    /// NB: The list of seed nodes is Vec<SocketAddr> and *not* Vec<ToSocketAddrs> to have a well-defined
    ///      number of nodes which can serve as basis for quora decisions
    seed_nodes: Vec<SocketAddr>,
    am_i_seed_node: bool,
}
impl SeedNodesStrategy {
    pub fn new(seed_nodes: Vec<SocketAddr>, config: &ClusterConfig) -> anyhow::Result<SeedNodesStrategy> {
        let am_i_seed_node = seed_nodes.contains(&config.self_addr());

        if am_i_seed_node {
            if let Some(leader_roles) = &config.leader_eligible_roles {
                if !config.roles.iter().any(|r| leader_roles.contains(r)) {
                    return Err(anyhow!("none of this role's roles {:?} make it eligible for leadership: one of {:?} is needed", config.roles, leader_roles));
                }
            }
        }

        Ok(SeedNodesStrategy {
            seed_nodes,
            am_i_seed_node,
        })
    }
}
#[async_trait]
impl DiscoveryStrategy for SeedNodesStrategy {
    async fn do_discovery<M: MessageSender>(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<M>) -> anyhow::Result<()> {
        let myself = cluster_state.read().await.myself();

        if self.am_i_seed_node {
            let other_seed_nodes = self.seed_nodes.iter()
                .filter(|&&n| n != myself.socket_addr)
                .cloned()
                .collect::<Vec<_>>();

            select! {
                _ = send_join_message_loop(&other_seed_nodes, messaging.clone(), config.clone()) => { Ok(()) }
                _ = check_joined_as_seed_node(cluster_state.clone(), config.clone(), self.seed_nodes.clone(), myself) => { Ok(()) }
                _ = sleep(config.discovery_seed_node_give_up_timeout) => {
                    error!("discovery of seed nodes timed out, no quorum of seed nodes reached: giving up");
                    Err(anyhow!("discovery of seed nodes timed out, no quorum of seed nodes reached: giving up"))
                }
            }
        }
        else {
            select! {
                _ = send_join_message_loop(&self.seed_nodes, messaging.clone(), config.clone()) => { Ok(()) }
                _ = check_joined_other_seed_nodes(cluster_state.clone(), &self.seed_nodes) => { Ok(()) }
                _ = sleep(config.discovery_seed_node_give_up_timeout) => {
                    error!("discovery of seed nodes timed out, giving up");
                    Err(anyhow!("discovery of seed nodes timed out, giving up"))
                }
            }
        }

    }
}

/// wait until one of two conditions happen:
/// * A quorum of seed nodes has joined, I am the first of these, and cluster state has converged
///     --> promote myself to 'Up' to allow becoming the leader and bootstrap the cluster
/// * Some other node has become leader eligible
#[instrument(level = "trace", skip_all)]
async fn check_joined_as_seed_node(cluster_state: Arc<RwLock<ClusterState>>, config: Arc<ClusterConfig>, seed_nodes: Vec<SocketAddr>, myself: NodeAddr) {
    loop {
        if is_any_node_leader_eligible(config.as_ref(), cluster_state.read().await.node_states()) {
            //TODO add message for 'joining a cluster' (e.g. before  any node is up)
            info!("joined a cluster"); //TODO better log message - this may be long after the initial 'joining'
            break;
        }

        let seed_node_members = seed_node_members(cluster_state.read().await.node_states(), &seed_nodes);
        let has_quorum = seed_node_members.len() * 2 > seed_nodes.len();
        let i_am_first = *seed_node_members.iter().min().unwrap() == myself;

        //TODO documentation
        if has_quorum && i_am_first && cluster_state.read().await.is_converged() {
            cluster_state.write().await
                .promote_myself_to_up().await;
            info!("a quorum of seed nodes joined, promoting myself to leader");
            break;
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn check_joined_other_seed_nodes(cluster_state: Arc<RwLock<ClusterState>>, seed_nodes: &[SocketAddr]) {
    loop {
        if cluster_state.read().await
            .node_states()
            .any(|n| seed_nodes.contains(&n.addr.socket_addr))
        {
            // Seeing one of the seed nodes in our own cluster state means that someone gossipped it
            //  back - and since we didn't publish our address except by sending Join messages to
            //  seed nodes, that meant that some seed nodes are now aware of us. That takes care
            //  of discovery, and regular gossip should do the rest.

            info!("discovery successful, joined the cluster");
            break
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn send_join_message_loop<M: MessageSender>(other_seed_nodes: &[SocketAddr], messaging: Arc<M>, config: Arc<ClusterConfig>) {
    let join_msg = JoinMessage::Join{ roles: config.roles.clone(), };

    //NB: This endless loop *must* be in a separate function rather than inlined in the select! block
    //     due to limitations in the select! macro / rewriting of awaits
    loop {
        for seed_node in other_seed_nodes {
            debug!("trying to join cluster at {}", seed_node); //TODO clearer logging
            messaging.send_raw_fire_and_forget(seed_node.clone(), None, &join_msg).await;
        }
        sleep(config.discovery_seed_node_retry_interval).await;
    }
}

fn is_any_node_leader_eligible<'a>(config: &ClusterConfig, mut nodes: impl Iterator<Item=&'a NodeState>) -> bool {
    nodes.any(|n| n.is_leader_eligible(config))
}

fn seed_node_members<'a>(all_nodes: impl Iterator<Item=&'a NodeState>, seed_nodes: &[SocketAddr]) -> Vec<NodeAddr> {
    all_nodes
        .filter(|n| seed_nodes.contains(&n.addr.socket_addr))
        .map(|n| n.addr)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::cluster::cluster_state::*;
    use crate::messaging::messaging::MockMessageSender;
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;
    use rstest::rstest;
    use tokio::time;
    use MembershipState::*;

    #[tokio::test]
    async fn test_run_discovery() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));

        let cluster_state = ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()));
        let cluster_state = Arc::new(RwLock::new(cluster_state));
        let cluster_state_for_check = cluster_state.clone();

        let messaging = Arc::new(TrackingMockMessageSender::new(myself));
        let messaging_for_check = messaging.clone();

        let mut mock = MockDiscoveryStrategy::new();
        mock.expect_do_discovery()
            .times(1)
            .withf(move |_, s, _: &Arc<TrackingMockMessageSender>| std::ptr::addr_eq(Arc::as_ptr(s), Arc::as_ptr(&cluster_state_for_check)))
            .withf(move |_, _, m| std::ptr::addr_eq(Arc::as_ptr(m), Arc::as_ptr(&messaging_for_check)))
            .returning_st(|_a, _b, _c| Ok(()));

        time::pause();

        let handle = tokio::spawn(run_discovery(mock, config, cluster_state.clone(), messaging));

        time::advance(Duration::from_secs(9999999999)).await;
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[tokio::test]
    async fn test_start_as_cluster_strategy() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let cluster_state = ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()));
        let cluster_state = Arc::new(RwLock::new(cluster_state));

        let mut message_sender = MockMessageSender::new();
        message_sender.expect_send_raw_fire_and_forget::<JoinMessage>()
            .never();

        let discovery_result = StartAsClusterDiscoveryStrategy{}
            .do_discovery(config, cluster_state.clone(), Arc::new(message_sender)).await;

        assert!(discovery_result.is_ok());
        assert_eq!(cluster_state.read().await.get_node_state(&myself).unwrap().membership_state, MembershipState::Up);
        assert!(cluster_state.write().await.am_i_leader());
    }

    #[tokio::test]
    async fn test_part_of_seed_nodes_strategy_join_loop() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles = ["xyz".to_string()].into();
        let config = Arc::new(config);
        let strategy = SeedNodesStrategy::new(vec![
            myself.socket_addr,
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ], config.as_ref()).unwrap();

        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        time::pause();

        {
            let config = config.clone();
            let cluster_state = cluster_state.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                strategy.do_discovery(config, cluster_state, messaging).await
            })
        };

        for _ in 0..20 {
            sleep(config.discovery_seed_node_retry_interval).await;

            messaging.assert_message_sent(test_node_addr_from_number(2), JoinMessage::Join { roles: config.roles.clone() }).await;
            messaging.assert_message_sent(test_node_addr_from_number(3), JoinMessage::Join { roles: config.roles.clone() }).await;
            messaging.assert_no_remaining_messages().await;
        }
    }

    #[tokio::test]
    async fn test_part_of_seed_nodes_strategy_promote_self() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles = ["xyz".to_string()].into();
        let config = Arc::new(config);
        let strategy = SeedNodesStrategy::new(vec![
            myself.socket_addr,
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ], config.as_ref()).unwrap();

        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        time::pause();

        let handle = {
            let config = config.clone();
            let cluster_state = cluster_state.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                strategy.do_discovery(config, cluster_state, messaging).await
            })
        };

        for _ in 0..20 {
            sleep(config.discovery_seed_node_retry_interval).await;

            messaging.assert_message_sent(test_node_addr_from_number(2), JoinMessage::Join { roles: config.roles.clone() }).await;
            messaging.assert_message_sent(test_node_addr_from_number(3), JoinMessage::Join { roles: config.roles.clone() }).await;
            messaging.assert_no_remaining_messages().await;
        }

        cluster_state.write().await
            .merge_node_state(node_state!(1["xyz"]:Joining->[]@[1,2])).await;
        cluster_state.write().await
            .merge_node_state(node_state!(2[]:Joining->[]@[1,2])).await;

        sleep(Duration::from_millis(10)).await;
        assert!(handle.is_finished());
        assert!(handle.await.is_ok());
        assert_eq!(cluster_state.read().await
            .get_node_state(&myself).unwrap().membership_state, Up);
    }

    #[tokio::test]
    async fn test_part_of_seed_nodes_strategy_timeout() {
        time::pause();

        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles = ["xyz".to_string()].into();
        config.discovery_seed_node_give_up_timeout = Duration::from_millis(300);
        config.discovery_seed_node_retry_interval = Duration::from_millis(50);
        let config = Arc::new(config);
        let strategy = SeedNodesStrategy::new(vec![
            myself.socket_addr,
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ], config.as_ref()).unwrap();

        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let handle = {
            let config = config.clone();
            let cluster_state = cluster_state.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                strategy.do_discovery(config, cluster_state, messaging).await
            })
        };

        sleep(config.discovery_seed_node_give_up_timeout + Duration::from_secs(1)).await;
        assert!(handle.is_finished());
        assert!(handle.await.unwrap().is_err());
        assert_eq!(cluster_state.read().await
            .get_node_state(&myself).unwrap().membership_state, Joining);
    }

    #[rstest]
    #[case::simple(vec![1,2], vec![], vec![], true)]
    #[case::self_leader_role(vec![1,2], vec!["a"], vec!["a"], true)]
    #[case::self_leader_role_2(vec![1,2], vec!["a"], vec!["a", "b"], true)]
    #[case::self_not_leader_role(vec![1,2], vec!["a"], vec![], false)]
    #[case::self_not_leader_role_2(vec![1,2], vec!["a"], vec!["b"], false)]
    #[case::several_leader_roles_1(vec![1,2], vec!["a","b"], vec!["a"], true)]
    #[case::several_leader_roles_1_plus(vec![1,2], vec!["a","b"], vec!["a", "x"], true)]
    #[case::several_leader_roles_2(vec![1,2], vec!["a","b"], vec!["b"], true)]
    #[case::several_leader_roles_2_plus(vec![1,2], vec!["a","b"], vec!["b", "x"], true)]
    #[case::several_leader_roles_both(vec![1,2], vec!["a","b"], vec!["a", "b"], true)]
    #[case::several_leader_roles_both_plus(vec![1,2], vec!["a","b"], vec!["a", "b", "x"], true)]
    #[case::several_leader_roles_neither(vec![1,2], vec!["a","b"], vec![], false)]
    fn test_part_of_seed_nodes_strategy_new(#[case] seed_nodes: Vec<u16>, #[case] leader_roles: Vec<&str>, #[case] self_roles: Vec<&str>, #[case] expected: bool) {
        let mut config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr, None);

        if leader_roles.len() > 0 {
            config.leader_eligible_roles = Some(
                leader_roles.into_iter()
                    .map(|role| role.to_string())
                    .collect()
            );
        }
        config.roles = self_roles.into_iter()
            .map(|role| role.to_string())
            .collect();

        let seed_nodes = seed_nodes.into_iter()
            .map(|n| test_node_addr_from_number(n).socket_addr)
            .collect::<Vec<_>>();

        assert_eq!(SeedNodesStrategy::new(seed_nodes, &config).is_ok(), expected);
    }

    #[rstest]
    #[case(true,  true,  true,  true)]
    #[case(true,  true,  false, false)]
    #[case(true,  false, true,  false)]
    #[case(true,  false, false, false)]
    #[case(false, true,  true,  false)]
    #[case(false, true,  false, false)]
    #[case(false, false, true,  false)]
    #[case(false, false, false, false)]
    fn test_check_joined_as_seed_node_promote_self(#[case] has_quorum: bool, #[case] is_first: bool, #[case] is_converged: bool, #[case] expected: bool) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let myself_num = if is_first { 1 } else { 2 };

            let myself = test_node_addr_from_number(myself_num);
            let other_seed = test_node_addr_from_number(3 - myself_num);

            let seed_nodes = [test_node_addr_from_number(1).socket_addr, test_node_addr_from_number(2).socket_addr, test_node_addr_from_number(3).socket_addr].to_vec();

            let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
            let cluster_state = ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()));
            let cluster_state = Arc::new(RwLock::new(cluster_state));

            time::pause();

            let join_handle = tokio::spawn(check_joined_as_seed_node(cluster_state.clone(), config, seed_nodes, myself));

            sleep(Duration::from_millis(10)).await;
            assert!(!join_handle.is_finished());

            let mut node_state_template = node_state!(4[]:Joining->[]@[0]);
            if is_converged {
                node_state_template.seen_by.insert(test_node_addr_from_number(4));
            }
            if has_quorum {
                node_state_template.seen_by.insert(other_seed);
            }

            // add some non-seed nodes to verify that quorum is counted on seed nodes only, and that
            //  'myself' needs to be the first of the seed nodes, not all nodes
            node_state_template.addr = test_node_addr_from_number(0);
            cluster_state.write().await
                .merge_node_state(node_state_template.clone()).await;
            node_state_template.addr = test_node_addr_from_number(4);
            cluster_state.write().await
                .merge_node_state(node_state_template.clone()).await;

            sleep(Duration::from_millis(10)).await;
            assert!(!join_handle.is_finished());

            if has_quorum {
                node_state_template.addr = other_seed;
                cluster_state.write().await
                    .merge_node_state(node_state_template.clone()).await;

                sleep(Duration::from_millis(10)).await;
                assert!(!join_handle.is_finished());
            }

            node_state_template.addr = myself;
            cluster_state.write().await
                .merge_node_state(node_state_template.clone()).await;

            sleep(Duration::from_millis(10)).await;
            assert_eq!(join_handle.is_finished(), expected);
            if expected {
                let self_state = cluster_state.read().await
                    .get_node_state(&myself).unwrap()
                    .membership_state;

                assert_eq!(self_state, MembershipState::Up);
            }
        });
    }

    #[tokio::test]
    async fn test_check_joined_as_seed_node_other_leader() {
        let myself = test_node_addr_from_number(1);
        let seed_nodes = [myself.socket_addr, test_node_addr_from_number(2).socket_addr, test_node_addr_from_number(3).socket_addr].to_vec();

        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let cluster_state = ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()));
        let cluster_state = Arc::new(RwLock::new(cluster_state));

        time::pause();

        let join_handle = tokio::spawn(check_joined_as_seed_node(cluster_state.clone(), config, seed_nodes, myself));

        sleep(Duration::from_millis(300)).await;
        assert!(!join_handle.is_finished());

        // add some other node that is Up - NB: we do not need to wait for convergence
        cluster_state.write().await
            .merge_node_state(node_state!(2[]:Up->[]@[2])).await;

        sleep(Duration::from_millis(10)).await;
        assert!(join_handle.is_finished());
    }

    #[tokio::test]
    async fn test_send_join_message_loop() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles.insert("abc".to_string());
        let config = Arc::new(config);

        let other_seed_nodes = vec![
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ];

        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        time::pause();

        {
            let messaging = messaging.clone();
            let config = config.clone();
            tokio::spawn(async move {
                send_join_message_loop(&other_seed_nodes, messaging, config).await;
            });
        }

        for _ in 0..500 {
            sleep(config.discovery_seed_node_retry_interval).await;
            messaging.assert_message_sent(test_node_addr_from_number(2), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_message_sent(test_node_addr_from_number(3), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_no_remaining_messages().await;
        }
    }

    #[rstest]
    #[case::empty(vec![], false)]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], true)]
    #[case::up_unreachable(vec![node_state!(1[]:Up->[2:false@5]@[1])], true)]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], false)]
    #[case::weakly_up(vec![node_state!(1[]:WeaklyUp->[]@[1])], false)]
    #[case::leaving(vec![node_state!(1[]:Leaving->[]@[1])], true)]
    #[case::exiting(vec![node_state!(1[]:Exiting->[]@[1])], true)]
    #[case::down(vec![node_state!(1[]:Down->[]@[1])], false)]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[1])], false)]
    #[case::multiple(vec![node_state!(1[]:Joining->[]@[1,2,3]), node_state!(2[]:Up->[]@[1,2,3]), node_state!(3[]:WeaklyUp->[]@[1,2,3])], true)]
    fn test_is_any_leader_eligible(#[case] nodes: Vec<NodeState>, #[case] expected: bool) {
        let config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr, None);
        assert_eq!(is_any_node_leader_eligible(&config, nodes.iter()), expected);
    }

    #[rstest]
    #[case::empty(vec![], vec![], vec![])]
    #[case::up(vec![node_state!(1[]:Up->[]@[1])], vec![1,2,3], vec![1])]
    #[case::joining(vec![node_state!(1[]:Joining->[]@[1])], vec![1,2,3], vec![1])]
    #[case::down(vec![node_state!(1[]:Down->[]@[1])], vec![1,2,3], vec![1])]
    #[case::removed(vec![node_state!(1[]:Removed->[]@[1])], vec![1,2,3], vec![1])]
    #[case::filtering(vec![node_state!(1[]:Up->[]@[1]), node_state!(2[]:Up->[]@[1]), node_state!(5[]:Up->[]@[1])], vec![1,2,3], vec![1,2])]
    fn test_seed_node_members(#[case] nodes: Vec<NodeState>, #[case] seed_nodes: Vec<u16>, #[case] expected: Vec<u16>) {
        let seed_nodes = seed_nodes.into_iter()
            .map(|n| test_node_addr_from_number(n).socket_addr)
            .collect::<Vec<_>>();

        let expected = expected.into_iter()
            .map(|n| test_node_addr_from_number(n))
            .collect::<Vec<_>>();

        assert_eq!(seed_node_members(nodes.iter(), &seed_nodes), expected);
    }

    #[tokio::test]
    async fn test_join_others_strategy_success() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles.insert("abc".to_string());
        let config = Arc::new(config);

        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));

        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let strategy = SeedNodesStrategy::new(vec![
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ], config.as_ref()).unwrap();

        time::pause();

        let handle = {
            let config = config.clone();
            let cluster_state = cluster_state.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                strategy.do_discovery(config, cluster_state, messaging).await
            })
        };

        for _ in 0..10 {
            sleep(config.discovery_seed_node_retry_interval).await;
            messaging.assert_message_sent(test_node_addr_from_number(2), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_message_sent(test_node_addr_from_number(3), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_no_remaining_messages().await;
        }
        assert!(!handle.is_finished());

        cluster_state.write().await
            .merge_node_state(node_state!(4[]:Joining->[]@[1,2,3,4])).await; // not a seed node

        for _ in 0..10 {
            sleep(config.discovery_seed_node_retry_interval).await;
            messaging.assert_message_sent(test_node_addr_from_number(2), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_message_sent(test_node_addr_from_number(3), JoinMessage::Join { roles: ["abc".to_string()].into() }).await;
            messaging.assert_no_remaining_messages().await;
        }
        assert!(!handle.is_finished());

        cluster_state.write().await
            .merge_node_state(node_state!(3[]:Joining->[]@[1,2,3,4])).await; // Joining is enough

        sleep(Duration::from_millis(10)).await;
        assert!(handle.is_finished());
        assert!(handle.await.is_ok());
    }

    #[tokio::test]
    async fn test_join_others_strategy_timeout() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.roles.insert("abc".to_string());
        let config = Arc::new(config);

        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));

        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        let strategy = SeedNodesStrategy::new(vec![
            test_node_addr_from_number(2).socket_addr,
            test_node_addr_from_number(3).socket_addr,
        ], config.as_ref()).unwrap();

        time::pause();

        let handle = {
            let config = config.clone();
            let cluster_state = cluster_state.clone();
            let messaging = messaging.clone();
            tokio::spawn(async move {
                strategy.do_discovery(config, cluster_state, messaging).await
            })
        };

        sleep(config.discovery_seed_node_give_up_timeout + Duration::from_secs(1)).await;

        assert!(handle.is_finished());
        assert!(handle.await.unwrap().is_err());
    }
}
