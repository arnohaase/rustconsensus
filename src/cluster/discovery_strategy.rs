use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
#[cfg(test)] use mockall::automock;
use tokio::select;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::{ClusterState, MembershipState, NodeState};
use crate::cluster::join_messages::JoinMessage;
use crate::messaging::messaging::{Messaging, JOIN_MESSAGE_MODULE_ID};
use crate::messaging::node_addr::NodeAddr;

//TODO documentation
//TODO unit test

pub async fn run_discovery(discovery_strategy: impl DiscoveryStrategy, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<dyn Messaging>) {
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


pub fn create_join_seed_nodes_strategy(seed_nodes: impl ToSocketAddrs) -> anyhow::Result<impl DiscoveryStrategy> {
    let seed_nodes = seed_nodes.to_socket_addrs()?.collect::<Vec<_>>();
    Ok(JoinOthersStrategy {
        seed_nodes,
    })
}


/// Strategy for joining an existing cluster on startup. This is more important than it looks at
///  first glance because we want to avoid a split into two clusters due to race conditions on
///  startup.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait DiscoveryStrategy {
    async fn do_discovery(
        &self,
        config: Arc<ClusterConfig>,
        cluster_state: Arc<RwLock<ClusterState>>,
        messaging: Arc<dyn Messaging>,
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
    async fn do_discovery(&self, _config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, _messaging: Arc<dyn Messaging>) -> anyhow::Result<()> {
        cluster_state.write().await
            .promote_myself_to_up().await;
        Ok(())
    }
}


pub struct PartOfSeedNodeStrategy {
    seed_nodes: Vec<SocketAddr>,
}
impl PartOfSeedNodeStrategy {
    pub fn new(seed_nodes: Vec<impl ToSocketAddrs>) -> anyhow::Result<PartOfSeedNodeStrategy> {
        let mut resolved_nodes = Vec::new();
        for tsa in seed_nodes {
            for sa in tsa.to_socket_addrs()? {
                resolved_nodes.push(sa);
            }
        }
        Ok(PartOfSeedNodeStrategy {
            seed_nodes: resolved_nodes,
        })
    }
}
#[async_trait]
impl DiscoveryStrategy for PartOfSeedNodeStrategy {
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<dyn Messaging>) -> anyhow::Result<()> {
        let myself = cluster_state.read().await.myself();
        let other_seed_nodes = self.seed_nodes.iter()
            .filter(|&&n| n != myself.addr)
            .cloned()
            .collect::<Vec<_>>();

        if other_seed_nodes.len() == self.seed_nodes.len() {
            return Err(anyhow!("list of seed nodes {:?} does not contain this node's address {:?}", self.seed_nodes, myself));
        }

        select! {
            _ = send_join_message_loop(&other_seed_nodes, messaging.clone(), config.clone()) => { Ok(()) }
            _ = check_joined_as_seed_node(cluster_state.clone(), &self.seed_nodes, &myself) => { Ok(()) }
            _ = sleep(config.discovery_seed_node_give_up_timeout) => { Err(anyhow!("discovery timeout")) } //TODO better message; logging
        }
    }
}

async fn check_joined_as_seed_node(cluster_state: Arc<RwLock<ClusterState>>, seed_nodes: &[SocketAddr], myself: &NodeAddr) {
    loop {
        if is_any_node_up(cluster_state.read().await.node_states()) {
            //TODO add message for 'joining a cluster' (e.g. before  any node is up)
            info!("joined a cluster"); //TODO better log message - this may be long after the initial 'joining'
            break;
        }

        let seed_node_members = seed_node_members(cluster_state.read().await.node_states(), seed_nodes);
        let has_quorum = seed_node_members.len() * 2 > seed_nodes.len();
        let i_am_first = seed_node_members.iter().min().unwrap() == myself;

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
            .any(|n| seed_nodes.contains(&n.addr.addr))
        {
            info!("discovery successful, joined the cluster");
            break
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn send_join_message_loop(other_seed_nodes: &[SocketAddr], messaging: Arc<dyn Messaging>, config: Arc<ClusterConfig>) {
    let mut join_msg_buf = BytesMut::new();
    JoinMessage::Join{ roles: config.roles.clone(), }
        .ser(&mut join_msg_buf);

    //NB: This endless loop *must* be in a separate function rather than inlined in the select! block
    //     due to limitations in the select! macro / rewriting of awaits
    loop {
        for seed_node in other_seed_nodes {
            debug!("trying to join cluster at {}", seed_node); //TODO clearer logging
            let _ = messaging.send(seed_node.clone().into(), JOIN_MESSAGE_MODULE_ID, &join_msg_buf).await;
        }
        sleep(config.discovery_seed_node_retry_interval).await;
    }
}

fn is_any_node_up<'a>(mut nodes: impl Iterator<Item=&'a NodeState>) -> bool {
    nodes.any(|n| n.membership_state >= MembershipState::Up)
}

fn seed_node_members<'a>(all_nodes: impl Iterator<Item=&'a NodeState>, seed_nodes: &[SocketAddr]) -> Vec<NodeAddr> {
    all_nodes
        .filter(|n| seed_nodes.contains(&n.addr.addr))
        .map(|n| n.addr)
        .collect()
}

pub struct JoinOthersStrategy {
    seed_nodes: Vec<SocketAddr>,
}
impl JoinOthersStrategy {
    pub fn new(seed_nodes: Vec<impl ToSocketAddrs>) -> anyhow::Result<JoinOthersStrategy> {
        let mut resolved_nodes = Vec::new();
        for tsa in seed_nodes {
            for sa in tsa.to_socket_addrs()? {
                resolved_nodes.push(sa);
            }
        }
        Ok(JoinOthersStrategy {
            seed_nodes: resolved_nodes,
        })
    }
}
#[async_trait]
impl DiscoveryStrategy for JoinOthersStrategy {
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<dyn Messaging>) -> anyhow::Result<()> {
        let myself = cluster_state.read().await.myself().addr;
        if self.seed_nodes.contains(&myself) {
            return Err(anyhow!("this node's address {:?} is listed as one of the seed nodes {:?} although the strategy is meant for cases where it isn't", myself, self.seed_nodes));
        }

        select! {
            _ = send_join_message_loop(&self.seed_nodes, messaging.clone(), config.clone()) => { Ok(()) }
            _ = check_joined_other_seed_nodes(cluster_state.clone(), &self.seed_nodes) => { Ok(()) }
            _ = sleep(config.discovery_seed_node_give_up_timeout) => { Err(anyhow!("discovery timeout")) } //TODO better message; logging
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::time;
    use super::*;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::messaging::messaging::MockMessaging;
    use crate::test_util::test_node_addr_from_number;

    #[tokio::test]
    async fn test_run_discovery() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.addr));

        //TODO mock cluster state

        let cluster_state = ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()));
        let cluster_state = Arc::new(RwLock::new(cluster_state));
        let cluster_state_for_check = cluster_state.clone();

        let messaging = Arc::new(MockMessaging::new());
        let messaging_for_check = messaging.clone();

        let mut mock = MockDiscoveryStrategy::new();
        mock.expect_do_discovery()
            .times(1)
            .withf(move |_, s, _| std::ptr::addr_eq(Arc::as_ptr(s), Arc::as_ptr(&cluster_state_for_check)))
            .withf(move |_, _, m| std::ptr::addr_eq(Arc::as_ptr(m), Arc::as_ptr(&messaging_for_check)))
            .returning_st(|_a, _b, _c| Ok(()));

        time::pause();

        let handle = tokio::spawn(run_discovery(mock, config, cluster_state.clone(), messaging));

        time::advance(Duration::from_secs(9999999999)).await;
        assert!(!handle.is_finished());
        handle.abort();
    }
}