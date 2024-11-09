use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use anyhow::anyhow;

use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::info;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::{ClusterState, MembershipState, NodeState};
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::cluster::join_messages::JoinMessage;
use crate::messaging::messaging::{JOIN_MESSAGE_MODULE_ID, Messaging};
use crate::messaging::node_addr::NodeAddr;

///TODO documentation



pub fn create_join_seed_nodes_strategy(seed_nodes: impl ToSocketAddrs) -> anyhow::Result<impl DiscoveryStrategy> {
    let seed_nodes = seed_nodes.to_socket_addrs()?.collect::<Vec<_>>();
    Ok(JoinOthersStrategy {
        seed_nodes,
    })
}


/// Strategy for joining an existing cluster on startup. This is more important than it looks at
///  first glance because we want to avoid a split into two clusters due to race conditions on
///  startup.
#[async_trait]
pub trait DiscoveryStrategy {
    async fn do_discovery(
        &self,
        config: Arc<ClusterConfig>,
        cluster_state: Arc<RwLock<ClusterState>>,
        messaging: Arc<Messaging>,
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
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        cluster_state.write().await
            .promote_myself_to_up();
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
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        let myself = cluster_state.read().await.myself();
        let other_seed_nodes = self.seed_nodes.iter()
            .filter(|&&n| n != myself.addr)
            .cloned()
            .collect::<Vec<_>>();

        if other_seed_nodes.len() == self.seed_nodes.len() {
            return Err(anyhow!("list of seed nodes {:?} does not contain this node's address {:?}", self.seed_nodes, myself));
        }

        let mut join_msg_buf = BytesMut::new();
        JoinMessage::Join.ser(&mut join_msg_buf);

        for _ in 0..10 {
            if is_any_node_up(cluster_state.read().await.node_states()) {
                info!("joined existing cluster");
                return Ok(())
            }

            let seed_node_members = seed_node_members(cluster_state.read().await.node_states(), &self.seed_nodes);
            let has_quorum = seed_node_members.len() * 2 > self.seed_nodes.len();
            let i_am_first = seed_node_members.iter().min().unwrap() == &myself;

            //TODO documentation
            if has_quorum && i_am_first && cluster_state.read().await.is_converged() {
                cluster_state.write().await
                    .promote_myself_to_up().await;
                info!("a quorum of seed nodes joined, promoting myself to leader");
                return Ok(())
            }

            for seed_node in &other_seed_nodes {
                info!("trying to join cluster at {}", seed_node);
                let _ = messaging.send(seed_node.clone().into(), JOIN_MESSAGE_MODULE_ID, &join_msg_buf).await;
            }
            sleep(Duration::from_secs(1)).await; //TODO configurable
        }

        //TODO retry loop with configurable delay, until configurable timeout

        Err(anyhow!("could not reach any seed nodes in 10 seconds"))
    }
}

fn is_any_node_up<'a>(mut nodes: impl Iterator<Item=&'a NodeState>) -> bool {
    nodes.any(|n| n.membership_state >= MembershipState::Up)
}

fn seed_node_members<'a>(mut all_nodes: impl Iterator<Item=&'a NodeState>, seed_nodes: &[SocketAddr]) -> Vec<NodeAddr> {
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
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        let myself = cluster_state.read().await.myself().addr;
        if self.seed_nodes.contains(&myself) {
            return Err(anyhow!("this node's address {:?} is listed as one of the seed nodes {:?} although the strategy is meant for cases where it isn't", myself, self.seed_nodes));
        }

        let mut join_msg_buf = BytesMut::new();
        JoinMessage::Join.ser(&mut join_msg_buf);

        for _ in 0..10 {
            if cluster_state.read().await
                .node_states()
                .any(|n| self.seed_nodes.contains(&n.addr.addr))
            {
                info!("discovery successful");
                return Ok(())
            }

            for seed_node in &self.seed_nodes {
                info!("trying to join cluster at {}", seed_node);
                let _ = messaging.send(seed_node.clone().into(), JOIN_MESSAGE_MODULE_ID, &join_msg_buf).await;
            }
            sleep(Duration::from_secs(1)).await;
        }

        //TODO retry loop with configurable delay, until configurable timeout

        Err(anyhow!("could not reach any seed nodes in 10 seconds"))
    }
}

