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
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::cluster::join_messages::JoinMessage;
use crate::messaging::messaging::{JOIN_MESSAGE_MODULE_ID, Messaging};

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


pub struct JoinMyselfDiscoveryStrategy {}
impl JoinMyselfDiscoveryStrategy {
    pub fn new() -> JoinMyselfDiscoveryStrategy {
        JoinMyselfDiscoveryStrategy{}
    }
}
#[async_trait]
impl DiscoveryStrategy for JoinMyselfDiscoveryStrategy {
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        cluster_state.write().await
            .promote_myself_to_up();
        Ok(())
    }
}


pub struct MyselfIsSeedNodeStrategy {
    seed_nodes: Vec<SocketAddr>,
}
impl MyselfIsSeedNodeStrategy {
    pub fn new(seed_nodes: impl ToSocketAddrs) -> anyhow::Result<MyselfIsSeedNodeStrategy> {
        let seed_nodes = seed_nodes.to_socket_addrs()?.collect::<Vec<_>>();
        Ok(MyselfIsSeedNodeStrategy {
            seed_nodes,
        })
    }
}
#[async_trait]
impl DiscoveryStrategy for MyselfIsSeedNodeStrategy {
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
            if cluster_state.read().await
                .node_states()
                .any(|n| other_seed_nodes.contains(&n.addr.addr))
            {
                info!("discovery successful");
                return Ok(())
            }

            for seed_node in &other_seed_nodes {
                info!("trying to join cluster at {}", seed_node);
                let _ = messaging.send(seed_node.clone().into(), JOIN_MESSAGE_MODULE_ID, &join_msg_buf).await;
            }
            sleep(Duration::from_secs(1)).await;
        }

        //TODO retry loop with configurable delay, until configurable timeout

        Err(anyhow!("could not reach any seed nodes in 10 seconds"))
    }
}


pub struct JoinOthersStrategy {
    seed_nodes: Vec<SocketAddr>,
}
impl JoinOthersStrategy {
    pub fn new(seed_nodes: impl ToSocketAddrs) -> anyhow::Result<JoinOthersStrategy> {
        let seed_nodes = seed_nodes.to_socket_addrs()?.collect::<Vec<_>>();
        Ok(JoinOthersStrategy {
            seed_nodes,
        })
    }
}
#[async_trait]
impl DiscoveryStrategy for JoinOthersStrategy {
    async fn do_discovery(&self, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        //TODO check that myself is not one of the seed nodes

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

