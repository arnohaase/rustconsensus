use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use tokio::select;
use tracing::Level;

use rustconsensus::cluster::cluster::Cluster;
use rustconsensus::cluster::cluster_config::ClusterConfig;
use rustconsensus::cluster::discovery_strategy::PartOfSeedNodeStrategy;
use rustconsensus::messaging::messaging::Messaging;
use rustconsensus::messaging::node_addr::NodeAddr;

fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        // .with_max_level(Level::DEBUG)
        // .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

fn addr(n: usize) -> SocketAddr {
    SocketAddr::from_str(&format!("127.0.0.1:{}", 9810+n)).unwrap()
}

#[tracing::instrument(name="Cluster", skip(num_nodes))]
async fn new_node(num_nodes: usize, n: usize) -> anyhow::Result<()> {
    let messaging = Arc::new(Messaging::new(addr(n).into()).await?);
    let config = Arc::new(ClusterConfig::default());
    let cluster = Cluster::new(config, messaging);

    let discovery_strategy = PartOfSeedNodeStrategy::new((0..num_nodes).map(|n| addr(n)).collect())?;

    cluster.run(discovery_strategy).await
}


#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    init_logging();

    select! {
        _ = new_node(7, 0) => {}
        _ = new_node(7, 1) => {}
        _ = new_node(7, 2) => {}
        _ = new_node(7, 3) => {}
        _ = new_node(7, 4) => {}
        _ = new_node(7, 5) => {}
        _ = new_node(7, 6) => {}
    }

    Ok(())
}
