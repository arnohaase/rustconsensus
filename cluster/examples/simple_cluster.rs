use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use tokio::select;
use tracing::{info, Level};
use cluster::cluster::cluster::Cluster;
use cluster::cluster::cluster_config::ClusterConfig;
use cluster::cluster::discovery_strategy::SeedNodesStrategy;
use cluster::cluster::heartbeat::downing_strategy::QuorumOfSeedNodesStrategy;

fn init_logging() {
    tracing_subscriber::fmt()
        // .with_max_level(Level::INFO)
        // .with_max_level(Level::DEBUG)
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

fn addr(n: usize) -> SocketAddr {
    SocketAddr::from_str(&format!("127.0.0.1:{}", 9810+n)).unwrap()
}

#[tracing::instrument(name="Cluster", skip(num_nodes))]
async fn new_node(num_nodes: usize, n: usize) -> anyhow::Result<()> {
    // let config = ClusterConfig::new(addr(n), None);
    let config = ClusterConfig::new(addr(n), Some(b"1234567_1234567_1234567_1234567_".to_vec()));
    let cluster = Cluster::new(Arc::new(config)).await?;

    let seed_nodes = (0..num_nodes).map(|n| addr(n)).collect::<Vec<_>>();

    let discovery_strategy = SeedNodesStrategy::new(seed_nodes.clone(), cluster.config.as_ref())?;

    cluster.run(discovery_strategy, QuorumOfSeedNodesStrategy { seed_nodes }).await
}


#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    init_logging();

    info!("simple cluster demo");

    select! {
        _ = new_node(25, 0) => {}
        _ = new_node(25, 1) => {}
        _ = new_node(25, 2) => {}
        _ = new_node(25, 3) => {}
        _ = new_node(25, 4) => {}
        _ = new_node(25, 5) => {}
        _ = new_node(25, 6) => {}
        _ = new_node(25, 7) => {}
        _ = new_node(25, 8) => {}
        _ = new_node(25, 9) => {}
        _ = new_node(25, 10) => {}
        _ = new_node(25, 11) => {}
        _ = new_node(25, 12) => {}
        _ = new_node(25, 13) => {}
        _ = new_node(25, 14) => {}
        _ = new_node(25, 15) => {}
        _ = new_node(25, 16) => {}
        _ = new_node(25, 17) => {}
        _ = new_node(25, 18) => {}
        _ = new_node(25, 19) => {}
        _ = new_node(25, 20) => {}
        _ = new_node(25, 21) => {}
        _ = new_node(25, 22) => {}
        _ = new_node(25, 23) => {}
        _ = new_node(25, 24) => {}
    }

    Ok(())
}
