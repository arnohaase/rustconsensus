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
        // .with_max_level(Level::INFO)
        .with_max_level(Level::DEBUG)
        // .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

async fn create_messaging(addr: &str) -> anyhow::Result<Arc<Messaging>> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let messaging = Messaging::new(addr).await?;
    Ok(Arc::new(messaging))
}

#[tracing::instrument(name="BBB", skip_all)]
async fn run_and_join(cluster: Cluster) -> anyhow::Result<()> {
    cluster.run(PartOfSeedNodeStrategy::new(vec!["127.0.0.1:9810", "127.0.0.1:9811"]).unwrap()).await
}


#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    init_logging();

    let config1 = ClusterConfig::default();
    let messaging1 = create_messaging("127.0.0.1:9810").await?;
    let cluster1 = Cluster::new(Arc::new(config1), messaging1.clone());

    let config2 = ClusterConfig::default();
    let messaging2 = create_messaging("127.0.0.1:9811").await?;
    let cluster2 = Cluster::new(Arc::new(config2), messaging2.clone());

    select! {
        _ = cluster1.run(PartOfSeedNodeStrategy::new(vec!["127.0.0.1:9810", "127.0.0.1:9811"]).unwrap()) => {}
        _ = run_and_join(cluster2) => {}
    }

    Ok(())
}
