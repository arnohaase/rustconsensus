use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;

use tracing::Level;

use rustconsensus::cluster::cluster::Cluster;
use rustconsensus::cluster::cluster_config::ClusterConfig;
use rustconsensus::cluster::cluster_messages::ClusterMessageModule;
use rustconsensus::messaging::messaging::Messaging;
use rustconsensus::messaging::node_addr::NodeAddr;

fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

async fn create_messaging(addr: &str) -> anyhow::Result<Arc<Messaging>> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let messaging = Messaging::new(addr).await?;
    Ok(Arc::new(messaging))
}

async fn run_and_join(cluster: Cluster, other: &str) -> anyhow::Result<()> {
    sleep(Duration::from_millis(100)).await;
    cluster.run(Some(other)).await
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
        _ = cluster1.run(None::<&str>) => {} //TODO type annotation should not be necessary
        _ = run_and_join(cluster2, "127.0.0.1:9810") => {}
    }

    Ok(())
}