use clap::Parser;
use clap_derive::Parser;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Error, Response};
use hyper_util::rt::TokioIo;
use cluster::cluster::cluster::Cluster;
use cluster::cluster::cluster_config::ClusterConfig;
use cluster::cluster::discovery_strategy::SeedNodesStrategy;
use cluster::cluster::heartbeat::downing_strategy::QuorumOfSeedNodesStrategy;
use cluster::messaging::messaging::Messaging;
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::anyhow;
use tokio::net::TcpListener;
use tokio::select;
use tracing::{error, info, Level};

#[derive(Parser)]
struct Args {
    cluster_address: String,
    http_address: String,

    #[clap(long)]
    seed_node: Vec<String>,

    #[clap(short, long, default_value_t = false)]
    verbose: bool,

    #[clap(long, default_value_t = false)]
    very_verbose: bool,
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.seed_node.is_empty() {
        return Err(anyhow!("missing seed nodes"));
    }

    let level = match (args.verbose, args.very_verbose) {
        (_, true) => Level::TRACE,
        (true, _) => Level::DEBUG,
        (false, false) => Level::INFO,
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .try_init()
        .ok();

    let mut seed_nodes = Vec::new();
    for s in args.seed_node {
        let seed_node: SocketAddr = s.parse()?;
        seed_nodes.push(seed_node);
    }

    let cluster_config = ClusterConfig::new(args.cluster_address.parse()?, Some(vec![5u8;32]));

    let cluster_config = Arc::new(cluster_config);
    let cluster = Arc::new(Cluster::new(cluster_config.clone()).await?);

    let http_addr: SocketAddr = args.http_address.parse()?;
    let discovery_strategy = SeedNodesStrategy::new(seed_nodes.clone(), cluster_config.as_ref())?;
    let downing_strategy = QuorumOfSeedNodesStrategy { seed_nodes };

    select! {
        result = cluster.run(discovery_strategy, downing_strategy) => { result }
        result = run_http_server(http_addr, cluster.clone()) => { result }
    }
}


async fn run_http_server<M: Messaging>(addr: SocketAddr, cluster: Arc<Cluster<M>>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on http://{}", addr);
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let cluster = cluster.clone();
        let service = service_fn (move |_req| {
            let cluster = cluster.clone();
            async move {
                Ok::<_, Error>(Response::new(Full::new(Bytes::from(format!(
                    "leader: {:?}\nam_i_leader: {}\nis_converged: {}\nnodes: {:?}",
                    cluster.get_leader().await,
                    cluster.am_i_leader().await,
                    cluster.is_converged().await,
                    cluster.get_nodes().await,
                )))))
            }
        });

        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            error!("Error serving connection: {:?}", err);
        }
    }
}
