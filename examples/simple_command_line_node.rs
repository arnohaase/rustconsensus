use clap::Parser;
use clap_derive::Parser;
use http_body_util::Full;
use hyper::body::{Body, Bytes};
use hyper::server::conn::http1;
use hyper::service::{service_fn, Service};
use hyper::{Error, Response};
use hyper_util::rt::TokioIo;
use rustconsensus::cluster::cluster::Cluster;
use rustconsensus::cluster::cluster_config::ClusterConfig;
use rustconsensus::cluster::discovery_strategy::{JoinOtherNodesStrategy, PartOfSeedNodesStrategy};
use rustconsensus::cluster::heartbeat::downing_strategy::QuorumOfSeedNodesStrategy;
use rustconsensus::messaging::messaging::Messaging;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    let mut cluster_config = ClusterConfig::new(args.cluster_address.parse()?);
    cluster_config.self_addr = args.cluster_address.parse()?;

    let cluster_config = Arc::new(cluster_config);
    let cluster = Arc::new(Cluster::new(cluster_config.clone()).await?);

    let http_addr: SocketAddr = args.http_address.parse()?;
    let discovery_strategy = PartOfSeedNodesStrategy::new(seed_nodes.clone(), cluster_config.as_ref())?;
    let downing_strategy = QuorumOfSeedNodesStrategy { seed_nodes };

    select! {
        result = cluster.run(discovery_strategy, downing_strategy) => { result }
        result = run_http_server(http_addr, cluster.clone()) => { result }
    }
}


async fn run_http_server<M: Messaging>(addr: SocketAddr, cluster: Arc<Cluster<M>>) -> anyhow::Result<()> {
    let counter = Arc::new(AtomicUsize::new(0));

    let listener = TcpListener::bind(addr).await?;
    info!("Listening on http://{}", addr);
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let counter = counter.clone();

        let service = service_fn (move |_req| {
            let count = counter.fetch_add(1, Ordering::AcqRel);
            async move {
                Ok::<_, Error>(Response::new(Full::new(Bytes::from(format!(
                    "Request #{}",
                    count
                )))))
            }
        });

        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            error!("Error serving connection: {:?}", err);
        }
    }
}
