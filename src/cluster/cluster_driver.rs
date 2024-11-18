use std::sync::Arc;
use std::time::Duration;
use tokio::select;

use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::discovery_strategy::DiscoveryStrategy;
use crate::cluster::gossip::run_gossip;
use crate::cluster::heartbeat::run_heartbeat;
use crate::messaging::messaging::Messaging;

//TODO documentation
pub async fn run_cluster(
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
    messaging: Arc<Messaging>,
    discovery_strategy: impl DiscoveryStrategy,
) {
    //TODO termination / shutdown

    select! {
        _ = _do_discovery(discovery_strategy, config.clone(), cluster_state.clone(), messaging.clone()) => { }
        _ = _run_active_loop(config.clone(), cluster_state.clone()) => { }
        result = run_gossip(config.clone(), messaging.clone(), cluster_state.clone()) => {
            if let Err(err) = result {
                error!("error running gossip - shutting down: {}", err);
            }
        }
        result = run_heartbeat(config.clone(), messaging.clone(), cluster_state.clone()) => {
            if let Err(err) = result {
                error!("error running gossip - shutting down: {}", err);
            }
        }
    }
}

async fn _do_discovery(discovery_strategy: impl DiscoveryStrategy, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>, messaging: Arc<Messaging>) {
    match discovery_strategy.do_discovery(config.clone(), cluster_state.clone(), messaging.clone()).await {
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

async fn _run_active_loop(
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
) {
    let mut prev_time = Instant::now();

    let mut millis_until_next_leader_actions: u32 = 0;

    info!("starting cluster on {:?}", cluster_state.read().await.myself());

    loop {
        sleep(Duration::from_millis(10)).await;
        let new_time = Instant::now();
        let elapsed_millis: u32 = new_time.saturating_duration_since(prev_time).as_millis().try_into()
            .unwrap_or_else(|_| {
                error!("system clock jumped forward");
                10
            });
        prev_time = new_time;

        //TODO WeaklyUp actions

        millis_until_next_leader_actions = do_leader_actions(&config, &cluster_state, millis_until_next_leader_actions, elapsed_millis).await;
    }
}

async fn do_leader_actions(config: &ClusterConfig, cluster_state: &RwLock<ClusterState>, millis_until_next_leader_actions: u32, elapsed_millis: u32) -> u32 {
    match millis_until_next_leader_actions.checked_sub(elapsed_millis) {
        Some(millis) => millis,
        None => {
            debug!("periodic leader actions");
            cluster_state.write().await.leader_actions().await;
            config.leader_action_interval.as_millis() as u32
        }
    }
}

