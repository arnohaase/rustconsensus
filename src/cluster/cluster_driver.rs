use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;
use tokio::select;

use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_messages::{CLUSTER_MESSAGE_MODULE_ID, ClusterMessage};
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::discovery_strategy::DiscoveryStrategy;
use crate::cluster::_gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::messaging::messaging::Messaging;

//TODO documentation
pub async fn run_cluster(
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
    heart_beat: Arc<RwLock<HeartBeat>>,
    gossip: Arc<RwLock<Gossip>>,
    messaging: Arc<Messaging>,
    discovery_strategy: impl DiscoveryStrategy,
) {
    //TODO termination / shutdown

    select! {
        _ = _do_discovery(discovery_strategy, config.clone(), cluster_state.clone(), messaging.clone()) => {}
        _ = _run_active_loop(config, cluster_state, heart_beat, gossip, messaging) => {}
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
    heart_beat: Arc<RwLock<HeartBeat>>,
    gossip: Arc<RwLock<Gossip>>,
    messaging: Arc<Messaging>,
) {
    let mut prev_time = Instant::now();

    let mut millis_until_next_gossip: u32 = 0;
    let mut millis_until_next_heartbeat: u32 = 0;
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

        update_reachability(&cluster_state, &heart_beat).await;

        millis_until_next_gossip = do_gossip(&config, &gossip, &messaging, &cluster_state, millis_until_next_gossip, elapsed_millis).await;
        millis_until_next_heartbeat = do_heartbeat(&config, &cluster_state, &heart_beat, &messaging, millis_until_next_heartbeat, elapsed_millis).await;

        //TODO WeaklyUp actions

        millis_until_next_leader_actions = do_leader_actions(&config, &cluster_state, millis_until_next_leader_actions, elapsed_millis).await;
    }
}


async fn update_reachability(cluster_state: &RwLock<ClusterState>, heart_beat: &RwLock<HeartBeat>) {
    let current_reachability = heart_beat.write().await
        .get_current_reachability();
    cluster_state.write().await
        .update_current_reachability(&current_reachability)
        .await;
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

async fn do_heartbeat(config: &ClusterConfig, cluster_state: &RwLock<ClusterState>, heart_beat: &RwLock<HeartBeat>, messaging: &Messaging, millis_until_next_heartbeat: u32, elapsed_millis: u32) -> u32 {
    match millis_until_next_heartbeat.checked_sub(elapsed_millis) {
        Some(millis) => millis,
        None => {
            debug!("periodic heartbeat");
            let msg = heart_beat.write().await
                .new_heartbeat_message();
            let msg = ClusterMessage::Heartbeat(msg);
            let recipients = heart_beat.write().await
                .heartbeat_recipients(&*cluster_state.read().await);
            debug!("sending heartbeat message to {:?}", recipients);
            for recipient in recipients {
                let mut buf = BytesMut::new();
                msg.ser(&mut buf);
                let _ = messaging.send(recipient, CLUSTER_MESSAGE_MODULE_ID, &buf).await;
            }

            config.heartbeat_interval.as_millis() as u32 //TODO make sure it fits into u32
        }
    }
}

async fn do_gossip(config: &ClusterConfig, gossip: &RwLock<Gossip>, messaging: &Messaging, cluster_state: &RwLock<ClusterState>, millis_until_next_gossip: u32, elapsed_millis: u32) -> u32 {
    match millis_until_next_gossip.checked_sub(elapsed_millis) {
        Some(millis) => millis,
        None => {
            debug!("periodic gossip");
            let gossip_partners = gossip.read().await
                .gossip_partners().await;
            for (addr, msg) in gossip_partners {
                debug!("sending gossip message to {:?}", addr);
                let mut buf = BytesMut::new();
                msg.ser(&mut buf);
                let _ = messaging.send(addr, CLUSTER_MESSAGE_MODULE_ID, &buf).await;
            }

            if cluster_state.read().await
                .is_converged()
            {
                config.converged_gossip_interval.as_millis() as u32 //TODO make sure it fits into u32
            }
            else {
                config.converged_gossip_interval.as_millis() as u32 / 3 //TODO make sure it's not 0, fits into u32; separately configurable?
            }
        }
    }
}

