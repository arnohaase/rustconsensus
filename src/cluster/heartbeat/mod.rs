use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::{select, time};
use tokio::sync::broadcast::error::RecvError;
use tracing::debug;
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::ClusterEvent;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
use crate::cluster::heartbeat::heartbeat_messages::{HEARTBEAT_MESSAGE_MODULE_ID, HeartbeatMessage, HeartbeatMessageModule, HeartbeatResponseData};
use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;

mod heartbeat_messages;
mod heartbeat_logic;
mod unreachable_tracker;
pub mod downing_strategy;


pub async fn run_heartbeat<M: Messaging> (config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: Arc<RwLock<ClusterState>>, mut cluster_events: broadcast::Receiver<ClusterEvent>, downing_strategy: Arc<dyn DowningStrategy>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();
    let mut heartbeat = HeartBeat::new(myself, config.clone());

    let mut unreachable_tracker = UnreachableTracker::new(config.clone(), cluster_state.clone(), downing_strategy);

    let (send, mut recv) = mpsc::channel(1024);

    let heartbeat_message_module = HeartbeatMessageModule::new(send);
    messaging.register_module(heartbeat_message_module.clone()).await?;

    let mut update_reachability_ticks = time::interval(Duration::from_millis(10)); //TODO configurable?
    let mut heartbeat_ticks = time::interval(config.heartbeat_interval);

    loop {
        select! {
            msg = recv.recv() => {
                let (sender, msg) = msg.expect("sender and receiver are defined locally - this should never happen");
                on_heartbeat_message(sender, msg, &mut heartbeat, messaging.as_ref()).await
            }
            _ = update_reachability_ticks.tick() => {
                update_reachability_from_here(cluster_state.as_ref(), &heartbeat).await
            }
            _ = heartbeat_ticks.tick() => {
                do_heartbeat(cluster_state.as_ref(), &mut heartbeat, messaging.as_ref()).await
            }
            evt = cluster_events.recv() => {
                match evt {
                    Ok(ClusterEvent::ReachabilityChanged(data)) => {
                        unreachable_tracker.update_reachability(data.addr, data.new_is_reachable, messaging.clone()).await;
                    }
                    Err(RecvError::Lagged(_)) => {
                        //TODO re-sync based on cluster state
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn on_heartbeat_message<M: Messaging> (sender: NodeAddr, msg: HeartbeatMessage, heartbeat: &mut HeartBeat, messaging: &M) {
    match msg {
        HeartbeatMessage::Heartbeat(data) => {
            debug!("received heartbeat message");
            //TODO extract into MessageModule
            let mut buf = BytesMut::new();
            HeartbeatMessage::HeartbeatResponse(HeartbeatResponseData {
                counter: data.counter,
                timestamp_nanos: data.timestamp_nanos,
            }).ser(&mut buf);
            let _ = messaging.send(sender, HEARTBEAT_MESSAGE_MODULE_ID, &buf).await;
        }
        HeartbeatMessage::HeartbeatResponse(data) => {
            debug!("received heartbeat response message");
            heartbeat.on_heartbeat_response(&data, sender);
        }
    }
}

async fn update_reachability_from_here(cluster_state: &RwLock<ClusterState>, heart_beat: &HeartBeat) {
    let current_reachability = heart_beat.get_current_reachability();
    cluster_state.write().await
        .update_current_reachability(&current_reachability)
        .await;
}

async fn do_heartbeat<M: Messaging> (cluster_state: &RwLock<ClusterState>, heart_beat: &mut HeartBeat, messaging: &M) {
    debug!("periodic heartbeat");
    let msg = heart_beat.new_heartbeat_message();
    let msg = HeartbeatMessage::Heartbeat(msg);
    let recipients = heart_beat.heartbeat_recipients(&*cluster_state.read().await);
    debug!("sending heartbeat message to {:?}", recipients);
    for recipient in recipients {
        let mut buf = BytesMut::new();
        msg.ser(&mut buf);
        let _ = messaging.send(recipient, HEARTBEAT_MESSAGE_MODULE_ID, &buf).await;
    }
}
