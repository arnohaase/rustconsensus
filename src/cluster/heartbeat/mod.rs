use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::{select, time};
use tokio::sync::broadcast::error::RecvError;
use tracing::debug;
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::ClusterEvent;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
use crate::cluster::heartbeat::heartbeat_messages::{HeartbeatMessage, HeartbeatMessageModule, HeartbeatResponseData};
use crate::cluster::heartbeat::reachability_decider::{FixedTimeoutDecider, ReachabilityDecider};
use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;

mod heartbeat_messages;
mod heartbeat_logic;
mod unreachable_tracker;
pub mod downing_strategy;
pub mod reachability_decider;

pub async fn run_heartbeat<M: Messaging>(config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: Arc<RwLock<ClusterState>>, mut cluster_events: broadcast::Receiver<ClusterEvent>, downing_strategy: Arc<dyn DowningStrategy>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();
    let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config.clone()); //TODO configurable reachability decider

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

async fn on_heartbeat_message<M: MessageSender, D: ReachabilityDecider>(sender: NodeAddr, msg: HeartbeatMessage, heartbeat: &mut HeartBeat<D>, messaging: &M) {
    match msg {
        HeartbeatMessage::Heartbeat(data) => {
            debug!("received heartbeat message");
            let response = HeartbeatMessage::HeartbeatResponse(HeartbeatResponseData {
                counter: data.counter,
                timestamp_nanos: data.timestamp_nanos,
            });
            messaging.send(sender, &response).await;
        }
        HeartbeatMessage::HeartbeatResponse(data) => {
            debug!("received heartbeat response message");
            heartbeat.on_heartbeat_response(&data, sender);
        }
    }
}

async fn update_reachability_from_here<D: ReachabilityDecider>(cluster_state: &RwLock<ClusterState>, heart_beat: &HeartBeat<D>) {
    let current_reachability = heart_beat.get_current_reachability();
    cluster_state.write().await
        .update_current_reachability(&current_reachability)
        .await;
}

async fn do_heartbeat<M: MessageSender, D: ReachabilityDecider>(cluster_state: &RwLock<ClusterState>, heart_beat: &mut HeartBeat<D>, messaging: &M) {
    debug!("periodic heartbeat");
    let msg = heart_beat.new_heartbeat_message();
    let msg = HeartbeatMessage::Heartbeat(msg);
    let recipients = heart_beat.heartbeat_recipients(&*cluster_state.read().await);
    debug!("sending heartbeat message to {:?}", recipients);
    for recipient in recipients {
        messaging.send(recipient, &msg).await;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_heartbeat_loop() {
        todo!()
    }
}