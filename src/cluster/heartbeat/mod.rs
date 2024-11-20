use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;
use tokio::sync::{mpsc, RwLock};
use tokio::{select, time};
use tracing::debug;
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
use crate::cluster::heartbeat::heartbeat_messages::{HEARTBEAT_MESSAGE_MODULE_ID, HeartbeatMessage, HeartbeatMessageModule, HeartbeatResponseData};
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;

mod heartbeat_messages;
mod heartbeat_logic;
mod unreachable_set;
mod downing_strategy;


pub async fn run_heartbeat(config: Arc<ClusterConfig>, messaging: Arc<Messaging>, cluster_state: Arc<RwLock<ClusterState>>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();
    let mut heartbeat = HeartBeat::new(myself, config.clone());

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
                update_reachability(cluster_state.as_ref(), &heartbeat).await
            }
            _ = heartbeat_ticks.tick() => {
                do_heartbeat(cluster_state.as_ref(), &mut heartbeat, messaging.as_ref()).await
            }
        }
    }
}

async fn on_heartbeat_message(sender: NodeAddr, msg: HeartbeatMessage, heartbeat: &mut HeartBeat, messaging: &Messaging) {
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

async fn update_reachability(cluster_state: &RwLock<ClusterState>, heart_beat: &HeartBeat) {
    let current_reachability = heart_beat.get_current_reachability();
    cluster_state.write().await
        .update_current_reachability(&current_reachability)
        .await;
}

async fn do_heartbeat(cluster_state: &RwLock<ClusterState>, heart_beat: &mut HeartBeat, messaging: &Messaging) {
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
