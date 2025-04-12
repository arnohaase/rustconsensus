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
use crate::cluster::heartbeat::reachability_decider::fixed_timeout::FixedTimeoutDecider;
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;
use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;

mod heartbeat_messages;
mod heartbeat_logic;
mod unreachable_tracker;
pub mod downing_strategy;
pub mod reachability_decider;

//TODO unit test heartbeat loop
pub async fn run_heartbeat<M: Messaging>(config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: Arc<RwLock<ClusterState>>, mut cluster_events: broadcast::Receiver<ClusterEvent>, downing_strategy: Arc<dyn DowningStrategy>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();
    let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config.clone()); //TODO configurable reachability decider

    let mut unreachable_tracker = UnreachableTracker::new(config.clone(), cluster_state.clone(), downing_strategy);

    let (send, mut recv) = mpsc::channel(1024);

    let heartbeat_message_module = HeartbeatMessageModule::new(send);
    messaging.register_module(heartbeat_message_module.clone());

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
                timestamp_nanos: data.timestamp_nanos,
            });
            messaging.send_raw_fire_and_forget(sender.socket_addr, Some(sender.unique), &response).await;
        }
        HeartbeatMessage::HeartbeatResponse(data) => {
            debug!("received heartbeat response message");
            heartbeat.on_heartbeat_response(&data, sender);
        }
    }
}

async fn update_reachability_from_here<D: ReachabilityDecider>(cluster_state: &RwLock<ClusterState>, heart_beat: &HeartBeat<D>) {
    let current_reachability = heart_beat.get_current_reachability_from_here();
    cluster_state.write().await
        .update_reachability_from_myself(&current_reachability)
        .await;
}

async fn do_heartbeat<M: MessageSender, D: ReachabilityDecider>(cluster_state: &RwLock<ClusterState>, heart_beat: &mut HeartBeat<D>, messaging: &M) {
    debug!("periodic heartbeat");
    let msg = heart_beat.create_heartbeat_message();
    let msg = HeartbeatMessage::Heartbeat(msg);
    let recipients = heart_beat.heartbeat_recipients(&*cluster_state.read().await);
    debug!("sending heartbeat message to {:?}", recipients);
    for recipient in recipients {
        messaging.send_raw_fire_and_forget(recipient.socket_addr, Some(recipient.unique), &msg).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time;
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::cluster::cluster_state::*;
    use crate::cluster::cluster_state::MembershipState::Up;
    use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
    use crate::cluster::heartbeat::heartbeat_messages::{HeartbeatData, HeartbeatMessage, HeartbeatResponseData};
    use crate::cluster::heartbeat::{do_heartbeat, on_heartbeat_message, update_reachability_from_here};
    use crate::cluster::heartbeat::reachability_decider::fixed_timeout::FixedTimeoutDecider;
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;

    #[tokio::test]
    async fn test_on_heartbeat_message_heartbeat() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        on_heartbeat_message(test_node_addr_from_number(2), HeartbeatMessage::Heartbeat(HeartbeatData { timestamp_nanos: 12345 }), &mut heartbeat, messaging.as_ref()).await;

        assert!(heartbeat.get_current_reachability_from_here().is_empty());

        messaging.assert_message_sent(test_node_addr_from_number(2), HeartbeatMessage::HeartbeatResponse(HeartbeatResponseData { timestamp_nanos: 12345 })).await;
        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_on_heartbeat_message_response() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        on_heartbeat_message(test_node_addr_from_number(2), HeartbeatMessage::HeartbeatResponse(HeartbeatResponseData { timestamp_nanos: 12 }), &mut heartbeat, messaging.as_ref()).await;

        assert_eq!(heartbeat.get_current_reachability_from_here(), [(test_node_addr_from_number(2), true)].into());

        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_update_reachability_from_here() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr, None));
        let cluster_state = RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new())));
        for n in [2,3,4,5] {
            let mut node_state = node_state!(2[]:Up->[]@[1,2,3,4,5]);
            node_state.addr = test_node_addr_from_number(n);
            cluster_state.write().await
                .merge_node_state(node_state).await;
        }

        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);

        time::sleep(Duration::from_millis(100)).await;

        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(2));
        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(3));
        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(4));

        time::sleep(Duration::from_secs(4)).await;

        update_reachability_from_here(&cluster_state, &heartbeat).await;

        assert_eq!(cluster_state.read().await.get_node_state(&test_node_addr_from_number(2)).unwrap(), &node_state!(2[]:Up->[1:false@1]@[1]));
        assert_eq!(cluster_state.read().await.get_node_state(&test_node_addr_from_number(3)).unwrap(), &node_state!(3[]:Up->[1:false@1]@[1]));
        assert_eq!(cluster_state.read().await.get_node_state(&test_node_addr_from_number(4)).unwrap(), &node_state!(4[]:Up->[1:false@1]@[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn test_do_heartbeat() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.num_heartbeat_partners_per_node = 2;
        let config = Arc::new(config);
        let cluster_state = RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new())));
        for n in [2,3,4,5] {
            let mut node_state = node_state!(2[]:Up->[]@[1,2,3,4,5]);
            node_state.addr = test_node_addr_from_number(n);
            cluster_state.write().await
                .merge_node_state(node_state).await;
        }

        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        do_heartbeat(&cluster_state, &mut heartbeat, messaging.as_ref()).await;

        println!("a");
        messaging.assert_message_sent(test_node_addr_from_number(2), HeartbeatMessage::Heartbeat(HeartbeatData { timestamp_nanos: 0 })).await;
        println!("b");
        messaging.assert_message_sent(test_node_addr_from_number(3), HeartbeatMessage::Heartbeat(HeartbeatData { timestamp_nanos: 0 })).await;
        println!("c");
        messaging.assert_no_remaining_messages().await;
    }
}