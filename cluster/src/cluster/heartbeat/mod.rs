use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::ClusterEvent;
use crate::cluster::cluster_state::ClusterStateHandle;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
use crate::cluster::heartbeat::heartbeat_messages::{HeartbeatMessage, HeartbeatMessageModule, HeartbeatResponseData};
use crate::cluster::heartbeat::reachability_decider::{FixedTimeoutDecider, ReachabilityDecider};
use crate::cluster::heartbeat::unreachable_tracker::UnreachableTracker;
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::{select, time};
use tracing::{debug, warn};

mod heartbeat_messages;
mod heartbeat_logic;
mod unreachable_tracker;
pub mod downing_strategy;
pub mod reachability_decider;

//TODO unit test heartbeat loop
pub(crate) async fn run_heartbeat<M: Messaging>(config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: ClusterStateHandle, mut cluster_events: broadcast::Receiver<ClusterEvent>, downing_strategy: Arc<dyn DowningStrategy>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();
    let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config.clone()); //TODO configurable reachability decider

    let mut unreachable_tracker = UnreachableTracker::new(config.clone(), cluster_state.clone(), downing_strategy);

    let (send, mut recv) = mpsc::channel(1024);

    let heartbeat_message_module = HeartbeatMessageModule::new(send);
    messaging.register_module(heartbeat_message_module.clone()).await;

    let mut update_reachability_ticks = time::interval(Duration::from_millis(10)); //TODO configurable?
    let mut heartbeat_ticks = time::interval(config.heartbeat_interval);

    loop {
        select! {
            msg = recv.recv() => {
                let (sender, msg) = msg.expect("sender and receiver are defined locally - this should never happen");
                on_heartbeat_message(sender, msg, &mut heartbeat, messaging.as_ref()).await
            }
            _ = update_reachability_ticks.tick() => {
                update_reachability_from_here(&cluster_state, &heartbeat).await
            }
            _ = heartbeat_ticks.tick() => {
                do_heartbeat(&cluster_state, &mut heartbeat, messaging.as_ref()).await
            }
            evt = cluster_events.recv() => {
                match evt {
                    Ok(ClusterEvent::ReachabilityChanged(data)) => {
                        unreachable_tracker.update_reachability(data.addr, data.new_is_reachable, messaging.clone()).await;
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        // We missed `skipped` reachability events. Re-derive
                        // the full unreachable set from the most recently
                        // published snapshot (lock-free) and feed it into
                        // the tracker as a diff against its current view.
                        // Subsequent envelopes (whose `snapshot_version` is
                        // >= the snapshot we just observed) will continue
                        // to apply on top of this re-synced baseline.
                        warn!("heartbeat event subscriber lagged by {} envelopes; re-syncing unreachable set from snapshot", skipped);
                        let snapshot = cluster_state.snapshot();
                        let unreachable_now: rustc_hash::FxHashSet<NodeAddr> = snapshot
                            .node_states()
                            .filter(|n| !n.is_reachable())
                            .map(|n| n.addr)
                            .collect();
                        unreachable_tracker.resync_from_snapshot(unreachable_now, messaging.clone()).await;
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
            messaging.send_to_node(sender, &response).await
                .expect("HEARTBEAT should fit into a single packet");
        }
        HeartbeatMessage::HeartbeatResponse(data) => {
            debug!("received heartbeat response message");
            heartbeat.on_heartbeat_response(&data, sender);
        }
    }
}

async fn update_reachability_from_here<D: ReachabilityDecider>(cluster_state: &ClusterStateHandle, heart_beat: &HeartBeat<D>) {
    let current_reachability = heart_beat.get_current_reachability_from_here();
    cluster_state.cmd_update_reachability_from_myself(current_reachability).await;
}

async fn do_heartbeat<M: MessageSender, D: ReachabilityDecider>(cluster_state: &ClusterStateHandle, heart_beat: &mut HeartBeat<D>, messaging: &M) {
    debug!("periodic heartbeat");
    let msg = heart_beat.create_heartbeat_message();
    let msg = HeartbeatMessage::Heartbeat(msg);
    let recipients = heart_beat.heartbeat_recipients(&cluster_state.snapshot());
    debug!("sending heartbeat message to {:?}", recipients);
    for recipient in recipients {
        messaging.send_to_node(recipient, &msg).await
            .expect("HEARTBEAT RESPONSE should fit into a single packet");
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_state::ClusterStateHandle;
    use crate::cluster::heartbeat::heartbeat_logic::HeartBeat;
    use crate::cluster::heartbeat::heartbeat_messages::{HeartbeatData, HeartbeatMessage, HeartbeatResponseData};
    use crate::cluster::heartbeat::reachability_decider::FixedTimeoutDecider;
    use crate::cluster::heartbeat::{do_heartbeat, on_heartbeat_message, update_reachability_from_here};
    use crate::cluster::state::node_state::MembershipState::Up;
    use crate::messaging::node_addr::NodeAddr;
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time;

    fn handle_for_new(myself: NodeAddr, config: Arc<ClusterConfig>) -> ClusterStateHandle {
        ClusterStateHandle::new(myself, config)
    }

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
        let handle = handle_for_new(myself, config.clone());
        for n in [2,3,4,5] {
            let mut node_state = node_state!(2[]:Up->[]@[1,2,3,4,5]);
            node_state.addr = test_node_addr_from_number(n);
            handle.cmd_merge_node_state(node_state).await;
        }
        handle.flush().await;

        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);

        time::sleep(Duration::from_millis(100)).await;

        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(2));
        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(3));
        heartbeat.on_heartbeat_response(&HeartbeatResponseData { timestamp_nanos: 10 }, test_node_addr_from_number(4));

        time::sleep(Duration::from_secs(4)).await;

        update_reachability_from_here(&handle, &heartbeat).await;
        // `update_reachability_from_here` dispatches to the actor and
        // returns as soon as the command is enqueued; flush to ensure the
        // actor has applied it before we read the underlying state.
        handle.flush().await;

        let snap = handle.snapshot();
        assert_eq!(snap.get_node_state(&test_node_addr_from_number(2)).unwrap().as_ref(), &node_state!(2[]:Up->[1:false@1]@[1]));
        assert_eq!(snap.get_node_state(&test_node_addr_from_number(3)).unwrap().as_ref(), &node_state!(3[]:Up->[1:false@1]@[1]));
        assert_eq!(snap.get_node_state(&test_node_addr_from_number(4)).unwrap().as_ref(), &node_state!(4[]:Up->[1:false@1]@[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn test_do_heartbeat() {
        let myself = test_node_addr_from_number(1);
        let mut config = ClusterConfig::new(myself.socket_addr, None);
        config.num_heartbeat_partners_per_node = 2;
        let config = Arc::new(config);
        let handle = handle_for_new(myself, config.clone());
        for n in [2,3,4,5] {
            let mut node_state = node_state!(2[]:Up->[]@[1,2,3,4,5]);
            node_state.addr = test_node_addr_from_number(n);
            handle.cmd_merge_node_state(node_state).await;
        }
        handle.flush().await;

        let mut heartbeat = HeartBeat::<FixedTimeoutDecider>::new(myself, config);
        let messaging = Arc::new(TrackingMockMessageSender::new(myself));

        do_heartbeat(&handle, &mut heartbeat, messaging.as_ref()).await;

        messaging.assert_message_sent(test_node_addr_from_number(2), HeartbeatMessage::Heartbeat(HeartbeatData { timestamp_nanos: 0 })).await;
        messaging.assert_message_sent(test_node_addr_from_number(3), HeartbeatMessage::Heartbeat(HeartbeatData { timestamp_nanos: 0 })).await;
        messaging.assert_no_remaining_messages().await;
    }
}