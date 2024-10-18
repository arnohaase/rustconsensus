use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;

use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
use tracing::error;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_messages::{CLUSTER_MESSAGE_MODULE_ID, ClusterMessage};
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::messaging::messaging::Messaging;

//TODO documentation
pub async fn run_cluster(
    config: Arc<ClusterConfig>,
    cluster_state: Arc<RwLock<ClusterState>>,
    heart_beat: Arc<RwLock<HeartBeat>>,
    gossip: Arc<RwLock<Gossip>>,
    messaging: Arc<Messaging>,
) {

    //TODO termination / shutdown

    let mut prev_time = Instant::now();

    let mut millis_until_next_gossip: u32 = 0;
    let mut millis_until_next_heartbeat: u32 = 0;

    loop {
        sleep(Duration::from_millis(10)).await;
        let new_time = Instant::now();
        let elapsed_millis: u32 = new_time.saturating_duration_since(prev_time).as_millis().try_into()
            .unwrap_or_else(|_| {
                error!("system clock jumped forward");
                10
            });
        prev_time = new_time;

        // update reachability continuously
        let current_reachability = heart_beat.write().await
            .get_current_reachability();
        cluster_state.write().await
            .update_current_reachability(&current_reachability)
            .await;

        // send gossip at configured intervals
        millis_until_next_gossip = match millis_until_next_gossip.checked_sub(elapsed_millis) {
            Some(millis) => millis,
            None => {
                let gossip_partners = gossip.read().await
                    .gossip_partners().await;
                for (addr, msg) in gossip_partners {
                    let mut buf = BytesMut::new();
                    msg.ser(&mut buf);
                    let _ = messaging.send(addr, CLUSTER_MESSAGE_MODULE_ID, &buf).await;
                }

                //TODO increased gossip frequency if not converged
                config.regular_gossip_interval.as_millis() as u32 //TODO make sure it fits into u32
            }
        };


        // send heartbeat at configured intervals
        millis_until_next_heartbeat = match millis_until_next_heartbeat.checked_sub(elapsed_millis) {
            Some(millis) => millis,
            None => {
                let msg = heart_beat.write().await
                    .new_heartbeat_message();
                let msg = ClusterMessage::Heartbeat(msg);
                let recipients = heart_beat.write().await
                    .heartbeat_recipients(&*cluster_state.read().await);
                for recipient in recipients {
                    let mut buf = BytesMut::new();
                    msg.ser(&mut buf);
                    let _ = messaging.send(recipient, CLUSTER_MESSAGE_MODULE_ID, &buf).await;
                }

                config.heartbeat_interval.as_millis() as u32 //TODO make sure it fits into u32
            }
        }

        //TODO WeaklyUp actions


        //TODO leader actions
    }
}

