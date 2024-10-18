use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::sleep;

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

    loop {
        // update reachability continuously
        let current_reachability = heart_beat.write().await
            .get_current_reachability();
        cluster_state.write().await
            .update_current_reachability(&current_reachability);

        // send gossip at configured intervals
        //TODO gossip interval
        //TODO abstraction for clock / now / ...?

        let gossip_partners = gossip.read().await
            .gossip_partners().await;
        for (addr, msg) in gossip_partners {
            // there is nothing we can do about an error when sending a message
            let _ = messaging.send(addr, CLUSTER_MESSAGE_MODULE_ID, &msg.ser()).await;
        }

        // send heartbeat at configured intervals
        //TODO heartbeat interval

        let msg = heart_beat.write().await
            .new_heartbeat_message();
        let msg = ClusterMessage::Heartbeat(msg);
        let recipients = heart_beat.write().await
            .heartbeat_recipients(&*cluster_state.read().await);
        for recipient in recipients {
            let _ = messaging.send(recipient, CLUSTER_MESSAGE_MODULE_ID, &msg.ser()).await;
        }

        sleep(Duration::from_millis(10)).await;
    }
}

