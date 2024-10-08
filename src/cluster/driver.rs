use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::cluster::cluster::Cluster;
use crate::msg::message_module::MessageModuleId;
use crate::msg::messaging::Messaging;

/// This is the active part and wrapper around a node's view of cluster state.
pub struct ClusterDriver {
    state: RwLock<Cluster>,
    messaging: Arc<Messaging>,
    message_module_id: MessageModuleId,
}
impl ClusterDriver {
    //TODO init with 'myself'

    pub async fn start(&self) {
        //TODO shutdown
        loop {

        }
    }

    async fn do_gossip_loop(&self) {
        loop {
            let targets = {
                let state = self.state.read().await;
                let gossip_partners = state.new_gossip_partners();
                let gossip_messages = gossip_partners.iter()
                    .map(|a| state.new_gossip_message(a))
                    .collect::<Vec<_>>();

                gossip_partners.into_iter().zip(gossip_messages.into_iter())
            };

            //TODO sequential updates --> later gossip partners receive updates from earlier partners

            for (addr, msg) in targets {
                let messaging = self.messaging.clone();
                let message_module_id = self.message_module_id;

                task::spawn(async move {
                    messaging.send(addr, message_module_id, b"asdf").await //TODO
                });
            }

            sleep(Duration::from_secs(1)).await //TODO config
        }
    }
}