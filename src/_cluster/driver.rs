use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;

use tokio::sync::RwLock;
use tokio::task;
use tokio::time::sleep;
use tracing::error;

use crate::_cluster::cluster::Cluster;
use crate::messaging::message_module::MessageModuleId;
use crate::messaging::messaging::Messaging;

/// This is the active part and wrapper around a node's view of _cluster state.
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
                let gossip_partners = state.choose_gossip_partners();
                let gossip_messages = gossip_partners.iter()
                    .map(|a| state.gossip_message_for(a))
                    .collect::<Vec<_>>();

                gossip_partners.into_iter().zip(gossip_messages.into_iter())
            };

            for (addr, msg) in targets {
                let messaging = self.messaging.clone();
                let message_module_id = self.message_module_id;

                task::spawn(async move {
                    let mut buf = BytesMut::new();
                    msg.ser(&mut buf);

                    if let Err(e) = messaging.send(addr, message_module_id, &buf).await {
                        error!("error sending message: {}", e);
                    }
                });
            }

            sleep(Duration::from_secs(1)).await //TODO config
        }
    }
}