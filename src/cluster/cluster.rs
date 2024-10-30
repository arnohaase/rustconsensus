use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_driver::run_cluster;
use crate::cluster::cluster_events::{ClusterEventListener, ClusterEventNotifier};
use crate::cluster::cluster_messages::ClusterMessageModule;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::messaging::message_module::MessageModule;
use crate::messaging::messaging::Messaging;

/// This is the cluster's public API
pub struct Cluster {
    config: Arc<ClusterConfig>,
    messaging: Arc<Messaging>,
    event_notifier: Arc<ClusterEventNotifier>,
}
impl Cluster {
    pub fn new(config: Arc<ClusterConfig>, messaging: Arc<Messaging>) -> Cluster {
        Cluster {
            config,
            messaging,
            event_notifier: Arc::new(ClusterEventNotifier::new()),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let myself = self.messaging.get_self_addr();
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, self.config.clone(), self.event_notifier.clone())));
        let heart_beat = Arc::new(RwLock::new(HeartBeat::new(myself, self.config.clone())));
        let gossip = Arc::new(RwLock::new(Gossip::new(myself, self.config.clone(), cluster_state.clone())));

        let cluster_messaging = ClusterMessageModule::new(gossip.clone(), self.messaging.clone(), heart_beat.clone());
        self.messaging.register_module(cluster_messaging.clone()).await?;

        run_cluster(self.config.clone(), cluster_state, heart_beat, gossip, self.messaging.clone()).await;

        self.messaging.deregister_module(cluster_messaging.id()).await
    }

    pub async fn add_listener(&self, listener: Arc<dyn ClusterEventListener>) -> Uuid {
        self.event_notifier.add_listener(listener).await
    }

    pub async fn remove_listener(&self, id: &Uuid) -> anyhow::Result<()> {
        self.event_notifier.try_remove_listener(id).await
    }

    //TODO external API for accessing state
}