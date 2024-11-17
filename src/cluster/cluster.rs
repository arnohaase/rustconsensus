use std::sync::Arc;
use tokio::select;

use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_driver::run_cluster;
use crate::cluster::cluster_events::{ClusterEventListener, ClusterEventNotifier};
use crate::cluster::cluster_messages::{CLUSTER_MESSAGE_MODULE_ID, ClusterMessageModule};
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::discovery_strategy::DiscoveryStrategy;
use crate::cluster::_gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::cluster::join_messages::JoinMessageModule;
use crate::messaging::messaging::{JOIN_MESSAGE_MODULE_ID, Messaging};

/// This is the cluster's public API
pub struct Cluster {
    config: Arc<ClusterConfig>,
    messaging: Arc<Messaging>,
    event_notifier: Arc<ClusterEventNotifier>,
    cluster_state: Arc<RwLock<ClusterState>>,
    heart_beat: Arc<RwLock<HeartBeat>>,
    gossip: Arc<RwLock<Gossip>>,
}
impl Cluster {
    pub async fn new(config: Arc<ClusterConfig>, messaging: Arc<Messaging>) -> anyhow::Result<Cluster> {
        let myself = messaging.get_self_addr();
        let event_notifier = Arc::new(ClusterEventNotifier::new());
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), event_notifier.clone())));
        let heart_beat = Arc::new(RwLock::new(HeartBeat::new(myself, config.clone())));
        let gossip = Arc::new(RwLock::new(Gossip::new(myself, config.clone(), cluster_state.clone())));

        debug!("registering cluster message module");
        let cluster_messaging = ClusterMessageModule::new(gossip.clone(), messaging.clone(), heart_beat.clone());
        messaging.register_module(cluster_messaging.clone()).await?;
        debug!("registering cluster join module");
        let join_messaging = JoinMessageModule::new(cluster_state.clone());
        messaging.register_module(join_messaging.clone()).await?;

        Ok(Cluster {
            config,
            messaging,
            event_notifier,
            cluster_state,
            heart_beat,
            gossip,
        })
    }

    pub async fn run(&self, discovery_strategy: impl DiscoveryStrategy) -> anyhow::Result<()> {
        select! {
            //TODO start messaging receive loop only after the cluster is started
            //TODO and allow registration of application level message handlers before the receive loop is started
            r = self.messaging.recv() => r, //TODO spawn messaging? or at least message handling?
            r = self._run(discovery_strategy) => r,
        }
    }

    async fn _run(&self, discovery_strategy: impl DiscoveryStrategy) -> anyhow::Result<()> {
        run_cluster(self.config.clone(), self.cluster_state.clone(), self.heart_beat.clone(), self.gossip.clone(), self.messaging.clone(), discovery_strategy).await;

        debug!("deregistering cluster join module");
        self.messaging.deregister_module(JOIN_MESSAGE_MODULE_ID).await?;
        debug!("deregistering cluster message module");
        self.messaging.deregister_module(CLUSTER_MESSAGE_MODULE_ID).await
    }

    pub async fn add_listener(&self, listener: Arc<dyn ClusterEventListener>) -> Uuid {
        self.event_notifier.add_listener(listener).await
    }

    pub async fn remove_listener(&self, id: &Uuid) -> anyhow::Result<()> {
        self.event_notifier.try_remove_listener(id).await
    }

    //TODO external API for accessing state
}
