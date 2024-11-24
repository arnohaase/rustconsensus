use std::sync::Arc;

use tokio::select;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{ClusterEvent, ClusterEventNotifier};
use crate::cluster::cluster_state::{ClusterState, run_administrative_tasks_loop};
use crate::cluster::discovery_strategy::{DiscoveryStrategy, run_discovery};
use crate::cluster::gossip::run_gossip;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::run_heartbeat;
use crate::cluster::join_messages::JoinMessageModule;
use crate::messaging::messaging::{JOIN_MESSAGE_MODULE_ID, Messaging};
use crate::messaging::node_addr::NodeAddr;

/// This is the cluster's public API
pub struct Cluster {
    pub config: Arc<ClusterConfig>,
    pub messaging: Arc<Messaging>,
    event_notifier: Arc<ClusterEventNotifier>,
    cluster_state: Arc<RwLock<ClusterState>>,
}
impl Cluster {
    pub async fn new(config: Arc<ClusterConfig>) -> anyhow::Result<Cluster> {
        let myself = NodeAddr::from(config.self_addr);
        let messaging = Arc::new(Messaging::new(myself).await?); //TODO configurable Transport
        let event_notifier = Arc::new(ClusterEventNotifier::new());
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), event_notifier.clone())));

        debug!("registering cluster join module");
        let join_messaging = JoinMessageModule::new(cluster_state.clone());
        messaging.register_module(join_messaging.clone()).await?;

        Ok(Cluster {
            config,
            messaging,
            event_notifier,
            cluster_state,
        })
    }

    pub async fn run(&self, discovery_strategy: impl DiscoveryStrategy, downing_strategy: impl DowningStrategy + 'static) -> anyhow::Result<()> {
        //TODO make discovery strategy and downing strategy part of the cluster's config - that should include seed nodes

        select! {
            //TODO start messaging receive loop only after the cluster is started
            r = self.messaging.recv() => r, //TODO spawn messaging? or at least message handling?
            r = self._run(discovery_strategy, downing_strategy) => r,
        }
    }

    async fn _run(&self, discovery_strategy: impl DiscoveryStrategy, downing_strategy: impl DowningStrategy + 'static) -> anyhow::Result<()> {
        select! {
            _ = run_discovery(discovery_strategy, self.config.clone(), self.cluster_state.clone(), self.messaging.clone()) => { }
            _ = run_administrative_tasks_loop(self.config.clone(), self.cluster_state.clone(), self.event_notifier.subscribe()) => {}
            result = run_gossip(self.config.clone(), self.messaging.clone(), self.cluster_state.clone()) => {
                if let Err(err) = result {
                    error!("error running gossip - shutting down: {}", err);
                }
            }
            result = run_heartbeat(self.config.clone(), self.messaging.clone(), self.cluster_state.clone(), self.event_notifier.subscribe(), Arc::new(downing_strategy)) => {
                if let Err(err) = result {
                    error!("error running heartbeat - shutting down: {}", err);
                }
            }
        }

        debug!("deregistering cluster join module");
        self.messaging.deregister_module(JOIN_MESSAGE_MODULE_ID).await?;

        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_notifier.subscribe()
    }

    //TODO external API for accessing state
}
