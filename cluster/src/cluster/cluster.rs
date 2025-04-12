use std::sync::Arc;

use tokio::select;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::{ClusterEvent, ClusterEventNotifier};
use crate::cluster::cluster_state::{ClusterState, run_administrative_tasks_loop, NodeState};
use crate::cluster::discovery_strategy::{DiscoveryStrategy, run_discovery};
use crate::cluster::gossip::run_gossip;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::run_heartbeat;
use crate::cluster::join_messages::{JoinMessage, JoinMessageModule};
use crate::messaging::messaging::{MessageSender, Messaging, RudpMessagingImpl};
use crate::messaging::node_addr::NodeAddr;

/// This is the cluster's public API
pub struct Cluster<M: Messaging>  {
    pub config: Arc<ClusterConfig>,
    pub messaging: Arc<M>,
    event_notifier: Arc<ClusterEventNotifier>,
    cluster_state: Arc<RwLock<ClusterState>>,
}

impl Cluster<RudpMessagingImpl> {
    pub async fn new(config: Arc<ClusterConfig>) -> anyhow::Result<Cluster<RudpMessagingImpl>> {
        let messaging = Arc::new(RudpMessagingImpl::new(config.transport_config.clone()).await?);
        let myself = messaging.get_self_addr();
        let event_notifier = Arc::new(ClusterEventNotifier::new());
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), event_notifier.clone())));

        debug!("registering cluster join module");
        let join_messaging = JoinMessageModule::new(cluster_state.clone());
        messaging.register_module(join_messaging.clone());

        Ok(Cluster {
            config,
            messaging,
            event_notifier,
            cluster_state,
        })
    }
}
impl <M: Messaging> Cluster<M> {
    pub async fn run(&self, discovery_strategy: impl DiscoveryStrategy, downing_strategy: impl DowningStrategy + 'static) -> anyhow::Result<()> {
        //TODO make discovery strategy and downing strategy part of the cluster's config - that should include seed nodes

        select! {
            //TODO start transport receive loop only after the cluster is started
            _ = self.messaging.recv() => Ok(()), //TODO spawn transport? or at least message handling?
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
        self.messaging.deregister_module(JoinMessage::JOIN_MESSAGE_MODULE_ID);

        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.event_notifier.subscribe()
    }

    pub async fn get_nodes(&self) -> Vec<NodeState> {
        self.cluster_state.read().await
            .node_states()
            .cloned()
            .collect()
    }

    pub async fn get_node_state(&self, addr: NodeAddr) -> Option<NodeState> {
        self.cluster_state.read().await
            .get_node_state(&addr)
            .cloned()
    }

    pub async fn is_converged(&self) -> bool {
        self.cluster_state.read().await
            .is_converged()
    }

    pub async fn get_leader(&self) -> Option<NodeAddr> {
        self.cluster_state.write().await
            .get_leader()
    }

    pub async fn am_i_leader(&self) -> bool {
        self.cluster_state.write().await
            .am_i_leader()
    }
}
