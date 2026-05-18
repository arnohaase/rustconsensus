use std::sync::Arc;

use tokio::select;
use tokio::sync::broadcast;
use tracing::{debug, error};

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::ClusterEvent;
use crate::cluster::cluster_state::run_administrative_tasks_loop;
use crate::cluster::discovery_strategy::{DiscoveryStrategy, run_discovery};
use crate::cluster::gossip::run_gossip;
use crate::cluster::heartbeat::downing_strategy::DowningStrategy;
use crate::cluster::heartbeat::run_heartbeat;
use crate::cluster::join_messages::{JoinMessage, JoinMessageModule};
use crate::cluster::cluster_state::ClusterStateHandle;
use crate::cluster::state::node_state::NodeState;
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::quic::quic_messaging::QuicMessaging;

/// This is the cluster's public API
pub struct Cluster  {
    pub config: Arc<ClusterConfig>,
    pub messaging: Arc<QuicMessaging>,
    /// Lock-free snapshot view of cluster state, published by the
    /// single-writer actor inside `ClusterStateHandle`. Public read methods
    /// on `Cluster` (`get_nodes`, `get_node_state`, `is_converged`,
    /// `get_leader`, `am_i_leader`) read through this handle. The
    /// `ClusterEventNotifier` is also owned by the handle and reached via
    /// `state_handle.events()`.
    /// All mutations go through `state_handle.cmd_*` commands.
    state_handle: ClusterStateHandle,
}

impl Cluster {
    pub async fn new(config: Arc<ClusterConfig>) -> anyhow::Result<Cluster> {
        let messaging = Arc::new(QuicMessaging::new(&config.transport_config).await?);
        let myself = messaging.get_self_addr();
        let state_handle = ClusterStateHandle::new(myself, config.clone());

        debug!("registering cluster join module");
        let join_messaging = JoinMessageModule::new(state_handle.clone());
        messaging.register_module(join_messaging.clone()).await;

        Ok(Cluster {
            config,
            messaging,
            state_handle,
        })
    }
}
impl Cluster {
    pub async fn run(&self, discovery_strategy: impl DiscoveryStrategy, downing_strategy: impl DowningStrategy + 'static) -> anyhow::Result<()> {
        //TODO make discovery strategy and downing strategy part of the cluster's config - that should include seed nodes

        select! {
            //TODO start transport receive loop only after the cluster is started
            _ = self.messaging.recv() => Ok(()),
            r = self._run(discovery_strategy, downing_strategy) => r,
        }
    }

    async fn _run(&self, discovery_strategy: impl DiscoveryStrategy, downing_strategy: impl DowningStrategy + 'static) -> anyhow::Result<()> {
        select! {
            _ = run_discovery(discovery_strategy, self.config.clone(), self.state_handle.clone(), self.messaging.clone()) => { }
            _ = run_administrative_tasks_loop(self.config.clone(), self.state_handle.clone(), self.messaging.get_self_addr()) => {}
            result = run_gossip(self.config.clone(), self.messaging.clone(), self.state_handle.clone()) => {
                if let Err(err) = result {
                    error!("error running gossip - shutting down: {}", err);
                }
            }
            result = run_heartbeat(self.config.clone(), self.messaging.clone(), self.state_handle.clone(), self.state_handle.events().subscribe(), Arc::new(downing_strategy)) => {
                if let Err(err) = result {
                    error!("error running heartbeat - shutting down: {}", err);
                }
            }
        }

        debug!("deregistering cluster join module");
        self.messaging.deregister_module(JoinMessage::JOIN_MESSAGE_MODULE_ID).await;

        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.state_handle.events().subscribe()
    }

    pub async fn get_nodes(&self) -> Vec<NodeState> {
        // Lock-free read via the published snapshot. Snapshot is refreshed by
        // `run_administrative_tasks_loop` on every leader-action tick (and
        // eventually after every write, once the actor lands), so this can
        // lag the live `RwLock<ClusterState>` by up to one tick.
        self.state_handle
            .snapshot()
            .nodes
            .values()
            .map(|n| (**n).clone())
            .collect()
    }

    pub async fn get_node_state(&self, addr: NodeAddr) -> Option<NodeState> {
        self.state_handle
            .snapshot()
            .get_node_state(&addr)
            .map(|arc| (**arc).clone())
    }

    pub async fn is_converged(&self) -> bool {
        self.state_handle.snapshot().is_converged()
    }

    /// Pure, side-effect-free read of the current leader from the published
    /// snapshot. May lag actual cluster state by up to one
    /// `leader_action_interval` (~1s). The corresponding `LeaderChanged`
    /// event is emitted exclusively by `do_leader_actions`.
    pub async fn get_leader(&self) -> Option<NodeAddr> {
        self.state_handle.snapshot().get_leader()
    }

    pub async fn am_i_leader(&self) -> bool {
        self.state_handle.snapshot().am_i_leader()
    }
}
