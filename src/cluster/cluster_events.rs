use std::sync::Arc;

use anyhow::anyhow;
use rustc_hash::FxHashMap;
use tokio::spawn;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::cluster::cluster_state::MembershipState;
use crate::messaging::node_addr::NodeAddr;

#[derive(Clone, Debug)]
pub enum ClusterEvent {
    /// low-level event, sent redundantly to higher-level events - should be uninteresting for
    ///  typical consumers
    NodeUpdated(NodeUpdatedData),
    NodeAdded(NodeAddedData),
    NodeRemoved(NodeRemovedData),
    LeaderChanged(LeaderChangedData), //TODO fire this event
    ReachabilityChanged(ReachabilityChangedData),
    NodeStateChanged(NodeStateChangedData),
}

#[derive(Clone, Debug)]
pub struct NodeUpdatedData {
    pub addr: NodeAddr,
}


#[derive(Clone, Debug)]
pub struct NodeAddedData {
    pub addr: NodeAddr,
    pub state: MembershipState,
}

#[derive(Clone, Debug)]
pub struct NodeRemovedData {
    pub addr: NodeAddr,
}

#[derive(Clone, Debug)]
pub struct LeaderChangedData {
    pub old_leader: Option<NodeAddr>,
    pub new_leader: Option<NodeAddr>,
}

#[derive(Clone, Debug)]
pub struct ReachabilityChangedData {
    pub addr: NodeAddr,
    pub old_is_reachable: bool,
    pub new_is_reachable: bool,
}

#[derive(Clone, Debug)]
pub struct NodeStateChangedData {
    pub addr: NodeAddr,
    pub old_state: MembershipState,
    pub new_state: MembershipState,
}

#[async_trait::async_trait]
pub trait ClusterEventListener: Sync + Send {
    async fn on_cluster_event(&self, event: ClusterEvent);
}

pub struct ClusterEventNotifier {
    listeners: RwLock<FxHashMap<Uuid, Arc<dyn ClusterEventListener>>>,
}
impl ClusterEventNotifier {
    pub fn new() -> ClusterEventNotifier {
        ClusterEventNotifier {
            listeners: Default::default(),
        }
    }

    //TODO documentation, especially for key / removal
    pub async fn add_listener(&self, listener: Arc<dyn ClusterEventListener>) -> Uuid {
        let id = Uuid::new_v4();
        self.listeners.write().await
            .insert(id.clone(), listener);
        id
    }

    pub async fn try_remove_listener(&self, listener_id: &Uuid) -> anyhow::Result<()> {
        match self.listeners.write().await
            .remove(listener_id)
        {
            None => Err(anyhow!("tried to remove a listener that was not (no longer?) registered: {}", listener_id)),
            Some(_) => Ok(()),
        }
    }

    pub async fn run_loop(&self, mut recv: mpsc::Receiver<ClusterEvent>) {
        loop {
            if let Some(event) = recv.recv().await {
                let listeners = self.listeners.read().await
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                for l in listeners {
                    let evt = event.clone();
                    spawn(async move { l.on_cluster_event(evt.clone()).await });
                }
            }
            else {
                break;
            }
        }
    }
}
