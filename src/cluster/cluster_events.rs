use tokio::sync::broadcast;
use tracing::trace;

use crate::cluster::cluster_state::MembershipState;
use crate::messaging::node_addr::NodeAddr;

#[derive(Clone, Debug)]
pub enum ClusterEvent {
    /// low-level event, sent redundantly to higher-level events - should be uninteresting for
    ///  typical consumers.
    NodeUpdated(NodeUpdatedData),
    NodeAdded(NodeAddedData),
    NodeRemoved(NodeRemovedData),
    LeaderChanged(LeaderChangedData),
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


pub struct ClusterEventNotifier {
    sender: broadcast::Sender<ClusterEvent>,
}
impl ClusterEventNotifier {
    pub fn new() -> ClusterEventNotifier {
        let (sender, _) = broadcast::channel(128);

        ClusterEventNotifier {
            sender
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.sender.subscribe()
    }

    pub fn send_event(&self, event: ClusterEvent) {
        trace!("event: {:?}", event);
        let _ = self.sender.send(event);
    }
}
