use tokio::sync::broadcast;
use tracing::trace;
use crate::cluster::state::node_state::MembershipState;
use crate::messaging::node_addr::NodeAddr;

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeUpdatedData {
    pub addr: NodeAddr,
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAddedData {
    pub addr: NodeAddr,
    pub state: MembershipState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRemovedData {
    pub addr: NodeAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderChangedData {
    pub new_leader: NodeAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReachabilityChangedData {
    pub addr: NodeAddr,
    pub old_is_reachable: bool,
    pub new_is_reachable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStateChangedData {
    pub addr: NodeAddr,
    pub old_state: MembershipState,
    pub new_state: MembershipState,
}



/// Broadcaster for cluster events. Owned by [`ClusterStateHandle`] (which
/// constructs it during [`ClusterStateHandle::new`]) and reachable from
/// outside via [`ClusterStateHandle::events`].
///
/// Ownership rules:
///   * **Only the cluster-state actor calls [`send_event`]**. The actor is
///     the single writer of cluster state; every `ClusterEvent` is derived
///     from a reducer the actor just ran, so the actor is also the single
///     emitter. Other subsystems consume events through `subscribe` /
///     `subscribe_envelopes`.
///   * **The actor also owns the authoritative `published_version`
///     counter.** The notifier is constructed with a *read-only* `Arc`
///     clone of that counter so it can tag envelopes without ever bumping
///     the version itself.
pub struct ClusterEventNotifier {
    sender: broadcast::Sender<ClusterEvent>,
}
impl ClusterEventNotifier {
    /// Build a notifier that reads the published-snapshot version from
    /// `published_version`. The caller (the cluster-state actor) retains
    /// the only writable clone of that `Arc` and is the only entity that
    /// ever bumps it.
    pub(crate) fn new() -> ClusterEventNotifier {
        let (sender, _) = broadcast::channel(128); //TODO make configurable

        ClusterEventNotifier {
            sender,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent> {
        self.sender.subscribe()
    }

    /// Called **only by the cluster-state actor** after a snapshot publish.
    /// Tags the outgoing envelope with the version of the snapshot the
    /// event is derived from.
    pub(crate) fn send_event(&self, event: ClusterEvent) {
        trace!("event: {:?}", event);
        let _ = self.sender.send(event.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::node::test_node_addr_from_number;

    fn test_event() -> ClusterEvent {
        ClusterEvent::NodeUpdated(NodeUpdatedData {
            addr: test_node_addr_from_number(1),
        })
    }

    #[tokio::test]
    async fn test_event_is_sent() {
        let notifier = ClusterEventNotifier::new();
        let mut rx = notifier.subscribe();

        notifier.send_event(test_event());

        assert_eq!(rx.recv().await.unwrap(), test_event());
    }
}
