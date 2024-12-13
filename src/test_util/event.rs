use crate::cluster::cluster_events::{ClusterEvent, LeaderChangedData, NodeAddedData, NodeStateChangedData, NodeUpdatedData, ReachabilityChangedData};
use crate::cluster::cluster_state::MembershipState;
use crate::test_util::node::test_node_addr_from_number;

pub fn test_updated_evt(node: u16) -> ClusterEvent {
    ClusterEvent::NodeUpdated(NodeUpdatedData { addr: test_node_addr_from_number(node) })
}

pub fn test_state_evt(node: u16, old_state: MembershipState, new_state: MembershipState) -> ClusterEvent {
    ClusterEvent::NodeStateChanged(NodeStateChangedData {
        addr: test_node_addr_from_number(node),
        old_state,
        new_state,
    })
}

pub fn test_leader_evt(node: u16) -> ClusterEvent {
    ClusterEvent::LeaderChanged(LeaderChangedData {
        new_leader: test_node_addr_from_number(node),
    })
}

pub fn test_reachability_evt(node: u16, new_is_reachable: bool) -> ClusterEvent {
    ClusterEvent::ReachabilityChanged(ReachabilityChangedData {
        addr: test_node_addr_from_number(node),
        old_is_reachable: !new_is_reachable,
        new_is_reachable,
    })
}

pub fn test_added_evt(node: u16, state: MembershipState) -> ClusterEvent {
    ClusterEvent::NodeAdded(NodeAddedData {
        addr: test_node_addr_from_number(node),
        state,
    })
}