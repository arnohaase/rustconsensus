use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::RwLock;
use crate::cluster::cluster_events::{ClusterEvent, LeaderChangedData, NodeStateChangedData, NodeUpdatedData, ReachabilityChangedData};
use crate::cluster::cluster_state::MembershipState;
use crate::messaging::message_module::Message;
use crate::messaging::messaging::MessageSender;
use crate::messaging::node_addr::NodeAddr;


#[macro_export]
macro_rules! node_state {
    ($self_addr:literal [$($role:literal),*] : $ms:ident -> [$($r_id:literal : $reachable:literal @ $counter:literal),*] @ [$($seen_by:expr),*] ) => {{
        #[allow(unused_mut)]
        let mut roles = std::collections::BTreeSet::new();
        $(
            roles.insert($role.to_string());
        )*

        #[allow(unused_mut)]
        let mut reachability = std::collections::BTreeMap::new();
        $(
            reachability.insert($crate::test_util::test_node_addr_from_number($r_id), NodeReachability {
                counter_of_reporter: $counter,
                is_reachable: $reachable,
            });
        )*

        #[allow(unused_mut)]
        let mut seen_by = std::collections::BTreeSet::new();
        $(
            seen_by.insert($crate::test_util::test_node_addr_from_number($seen_by));
        )*

        NodeState {
            addr: $crate::test_util::test_node_addr_from_number($self_addr),
            membership_state: $ms,
            roles,
            reachability,
            seen_by,
        }
    }}
}

/// convenience method for unit test code: create a [NodeAddr] based on a number, the same number
///  generating the same address and different numbers different addresses
pub fn test_node_addr_from_number(number: u16) -> NodeAddr {
    NodeAddr {
        unique: number.into(),
        socket_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, number)),
    }
}

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

#[derive(Debug)]
pub struct TrackingMockMessageSender {
    myself: NodeAddr,
    tracker: Arc<RwLock<Vec<(NodeAddr, Box<dyn Message>)>>>,
}
impl TrackingMockMessageSender {
    pub fn new(myself: NodeAddr) -> Self {
        TrackingMockMessageSender {
            myself,
            tracker: Default::default()
        }
    }

    /// returns sent messages, clearing the internal buffer
    pub async fn sent_messages(&self) -> Vec<(NodeAddr, Box<dyn Message>)> {
        let mut lock = self.tracker.write().await;
        std::mem::take(&mut *lock)
    }
}

#[async_trait]
impl MessageSender for TrackingMockMessageSender {
    fn get_self_addr(&self) -> NodeAddr {
        self.myself
    }

    async fn send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.tracker.write().await.push((to, msg.box_clone()));
        Ok(())
    }
}
