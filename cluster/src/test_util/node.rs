use std::net::{Ipv4Addr, SocketAddrV4};
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
            reachability.insert($crate::test_util::node::test_node_addr_from_number($r_id), NodeReachability {
                counter_of_reporter: $counter,
                is_reachable: $reachable,
            });
        )*

        #[allow(unused_mut)]
        let mut seen_by = std::collections::BTreeSet::new();
        $(
            seen_by.insert($crate::test_util::node::test_node_addr_from_number($seen_by));
        )*

        $crate::cluster::cluster_state::NodeState {
            addr: $crate::test_util::node::test_node_addr_from_number($self_addr),
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
        socket_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, number).into(),
    }
}
