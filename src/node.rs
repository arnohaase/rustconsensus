use tokio::sync::RwLock;

use crate::node_addr::NodeAddr;

/// Each node in the cluster has a [Node] instance to represent its state - this is basically where
///  the node's mutable state lives.
pub struct Node {
    myself: NodeAddr,
    data: RwLock<NodeData>,
}

struct NodeData {
}

