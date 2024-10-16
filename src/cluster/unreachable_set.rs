use rustc_hash::FxHashSet;

use crate::messaging::node_addr::NodeAddr;

pub struct UnreachableSet {
    nodes: FxHashSet<NodeAddr>,
}