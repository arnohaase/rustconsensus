use rustc_hash::FxHashSet;
use tokio::time::Instant;
use crate::messaging::node_addr::NodeAddr;

pub struct UnreachableSet {
    stable_since: Instant, //TODO?!
    unreachable_nodes: FxHashSet<NodeAddr>,
}
impl UnreachableSet {
    pub fn new() -> UnreachableSet {
        UnreachableSet {
            stable_since: Instant::now(),
            unreachable_nodes: FxHashSet::default(),
        }
    }

    //TODO -> notify UnreachableSet when heartbeat was potentially modified? or as part of 'update heartbeat' in cluster_driver?

    pub fn update_reachability(&mut self, node: NodeAddr, is_reachable: bool) {
        let modified = if is_reachable {
            self.unreachable_nodes.insert(node)
        }
        else {
            self.unreachable_nodes.remove(&node)
        };

        let asdf = tokio::spawn(async {   });
        asdf.abort()



        //TODO track 'last modified'
        //TODO track 'unstable since'



    }
}