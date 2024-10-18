use bytes::BytesMut;
use rustc_hash::FxHashMap;
use crate::cluster::cluster_state::NodeState;
use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;


pub const CLUSTER_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"Cluster\0");


pub enum ClusterMessage {
    GossipSummaryDigest(GossipSummaryDigestData),
    GossipDetailedDigest(GossipDetailedDigestData),
    GossipDifferingAndMissingNodesData(GossipDifferingAndMissingNodesData),
    GossipNodesData(GossipNodesData),
    Heartbeat(HeartbeatData),
    HeartbeatResponse(HeartbeatResponseData),
}
impl ClusterMessage {
    pub fn ser(&self) -> BytesMut {
        todo!()
    }
}


pub struct GossipSummaryDigestData {
    pub full_sha1_digest: [u8;20],
}

pub struct GossipDetailedDigestData {
    pub nonce: u32,
    pub nodes: FxHashMap<NodeAddr, u64>,
}

pub struct GossipDifferingAndMissingNodesData {
    pub differing: Vec<NodeState>,
    pub missing: Vec<NodeAddr>,
}

pub struct GossipNodesData {
    pub nodes: Vec<NodeState>,
}

pub struct HeartbeatData {
    pub counter: u32,
    pub timestamp_nanos: u64,
}

pub struct HeartbeatResponseData {
    pub counter: u32,
    pub timestamp_nanos: u64,
}