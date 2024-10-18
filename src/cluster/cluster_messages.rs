use std::sync::Arc;
use bytes::BytesMut;
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;
use crate::cluster::cluster_state::NodeState;
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::messaging::envelope::Envelope;
use crate::messaging::message_module::{MessageModule, MessageModuleId};
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;


pub const CLUSTER_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"Cluster\0");

pub struct ClusterMessageModule {
    gossip: Arc<RwLock<Gossip>>,
    messaging: Arc<RwLock<Messaging>>,
    heart_beat: Arc<RwLock<HeartBeat>>,
}
impl ClusterMessageModule {
    async fn reply(&self, envelope: &Envelope, message: ClusterMessage) {
        let _ = self.messaging.read().await
            .send(envelope.from, CLUSTER_MESSAGE_MODULE_ID, &message.ser()).await;
    }
}

#[async_trait::async_trait]
impl MessageModule for ClusterMessageModule {
    fn id(&self) -> MessageModuleId {
        CLUSTER_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        use ClusterMessage::*;

        match ClusterMessage::deser(buf) {
            GossipSummaryDigest(digest) => {
                if let Some(response) = self.gossip.read().await
                    .on_summary_digest(&digest).await
                {
                    self.reply(envelope, GossipDetailedDigest(response)).await;
                }
            }
            GossipDetailedDigest(digest) => {
                if let Some(response) = self.gossip.read().await
                    .on_detailed_digest(&digest).await
                {
                    self.reply(envelope, GossipDifferingAndMissingNodesData(response)).await;
                }
            }
            GossipDifferingAndMissingNodesData(data) => {
                if let Some(response) = self.gossip.read().await
                    .on_differing_and_missing_nodes(data).await
                {
                    self.reply(envelope, GossipNodesData(response)).await;
                }
            }
            GossipNodesData(data) => {
                self.gossip.read().await
                    .on_nodes(data).await;
            }
            Heartbeat(data) => {
                //TODO document heartbeat protocol
                //TODO documentation here
                self.reply(envelope, HeartbeatResponse(HeartbeatResponseData {
                    counter: data.counter,
                    timestamp_nanos: data.timestamp_nanos,
                })).await;
            }
            HeartbeatResponse(data) => {
                self.heart_beat.write().await
                    .on_heartbeat_response(&data, envelope.from);
            }
        }
    }
}



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

    pub fn deser(buf: &[u8]) -> ClusterMessage {
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