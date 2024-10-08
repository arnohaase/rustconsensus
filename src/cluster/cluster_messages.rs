use std::sync::Arc;
use bytes::{Buf, BytesMut};
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;
use crate::cluster::cluster::{Cluster, NodeMembershipState};
use crate::msg::envelope::Envelope;
use crate::msg::message_module::{MessageModule, MessageModuleId};
use crate::msg::node_addr::NodeAddr;


pub enum ClusterMessage {
    //TODO two-way? three-way? fixpoint iteration?

    /// This message initiates a gossip exchange, sending seeded hash values for cluster state.
    ///
    /// Gossip is a three-way protocol:
    /// * 'Hi' (sent from A to B) sends hashes of A's state to allow negotiating content that
    ///    really needs to be transmitted. NB: Even if B is part of a 'seen by' list in A, that
    ///    list may still need to be sent from A to B to make B aware that some other node C has
    ///    seen the data
    /// * 'Reply' (sent from B to A) is the response to 'Hi'. B compares hashes,
    ///    determining data that may be ahead of A and sending that data. B can also explicitly
    ///    request data that A announced but that is missing in B completely. NB: If B sends an
    ///    update for some data item, that implies a request for an update from A. Even if B's value
    ///    is strictly bigger than A's (in the [Crdt] sense), adding A to its 'seen by' set
    ///    poses an update, so the complexity in determining the need for updates was kapt out of
    ///    the protocol to keep it simpler.
    /// * 'Data' (sent from A to B) sends data in a one-shot fashion, i.e. without expecting a
    ///    response. It can be sent as a response to 'reply', but it also works without being
    ///    part of a conversation.
    ///
    /// The gossip protocol is stateless, i.e. each gossip message is self-contained and does
    ///  not require the nodes to keep track of an ongoing conversation. So if a message is lost
    ///  or a node chooses not to respond, there is no need for cleanup.
    GossipHi(GossipSummaryData),

    /// This is the reply to 'hi', sending and requesting updates based on hash comparison.
    GossipReply(GossipReplyData),

    /// This is the response to 'reply', sending data requested by the reply.
    GossipData(GossipJustData),
}
impl ClusterMessage {
    pub fn ser(&self, buf: &mut BytesMut) {
        //TODO serialize directly into a buffer -> avoid cloning for intermediate 'message' representation

        todo!()
    }

    pub fn deser(mut buf: impl  Buf) -> ClusterMessage {
        todo!()
    }
}


pub struct GossipSummaryData {
    nonce: u32,
    nodes: FxHashMap<NodeAddr, u64>,
}

pub struct GossipReplyData {
    nodes_with_updates: Vec<NodeMembershipState>,
    requested_updates: Vec<NodeAddr>,
}

pub struct GossipJustData {
    nodes_with_updates: Vec<NodeMembershipState>,
}


pub struct ClusterMessageModule {
    cluster: Arc<RwLock<Cluster>>,
}

impl ClusterMessageModule {
    const ID: MessageModuleId = MessageModuleId::new(b"cluster\0");

    async fn on_hi(&self, data: GossipSummaryData) {
        let self_hashes = self.cluster.read().await
            .hash_for_equality_check(data.nonce);







        todo!()
    }
}

#[async_trait::async_trait]
impl MessageModule for ClusterMessageModule {
    fn id(&self) -> MessageModuleId {
        Self::ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        let msg = ClusterMessage::deser(buf);

        match msg {
            ClusterMessage::GossipHi(data) => self.on_hi(data).await,
            ClusterMessage::GossipReply(data) => todo!(),
            ClusterMessage::GossipData(data) => todo!(),
        }
    }
}