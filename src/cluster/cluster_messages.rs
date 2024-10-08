use crate::cluster::cluster::NodeMembershipState;
use crate::msg::message_module::{MessageModule, MessageModuleId};



pub enum ClusterMessages {
    //TODO two-way? three-way? fixpoint iteration?

    /// This message initiates gossip, with the sender sending those parts of its state that the
    ///  gossip partner has not seen yet (as represented by the sender's 'seen by' sets).
    ///
    /// NB: Sending data via gossip provides *no* guarantees that the gossip partner has seen the
    ///  updates.
    GossipUpdates(GossipUpdatesData),
    /// This is the response to 'Hi I have updates'. The only difference between the two messages
    ///  is that 'I have updates' invites a response while this message does not.
    ///
    /// NB: A node should send the changes received by 'I have updates' back with itself added to
    ///  the 'seen by' set. That is effectively an ACK for the gossip changes.
    ///
    /// These changes are however *not* the
    GossipResponseWithUpdates(GossipUpdatesData),
    /// If the gossip partner has the same state as far as we know, we send them a seeded hash of
    ///  our state to verify that state actually (still) is the same. The target node may choose
    ///  not to reply (when its state has the same hash), or it may respond by initiating a regular
    ///  delta based gossip based on its own 'seen by' lists.
    ///
    /// Or - if the hash differs but its 'seen by' list shows no delta - it should send its entire
    ///  state by gossip. This 'should' never be necessary, but it is a mechanism for healing
    ///  inconsistent state.
    GossipHiSummary(GossipSummary),
}

pub struct GossipSummary {
    nonce: u32,
    hash: u64,
}

pub struct GossipUpdatesData {
    nodes_with_updates: Vec<NodeMembershipState>,
}



pub struct ClusterMessageModule {

}

impl ClusterMessageModule {
    const ID: MessageModuleId = MessageModuleId::new(b"cluster\0");
}

impl MessageModule for ClusterMessageModule {

    fn id(&self) -> MessageModuleId {
        Self::ID
    }

    fn on_message(&self, buf: &[u8]) {
        todo!()
    }
}