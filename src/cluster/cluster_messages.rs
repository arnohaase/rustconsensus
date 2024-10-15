use std::collections::hash_map::Entry;
use std::sync::Arc;
use anyhow::anyhow;

use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;

use crate::cluster::cluster::{Cluster, NodeMembershipState};
use crate::msg::envelope::Envelope;
use crate::msg::message_module::{MessageModule, MessageModuleId};
use crate::msg::messaging::Messaging;
use crate::msg::node_addr::NodeAddr;
use crate::util::buf_ext::*;

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
    const ID_GOSSIP_HI: u8 = 1;
    const ID_GOSSIP_REPLY: u8 = 2;
    const ID_GOSSIP_DATA: u8 = 3;

    fn msg_id(&self) -> u8 {
        match self {
            ClusterMessage::GossipHi(_) => Self::ID_GOSSIP_HI,
            ClusterMessage::GossipReply(_) => Self::ID_GOSSIP_REPLY,
            ClusterMessage::GossipData(_) => Self::ID_GOSSIP_DATA,
        }
    }

    pub fn deser(mut buf: impl Buf) -> anyhow::Result<ClusterMessage> {
        //TODO Buf::try_get_u8() etc.

        match buf.try_get_u8()? {
            Self::ID_GOSSIP_HI => Self::deser_gossip_hi(buf),
            Self::ID_GOSSIP_REPLY => Self::deser_gossip_reply(buf),
            Self::ID_GOSSIP_DATA => Self::deser_gossip_data(buf),
            other => return Err(anyhow!("invalid discriminator {}", other)),
        }
    }

    pub fn ser(&self, buf: &mut BytesMut) {
        match self {
            ClusterMessage::GossipHi(data) => Self::ser_gossip_hi(data.nonce, &data.nodes, buf),
            ClusterMessage::GossipReply(data) => Self::ser_gossip_reply(&data.nodes_with_updates, &data.requested_updates, buf),
            ClusterMessage::GossipData(_) => {}
        }
    }

    pub fn ser_gossip_hi(nonce: u32, node_hashes: &FxHashMap<NodeAddr, u64>, buf: &mut BytesMut) {
        buf.put_u8(Self::ID_GOSSIP_HI);

        buf.put_u32_le(nonce);

        let mut addr_serializer = NodeAddressesSerializer::new(buf);
        buf.put_usize_varint(node_hashes.len());
        for (addr, &hash) in node_hashes {
            addr.ser(buf);
            buf.put_u64_le(hash);
        }
        addr_serializer.finalize(buf);
    }

    fn deser_gossip_hi(mut buf: impl Buf) -> anyhow::Result<ClusterMessage> {
        // discriminator is already consumed

        let nonce = buf.try_get_u32()?;

        let addr_deserializer = NodeAddressesDeserializer::new(&mut buf)?;
        let mut nodes: FxHashMap<NodeAddr, u64> = Default::default();

        let num_nodes = buf.try_get_usize_varint()?;
        for _ in 0..num_nodes {
            let addr = addr_deserializer.get_node_add(&mut buf)?;
            let hash = buf.try_get_u64()?;
            let _ = nodes.insert(addr, hash);
        }

        Ok(ClusterMessage::GossipHi(GossipSummaryData {
            nonce,
            nodes,
        }))
    }

    fn ser_gossip_reply(nodes_with_updates: &[NodeMembershipState], requested_updates: &[NodeAddr], buf: &mut BytesMut) {
        buf.put_u8(Self::ID_GOSSIP_REPLY);

        todo!()
    }

    fn deser_gossip_reply(mut buf: impl Buf) -> anyhow::Result<ClusterMessage> {
        // discriminator already consumed

        let mut nodes_with_updates = Default::default();
        let mut requested_updates = Default::default();

        //TODO
        //TODO
        //TODO

        Ok(ClusterMessage::GossipReply(GossipReplyData {
            nodes_with_updates,
            requested_updates,
        }))
    }
    fn deser_gossip_data(mut buf: impl Buf) -> anyhow::Result<ClusterMessage> {
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

    async fn on_hi(&self, envelope: &Envelope, data: GossipSummaryData, messaging: Arc<Messaging>) -> anyhow::Result<()> {
        let self_hashes = self.cluster.read().await
            .hash_for_equality_check(data.nonce);

        let mut buf = BytesMut::new();
        todo!("add message discriminator");

        let mut addr_serializer = NodeAddressesSerializer::new(&mut buf);

        // send data for nodes with a different hash, or that only exist locally...
        let mut provided_addresses = self_hashes.iter()
            .filter(|(addr, &hash)| Some(&hash) != data.nodes.get(addr))
            .map(|(addr, _)| addr)
            .collect::<Vec<_>>();

        provided_addresses.extend(self_hashes.keys()
            .filter(|addr| !data.nodes.contains_key(addr))
        );

        {
            let cluster = self.cluster.read().await;
            buf.put_u64_varint(provided_addresses.len() as u64); //TODO put_usize_varint
            for addr in provided_addresses {
                addr_serializer.put_node_addr(&mut buf, addr.clone());

                let s = cluster.get_membership_state(addr.clone())
                    .expect("the key was present when we calculated local hashes just now");

                buf.put_u8(s.state.into());
                buf.put_u64_varint(s.seen_by.len() as u64); //TODO put_usize_varint
                for seen_by in &s.seen_by {
                    addr_serializer.put_node_addr(&mut buf, seen_by.clone());
                }
            }
        }

        // ... and request data for nodes that only exist on the other node (NB: if we send data
        //  for a node, we request data for that node implicitly)

        let requested_addresses = data.nodes.into_keys()
            .filter(|addr| !self_hashes.contains_key(addr))
            .collect::<Vec<_>>();

        buf.put_u64_varint(requested_addresses.len() as u64); //TODO put_usize_varint
        for addr in requested_addresses {
            addr.ser(&mut buf);
        }

        addr_serializer.finalize(&mut buf);

        messaging.send(envelope.from, self.id(), &buf).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageModule for ClusterMessageModule {
    fn id(&self) -> MessageModuleId {
        Self::ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        let msg = ClusterMessage::deser(buf);

        // match msg {
        //     ClusterMessage::GossipHi(data) => self.on_hi(data).await,
        //     ClusterMessage::GossipReply(data) => todo!(),
        //     ClusterMessage::GossipData(data) => todo!(),
        // }
    }
}

/// Messages containing 'seen by' lists contain the same node addresses multiple times. This struct
///  represents them as a number, adding a resolution table.
pub struct NodeAddressesSerializer {
    offs_for_offs: usize,
    resolution_table: FxHashMap<NodeAddr, usize>,
    reverse_resolution_table: Vec<NodeAddr>,
}
impl NodeAddressesSerializer {
    pub fn new(buf: &mut BytesMut) -> NodeAddressesSerializer {
        let offs_for_offs = buf.len();
        buf.put_u32_le(0); // placeholder for the offset of the resolution table we write at the end
        NodeAddressesSerializer {
            offs_for_offs,
            resolution_table: Default::default(),
            reverse_resolution_table: Vec::new(),
        }
    }

    pub fn put_node_addr(&mut self, buf: &mut BytesMut, addr: NodeAddr) {
        let prev_len = self.resolution_table.len();

        let addr_id = match self.resolution_table.entry(addr) {
            Entry::Occupied(e) => {
                *e.get()
            }
            Entry::Vacant(mut e) => {
                let id = prev_len;
                self.reverse_resolution_table.push(addr.clone());
                *e.insert(id)
            }
        };
        buf.put_u64_varint(addr_id as u64); //TODO add support for put_usize_varint
    }

    pub fn finalize(self, buf: &mut BytesMut) {
        // overwrite the placeholder with the actual offset of the resolution table
        let offs_resolution_table = buf.len() as u32; //TODO overflow
        (&mut buf[self.offs_for_offs..]).put_u32_le(offs_resolution_table);

        // write the resolution table
        buf.put_u64_varint(self.resolution_table.len() as u64); //TODO add support for put_usize_varint
        for addr in self.reverse_resolution_table {
            // nodes are serialized in the order of their ids, so there is no need to store the id explicitly
            addr.ser(buf);
        }
    }
}

pub struct NodeAddressesDeserializer {
    resolution_table: Vec<NodeAddr>,
}
impl NodeAddressesDeserializer {
    pub fn new(mut buf: &mut impl Buf) -> anyhow::Result<NodeAddressesDeserializer> {
        let initial = buf.remaining();
        let offs_resolution_table = buf.get_u64_varint().a()? as usize; //TODO get_usize_varint()
        let len_of_offset = initial - buf.remaining();

        let offs_resolution_table = offs_resolution_table.checked_sub(len_of_offset)
            .ok_or(anyhow!("offset must point after the offset itself"))?;

        //NB: We want to consume the offset to the resolution table, but not the resolution table
        //     itself since it comes after the actual message
        let raw = buf.chunk();
        if offs_resolution_table >= raw.len() {
            return Err(anyhow!("offset of resolution table points after the end of the buffer"));
        }

        let mut buf_resolution_table = &raw[offs_resolution_table..];
        let num_addresses = buf_resolution_table.get_u64_varint().a()? as usize; //TODO get_usize_varint()

        let mut resolution_table = Vec::with_capacity(num_addresses);
        for _ in 0..num_addresses {
            resolution_table.push(NodeAddr::deser(&buf_resolution_table)?);
        }

        Ok(NodeAddressesDeserializer {
            resolution_table,
        })
    }

    pub fn get_node_add(&self, mut buf: impl Buf) -> anyhow::Result<NodeAddr> {
        let id = buf.get_u64_varint().a()? as usize; //TODO get_usize_varint
        if let Some(node_addr) = self.resolution_table.get(id) {
            return Ok(node_addr.clone());
        }
        return Err(anyhow!("node address index out of bounds"));
    }
}

