use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use num_enum::TryFromPrimitive;
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;
use tracing::error;

use crate::cluster::cluster_state::{MembershipState, NodeReachability, NodeState};
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::messaging::envelope::Envelope;
use crate::messaging::message_module::{MessageModule, MessageModuleId};
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;
use crate::util::buf_ext::{BufExt, BufMutExt, DummyErrorAdapter};

pub const CLUSTER_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"Cluster\0");

pub struct ClusterMessageModule {
    gossip: Arc<RwLock<Gossip>>,
    messaging: Arc<RwLock<Messaging>>,
    heart_beat: Arc<RwLock<HeartBeat>>,
}
impl ClusterMessageModule {
    async fn reply(&self, envelope: &Envelope, message: ClusterMessage) {
        let mut buf = BytesMut::new();
        message.ser(&mut buf);
        let _ = self.messaging.read().await
            .send(envelope.from, CLUSTER_MESSAGE_MODULE_ID, &buf).await;
    }

    async fn _on_message(&self, envelope: &Envelope, buf: &[u8]) -> anyhow::Result<()> {
        use ClusterMessage::*;

        match ClusterMessage::deser(buf)? {
            GossipSummaryDigest(digest) => {
                if let Some(response) = self.gossip.read().await
                    .on_summary_digest(&digest).await
                {
                    Ok(self.reply(envelope, GossipDetailedDigest(response)).await)
                }
                else { Ok(()) }
            }
            GossipDetailedDigest(digest) => {
                if let Some(response) = self.gossip.read().await
                    .on_detailed_digest(&digest).await
                {
                    Ok(self.reply(envelope, GossipDifferingAndMissingNodesData(response)).await)
                }
                else { Ok(()) }
            }
            GossipDifferingAndMissingNodesData(data) => {
                if let Some(response) = self.gossip.read().await
                    .on_differing_and_missing_nodes(data).await
                {
                    Ok(self.reply(envelope, GossipNodesData(response)).await)
                }
                else { Ok(()) }
            }
            GossipNodesData(data) => {
                Ok(
                    self.gossip.read().await
                        .on_nodes(data).await
                )
            }
            Heartbeat(data) => {
                //TODO document heartbeat protocol
                //TODO documentation here
                Ok(
                    self.reply(envelope, HeartbeatResponse(HeartbeatResponseData {
                        counter: data.counter,
                        timestamp_nanos: data.timestamp_nanos,
                    })).await
                )
            }
            HeartbeatResponse(data) => {
                Ok(
                    self.heart_beat.write().await
                        .on_heartbeat_response(&data, envelope.from)
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageModule for ClusterMessageModule {
    fn id(&self) -> MessageModuleId {
        CLUSTER_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        if let Err(e) = self._on_message(envelope, buf).await {
            error!("error deserializing message: {}", e);
        }
    }
}



const ID_GOSSIP_SUMMARY_DIGEST: u8 = 1;
const ID_GOSSIP_DETAILED_DIGEST: u8 = 2;
const ID_GOSSIP_DIFFERING_AND_MISSING_NODES: u8 = 3;
const ID_GOSSIP_NODES: u8 = 4;
const ID_HEARTBEAT: u8 = 5;
const ID_HEARTBEAT_RESPONSE: u8 = 6;

pub enum ClusterMessage {
    GossipSummaryDigest(GossipSummaryDigestData),
    GossipDetailedDigest(GossipDetailedDigestData),
    GossipDifferingAndMissingNodesData(GossipDifferingAndMissingNodesData),
    GossipNodesData(GossipNodesData),
    Heartbeat(HeartbeatData),
    HeartbeatResponse(HeartbeatResponseData),
}
impl ClusterMessage {
    fn id(&self) -> u8 {
        match self {
            ClusterMessage::GossipSummaryDigest(_) => ID_GOSSIP_SUMMARY_DIGEST,
            ClusterMessage::GossipDetailedDigest(_) => ID_GOSSIP_DETAILED_DIGEST,
            ClusterMessage::GossipDifferingAndMissingNodesData(_) => ID_GOSSIP_DIFFERING_AND_MISSING_NODES,
            ClusterMessage::GossipNodesData(_) => ID_GOSSIP_NODES,
            ClusterMessage::Heartbeat(_) => ID_HEARTBEAT,
            ClusterMessage::HeartbeatResponse(_) => ID_HEARTBEAT_RESPONSE,
        }
    }

    //TODO unit test
    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u8(self.id());
        match self {
            ClusterMessage::GossipSummaryDigest(data) => Self::ser_gossip_summary_digest(data, buf),
            ClusterMessage::GossipDetailedDigest(data) => Self::ser_gossip_detailed_digest(data, buf),
            ClusterMessage::GossipDifferingAndMissingNodesData(data) => Self::ser_gossip_differing_and_missing_nodes(data, buf),
            ClusterMessage::GossipNodesData(data) => Self::ser_gossip_nodes(data, buf),
            ClusterMessage::Heartbeat(data) => Self::ser_heartbeat(data, buf),
            ClusterMessage::HeartbeatResponse(data) => Self::ser_heartbeat_response(data, buf),
        }
    }

    fn ser_gossip_summary_digest(data: &GossipSummaryDigestData, buf: &mut impl BufMut) {
        for i in 0..data.full_sha256_digest.len() {
            buf.put_u8(data.full_sha256_digest[i]);
        }
    }

    fn ser_gossip_detailed_digest(data: &GossipDetailedDigestData, buf: &mut impl BufMut) {
        buf.put_u32_le(data.nonce);
        buf.put_usize_varint(data.nodes.len());
        for (addr, &hash) in &data.nodes {
            addr.ser(buf);
            buf.put_u64_le(hash);
        }
    }

    fn ser_gossip_differing_and_missing_nodes(data: &GossipDifferingAndMissingNodesData, buf: &mut BytesMut) {
        let mut addr_pool = NodeAddrPoolSerializer::new(buf);
        let mut string_pool = StringPoolSerializer::new(buf);

        buf.put_usize_varint(data.differing.len());
        for s in &data.differing {
            addr_pool.put_node_addr(buf, s.addr);
            buf.put_u8(s.membership_state.into());
            string_pool.put_string_set(buf, s.roles.iter());
            Self::ser_reachability(&s.reachability, buf, &mut addr_pool);
            addr_pool.put_addr_set(buf, s.seen_by.iter());
        }

        addr_pool.put_addr_set(buf, data.missing.iter());
        addr_pool.finalize(buf);
        string_pool.finalize(buf);
    }

    fn ser_gossip_nodes(data: &GossipNodesData, buf: &mut BytesMut) {
        let mut addr_pool = NodeAddrPoolSerializer::new(buf);
        let mut string_pool = StringPoolSerializer::new(buf);

        buf.put_usize_varint(data.nodes.len());
        for s in &data.nodes {
            addr_pool.put_node_addr(buf, s.addr);
            buf.put_u8(s.membership_state.into());
            string_pool.put_string_set(buf, s.roles.iter());
            Self::ser_reachability(&s.reachability, buf, &mut addr_pool);
            addr_pool.put_addr_set(buf, s.seen_by.iter());
        }

        addr_pool.finalize(buf);
        string_pool.finalize(buf);
    }

    fn ser_heartbeat(data: &HeartbeatData, buf: &mut impl BufMut) {
        buf.put_u32_le(data.counter);
        buf.put_u64_le(data.timestamp_nanos);
    }

    fn ser_heartbeat_response(data: &HeartbeatResponseData, buf: &mut impl BufMut) {
        buf.put_u32_le(data.counter);
        buf.put_u64_le(data.timestamp_nanos);
    }

    fn ser_reachability(reachability: &FxHashMap<NodeAddr, NodeReachability>, buf: &mut BytesMut, addr_pool: &mut NodeAddrPoolSerializer) {
        buf.put_usize_varint(reachability.len());

        for (&addr, node_reachability) in reachability {
            addr_pool.put_node_addr(buf, addr);
            buf.put_u32_varint(node_reachability.counter_of_reporter);
            buf.put_u8(if node_reachability.is_reachable { 1 } else { 0 });
        }
    }

    fn try_deser_reachability(buf: &mut impl Buf, addr_pool: &NodeAddrPoolDeserializer) -> anyhow::Result<FxHashMap<NodeAddr, NodeReachability>> {
        let num_entries = buf.try_get_usize_varint()?;

        let mut reachability = FxHashMap::default();
        for _ in 0..num_entries {
            let addr = addr_pool.try_get_node_addr(buf)?;
            let counter_of_reporter = buf.try_get_u32_le()?;
            let is_reachable = match buf.try_get_u8()? {
                0 => false,
                1 => true,
                b => return Err(anyhow!("invalid value for a boolean: {}", b)),
            };
            let _ = reachability.insert(addr, NodeReachability {
                counter_of_reporter,
                is_reachable,
            });
        }

        Ok(reachability)
    }

    //TODO &mut impl Buf
    pub fn deser(buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let mut buf = buf;
        match buf.try_get_u8()? {
            ID_GOSSIP_SUMMARY_DIGEST => Self::deser_gossip_summary_digest(buf),
            ID_GOSSIP_DETAILED_DIGEST => Self::deser_gossip_detailed_digest(buf),
            ID_GOSSIP_DIFFERING_AND_MISSING_NODES => Self::deser_gossip_differing_and_missing_nodes(buf),
            ID_GOSSIP_NODES => Self::deser_gossip_nodes(buf),
            ID_HEARTBEAT => Self::deser_heartbeat(buf),
            ID_HEARTBEAT_RESPONSE => Self::deser_heartbeat_response(buf),
            id => Err(anyhow!("invalid message discriminator {}", id)),
        }
    }

    fn deser_gossip_summary_digest(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let mut full_sha256_digest = [0u8; 32];
        for i in 0..full_sha256_digest.len() {
            full_sha256_digest[i] = buf.try_get_u8()?;
        }

        Ok(ClusterMessage::GossipSummaryDigest(GossipSummaryDigestData {
            full_sha256_digest,
        }))
    }

    fn deser_gossip_detailed_digest(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let nonce = buf.try_get_u32_le()?;

        let num_nodes = buf.try_get_usize_varint()?;
        let mut nodes = FxHashMap::default();
        for _ in 0..num_nodes {
            let addr = NodeAddr::try_deser(&mut buf)?;
            let hash = buf.try_get_u64_le()?;
            let _ = nodes.insert(addr, hash);
        }

        Ok(ClusterMessage::GossipDetailedDigest(GossipDetailedDigestData {
            nonce,
            nodes,
        }))
    }

    fn deser_gossip_differing_and_missing_nodes(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let addr_pool = NodeAddrPoolDeserializer::new(&mut buf)?;
        let string_pool = StringPoolDeserializer::new(&mut buf)?;

        let num_differing = buf.try_get_usize_varint()?;
        let mut differing = Vec::with_capacity(num_differing);
        for _ in 0..num_differing {
            let addr = NodeAddr::try_deser(&mut buf)?;
            let membership_state = MembershipState::try_from_primitive(buf.try_get_u8()?)?;
            let roles = string_pool.try_get_string_set(&mut buf)?;
            let reachability= Self::try_deser_reachability(&mut buf, &addr_pool)?;
            let seen_by = addr_pool.try_get_addr_set(&mut buf)?;

            differing.push(NodeState {
                addr,
                membership_state,
                roles,
                reachability,
                seen_by,
            })
        }

        let missing = addr_pool.try_get_addr_set(&mut buf)?;

        Ok(ClusterMessage::GossipDifferingAndMissingNodesData(GossipDifferingAndMissingNodesData {
            differing,
            missing,
        }))
    }

    fn deser_gossip_nodes(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let addr_pool = NodeAddrPoolDeserializer::new(&mut buf)?;
        let string_pool = StringPoolDeserializer::new(&mut buf)?;

        let num_nodes = buf.try_get_usize_varint()?;
        let mut nodes = Vec::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            let addr = NodeAddr::try_deser(&mut buf)?;
            let membership_state = MembershipState::try_from_primitive(buf.try_get_u8()?)?;
            let roles = string_pool.try_get_string_set(&mut buf)?;
            let reachability= Self::try_deser_reachability(&mut buf, &addr_pool)?;
            let seen_by = addr_pool.try_get_addr_set(&mut buf)?;

            nodes.push(NodeState {
                addr,
                membership_state,
                roles,
                reachability,
                seen_by,
            })
        }

        Ok(ClusterMessage::GossipNodesData(GossipNodesData {
            nodes,
        }))
    }

    fn deser_heartbeat(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let counter = buf.try_get_u32_le()?;
        let timestamp_nanos = buf.try_get_u64_le()?;

        Ok(ClusterMessage::Heartbeat(HeartbeatData {
            counter,
            timestamp_nanos,
        }))
    }

    fn deser_heartbeat_response(mut buf: &[u8]) -> anyhow::Result<ClusterMessage> {
        let counter = buf.try_get_u32_le()?;
        let timestamp_nanos = buf.try_get_u64_le()?;

        Ok(ClusterMessage::HeartbeatResponse(HeartbeatResponseData {
            counter,
            timestamp_nanos,
        }))
    }
}


pub struct GossipSummaryDigestData {
    pub full_sha256_digest: [u8;32],
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


/// Messages can contain the same NodeAddr values numerous times (e.g. in 'seen by' sets). This
///  serializer writes a single translation table associating each NodeAddr with a small ID number,
///  and serializes NodeAddr instances to their IDs to save storage space.
///
/// TODO bit sets for sets of NodeAddr?
struct NodeAddrPoolSerializer {
    offs_for_offs: usize,
    resolution_table: FxHashMap<NodeAddr, usize>,
    reverse_resolution_table: Vec<NodeAddr>,
}
impl NodeAddrPoolSerializer {
    pub fn new(buf: &mut BytesMut) -> NodeAddrPoolSerializer {
        let offs_for_offs = buf.len();
        buf.put_u32_le(0); // placeholder for the offset of the resolution table we write at the end
        NodeAddrPoolSerializer {
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

    pub fn put_addr_set<'a>(&mut self, buf: &mut BytesMut, addrs: impl ExactSizeIterator<Item = &'a NodeAddr>) {
        //TODO represent as a bit set
        buf.put_usize_varint(addrs.len());

        for &a in addrs {
            self.put_node_addr(buf, a);
        }
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

struct NodeAddrPoolDeserializer {
    resolution_table: Vec<NodeAddr>,
}
impl NodeAddrPoolDeserializer {
    pub fn new(mut buf: &mut impl Buf) -> anyhow::Result<NodeAddrPoolDeserializer> {
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
            resolution_table.push(NodeAddr::try_deser(&mut buf_resolution_table)?);
        }

        Ok(NodeAddrPoolDeserializer {
            resolution_table,
        })
    }

    pub fn try_get_node_addr(&self, buf: &mut impl Buf) -> anyhow::Result<NodeAddr> {
        let id = buf.get_u64_varint().a()? as usize; //TODO get_usize_varint
        if let Some(node_addr) = self.resolution_table.get(id) {
            return Ok(node_addr.clone());
        }
        return Err(anyhow!("node address index out of bounds"));
    }

    pub fn try_get_addr_set<T: FromIterator<NodeAddr>>(&self, buf: &mut impl Buf) -> anyhow::Result<T> {
        let len = buf.try_get_usize_varint()?;

        let mut addrs = Vec::with_capacity(len);
        for _ in 0..len {
            addrs.push(self.try_get_node_addr(buf)?);
        }
        Ok(addrs.into_iter().collect())
    }
}


struct StringPoolSerializer<'a> {
    offs_for_offs: usize,
    resolution_table: FxHashMap<&'a str, usize>,
    reverse_resolution_table: Vec<&'a str>,
}
impl <'a> StringPoolSerializer<'a> {
    pub fn new<'b>(buf: &'b mut BytesMut) -> StringPoolSerializer<'a> {
        let offs_for_offs = buf.len();
        buf.put_u32_le(0); // placeholder for the offset of the resolution table we write at the end
        StringPoolSerializer {
            offs_for_offs,
            resolution_table: Default::default(),
            reverse_resolution_table: Vec::new(),
        }
    }

    pub fn put_string(&mut self, buf: &mut BytesMut, s: &'a str) {
        let prev_len = self.resolution_table.len();

        let s_id = match self.resolution_table.entry(s) {
            Entry::Occupied(e) => {
                *e.get()
            }
            Entry::Vacant(mut e) => {
                let id = prev_len;
                self.reverse_resolution_table.push(s);
                *e.insert(id)
            }
        };
        buf.put_u64_varint(s_id as u64); //TODO add support for put_usize_varint
    }

    pub fn put_string_set(&mut self, buf: &mut BytesMut, strings: impl ExactSizeIterator<Item = &'a String>) {
        buf.put_usize_varint(strings.len());
        for s in strings {
            self.put_string(buf, s);
        }
    }

    pub fn finalize(self, buf: &mut BytesMut) {
        // overwrite the placeholder with the actual offset of the resolution table
        let offs_resolution_table = buf.len() as u32; //TODO overflow
        (&mut buf[self.offs_for_offs..]).put_u32_le(offs_resolution_table);

        // write the resolution table
        buf.put_u64_varint(self.resolution_table.len() as u64); //TODO add support for put_usize_varint
        for s in self.reverse_resolution_table {
            // strings are serialized in the order of their ids, so there is no need to store the id explicitly
            buf.put_string(s);
        }
    }
}

struct StringPoolDeserializer {
    resolution_table: Vec<String>,
}
impl StringPoolDeserializer {
    pub fn new(mut buf: &mut impl Buf) -> anyhow::Result<StringPoolDeserializer> {
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
        let num_entries = buf_resolution_table.get_u64_varint().a()? as usize; //TODO get_usize_varint()

        let mut resolution_table = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            resolution_table.push(buf_resolution_table.try_get_string()?);
        }

        Ok(StringPoolDeserializer {
            resolution_table,
        })
    }

    pub fn try_get_string(&self, buf: &mut impl Buf) -> anyhow::Result<String> {
        let id = buf.get_u64_varint().a()? as usize; //TODO get_usize_varint
        if let Some(s) = self.resolution_table.get(id) {
            return Ok(s.clone());
        }
        return Err(anyhow!("index out of bounds"));
    }

    pub fn try_get_string_set<T: FromIterator<String>>(&self, buf: &mut impl Buf) -> anyhow::Result<T> {
        let len = buf.try_get_usize_varint()?;

        let mut strings = Vec::with_capacity(len);
        for _ in 0..len {
            strings.push(self.try_get_string(buf)?);
        }
        Ok(strings.into_iter().collect())
    }
}
