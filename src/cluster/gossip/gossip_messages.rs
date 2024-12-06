use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;
use num_enum::TryFromPrimitive;
use tokio::sync::mpsc;
use tracing::error;

use crate::cluster::cluster_state::{MembershipState, NodeReachability, NodeState};
use crate::messaging::envelope::Envelope;
use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use crate::util::buf::{put_string, try_get_string};

pub(crate) const GOSSIP_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"CtrGossp");

//TODO make the message module infrastructure reusable

pub struct GossipMessageModule {
    gossip: mpsc::Sender<(NodeAddr, GossipMessage)>,
}
impl GossipMessageModule {
    pub fn new(gossip: mpsc::Sender<(NodeAddr, GossipMessage)>) -> Arc<GossipMessageModule> {
        Arc::new({
            GossipMessageModule {
                gossip,
            }
        })
    }

    async fn _on_message(&self, envelope: &Envelope, buf: &[u8]) -> anyhow::Result<()> {
        let msg = GossipMessage::deser(buf)?;
        self.gossip.send((envelope.from, msg)).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageModule for GossipMessageModule {
    fn id(&self) -> MessageModuleId {
        GOSSIP_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        if let Err(e) = self._on_message(envelope, buf).await {
            error!("error deserializing message: {}", e);
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum GossipMessage {
    GossipSummaryDigest(GossipSummaryDigestData),
    GossipDetailedDigest(GossipDetailedDigestData),
    GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData),
    GossipNodes(GossipNodesData),
    DownYourself,
}
impl Message for GossipMessage {
    fn module_id(&self) -> MessageModuleId {
        GOSSIP_MESSAGE_MODULE_ID
    }

    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u8(self.id());
        match self {
            GossipMessage::GossipSummaryDigest(data) => Self::ser_gossip_summary_digest(data, buf),
            GossipMessage::GossipDetailedDigest(data) => Self::ser_gossip_detailed_digest(data, buf),
            GossipMessage::GossipDifferingAndMissingNodes(data) => Self::ser_gossip_differing_and_missing_nodes(data, buf),
            GossipMessage::GossipNodes(data) => Self::ser_gossip_nodes(data, buf),
            GossipMessage::DownYourself => {}
        }
    }

    fn box_clone(&self) -> Box<dyn Message> {
        Box::new(self.clone())
    }
}


const ID_GOSSIP_SUMMARY_DIGEST: u8 = 1;
const ID_GOSSIP_DETAILED_DIGEST: u8 = 2;
const ID_GOSSIP_DIFFERING_AND_MISSING_NODES: u8 = 3;
const ID_GOSSIP_NODES: u8 = 4;
const ID_DOWN_YOURSELF: u8 = 5;

impl GossipMessage {
    pub fn id(&self) -> u8 {
        match self {
            GossipMessage::GossipSummaryDigest(_) => ID_GOSSIP_SUMMARY_DIGEST,
            GossipMessage::GossipDetailedDigest(_) => ID_GOSSIP_DETAILED_DIGEST,
            GossipMessage::GossipDifferingAndMissingNodes(_) => ID_GOSSIP_DIFFERING_AND_MISSING_NODES,
            GossipMessage::GossipNodes(_) => ID_GOSSIP_NODES,
            GossipMessage::DownYourself => ID_DOWN_YOURSELF,
        }
    }

    fn ser_gossip_summary_digest(data: &GossipSummaryDigestData, buf: &mut impl BufMut) {
        for i in 0..data.full_sha256_digest.len() {
            buf.put_u8(data.full_sha256_digest[i]);
        }
    }

    fn ser_gossip_detailed_digest(data: &GossipDetailedDigestData, buf: &mut impl BufMut) {
        buf.put_u32(data.nonce);
        buf.put_usize_varint(data.nodes.len());
        for (addr, &hash) in &data.nodes {
            addr.ser(buf);
            buf.put_u64(hash);
        }
    }

    fn ser_gossip_differing_and_missing_nodes(data: &GossipDifferingAndMissingNodesData, buf: &mut BytesMut) {
        let mut addr_pool = NodeAddrPoolSerializer::new(buf);
        let mut string_pool = StringPoolSerializer::new(buf);

        Self::_ser_node_states(&data.differing, &mut addr_pool, &mut string_pool, buf);

        addr_pool.put_addr_set(buf, data.missing.iter());
        addr_pool.finalize(buf);
        string_pool.finalize(buf);
    }

    fn _ser_node_states<'a> (
        nodes: &'a [NodeState],
        addr_pool: &mut NodeAddrPoolSerializer,
        string_pool: &mut StringPoolSerializer<'a>,
        buf: &mut BytesMut)
    {
        buf.put_usize_varint(nodes.len());
        for s in nodes {
            addr_pool.put_node_addr(buf, s.addr);
            buf.put_u8(s.membership_state.into());
            string_pool.put_string_set(buf, s.roles.iter());
            Self::ser_reachability(&s.reachability, buf, addr_pool);
            addr_pool.put_addr_set(buf, s.seen_by.iter());
        }
    }

    fn ser_gossip_nodes(data: &GossipNodesData, buf: &mut BytesMut) {
        let mut addr_pool = NodeAddrPoolSerializer::new(buf);
        let mut string_pool = StringPoolSerializer::new(buf);

        Self::_ser_node_states(&data.nodes, &mut addr_pool, &mut string_pool, buf);

        addr_pool.finalize(buf);
        string_pool.finalize(buf);
    }

    fn ser_reachability(reachability: &BTreeMap<NodeAddr, NodeReachability>, buf: &mut BytesMut, addr_pool: &mut NodeAddrPoolSerializer) {
        buf.put_usize_varint(reachability.len());

        for (&addr, node_reachability) in reachability {
            addr_pool.put_node_addr(buf, addr);
            buf.put_u32_varint(node_reachability.counter_of_reporter);
            buf.put_u8(if node_reachability.is_reachable { 1 } else { 0 });
        }
    }

    fn try_deser_reachability(buf: &mut impl Buf, addr_pool: &NodeAddrPoolDeserializer) -> anyhow::Result<BTreeMap<NodeAddr, NodeReachability>> {
        let num_entries = buf.try_get_usize_varint()?;

        let mut reachability = BTreeMap::default();
        for _ in 0..num_entries {
            let addr = addr_pool.try_get_node_addr(buf)?;
            let counter_of_reporter = buf.try_get_u32_varint()?;
            let is_reachable = match buf.try_get_u8()? {
                0 => false,
                1 => true,
                b => return Err(anyhow!("invalid value for a boolean: {}", b)),
            };
            reachability.insert(addr, NodeReachability {
                counter_of_reporter,
                is_reachable,
            });
        }

        Ok(reachability)
    }

    //TODO &mut impl Buf
    pub fn deser(buf: &[u8]) -> anyhow::Result<GossipMessage> {
        let mut buf = buf;
        match buf.try_get_u8()? {
            ID_GOSSIP_SUMMARY_DIGEST => Self::deser_gossip_summary_digest(buf),
            ID_GOSSIP_DETAILED_DIGEST => Self::deser_gossip_detailed_digest(buf),
            ID_GOSSIP_DIFFERING_AND_MISSING_NODES => Self::deser_gossip_differing_and_missing_nodes(buf),
            ID_GOSSIP_NODES => Self::deser_gossip_nodes(buf),
            ID_DOWN_YOURSELF => Ok(GossipMessage::DownYourself),
            id => Err(anyhow!("invalid message discriminator {}", id)),
        }
    }

    fn deser_gossip_summary_digest(mut buf: &[u8]) -> anyhow::Result<GossipMessage> {
        let mut full_sha256_digest = [0u8; 32];
        for i in 0..full_sha256_digest.len() {
            full_sha256_digest[i] = buf.try_get_u8()?;
        }

        Ok(GossipMessage::GossipSummaryDigest(GossipSummaryDigestData {
            full_sha256_digest,
        }))
    }

    fn deser_gossip_detailed_digest(mut buf: &[u8]) -> anyhow::Result<GossipMessage> {
        let nonce = buf.try_get_u32()?;

        let num_nodes = buf.try_get_usize_varint()?;
        let mut nodes = BTreeMap::default();
        for _ in 0..num_nodes {
            let addr = NodeAddr::try_deser(&mut buf)?;
            let hash = buf.try_get_u64()?;
            nodes.insert(addr, hash);
        }

        Ok(GossipMessage::GossipDetailedDigest(GossipDetailedDigestData {
            nonce,
            nodes,
        }))
    }

    fn deser_gossip_differing_and_missing_nodes(mut buf: &[u8]) -> anyhow::Result<GossipMessage> {
        let addr_pool = NodeAddrPoolDeserializer::new(&mut buf)?;
        let string_pool = StringPoolDeserializer::new(&mut buf)?;

        let differing = Self::_deser_node_states(&addr_pool, &string_pool, &mut buf)?;
        let missing = addr_pool.try_get_addr_set(&mut buf)?;

        Ok(GossipMessage::GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
            differing,
            missing,
        }))
    }

    fn _deser_node_states(addr_pool: &NodeAddrPoolDeserializer, string_pool: &StringPoolDeserializer, buf: &mut impl Buf) -> anyhow::Result<Vec<NodeState>> {
        let num_differing = buf.try_get_usize_varint()?;
        let mut result = Vec::with_capacity(num_differing);
        for _ in 0..num_differing {
            let addr = addr_pool.try_get_node_addr(buf)?;
            let membership_state = MembershipState::try_from_primitive(buf.try_get_u8()?)?;
            let roles = string_pool.try_get_string_set(buf)?;
            let reachability= Self::try_deser_reachability(buf, &addr_pool)?;
            let seen_by = addr_pool.try_get_addr_set(buf)?;

            result.push(NodeState {
                addr,
                membership_state,
                roles,
                reachability,
                seen_by,
            })
        }
        Ok(result)
    }

    fn deser_gossip_nodes(mut buf: &[u8]) -> anyhow::Result<GossipMessage> {
        let addr_pool = NodeAddrPoolDeserializer::new(&mut buf)?;
        let string_pool = StringPoolDeserializer::new(&mut buf)?;

        let nodes = Self::_deser_node_states(&addr_pool, &string_pool, &mut buf)?;
        Ok(GossipMessage::GossipNodes(GossipNodesData {
            nodes,
        }))
    }
}


#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GossipSummaryDigestData {
    pub full_sha256_digest: [u8;32],
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GossipDetailedDigestData {
    pub nonce: u32,
    pub nodes: BTreeMap<NodeAddr, u64>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GossipDifferingAndMissingNodesData {
    pub differing: Vec<NodeState>,
    pub missing: Vec<NodeAddr>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GossipNodesData {
    pub nodes: Vec<NodeState>,
}



/// Messages can contain the same NodeAddr values numerous times (e.g. in 'seen by' sets). This
///  serializer writes a single translation table associating each NodeAddr with a small ID number,
///  and serializes NodeAddr instances to their IDs to save storage space.
///
/// TODO bit sets for sets of NodeAddr?
struct NodeAddrPoolSerializer {
    offs_for_offs: usize,
    resolution_table: BTreeMap<NodeAddr, usize>,
    reverse_resolution_table: Vec<NodeAddr>,
}
impl NodeAddrPoolSerializer {
    pub fn new(buf: &mut BytesMut) -> NodeAddrPoolSerializer {
        let offs_for_offs = buf.len();
        buf.put_u32(0); // placeholder for the offset of the resolution table we write at the end

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
            Entry::Vacant(e) => {
                let id = prev_len;
                self.reverse_resolution_table.push(addr.clone());
                *e.insert(id)
            }
        };
        buf.put_usize_varint(addr_id);
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
        let offs_resolution_table = (buf.len() - self.offs_for_offs) as u32; //TODO overflow
        (&mut buf[self.offs_for_offs..]).put_u32(offs_resolution_table);

        // write the resolution table
        buf.put_usize_varint(self.resolution_table.len());
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
    pub fn new(buf: &mut impl Buf) -> anyhow::Result<NodeAddrPoolDeserializer> {
        let initial = buf.remaining();
        let offs_resolution_table = buf.try_get_u32()? as usize; //TODO overflow?
        let len_of_offset = initial - buf.remaining();

        let offs_resolution_table = offs_resolution_table.checked_sub(len_of_offset)
            .ok_or(anyhow!("node addr offset must point after the offset itself"))?;

        //NB: We want to consume the offset to the resolution table, but not the resolution table
        //     itself since it comes after the actual message
        let raw = buf.chunk();
        if offs_resolution_table >= raw.len() {
            return Err(anyhow!("offset of node addr resolution table points after the end of the buffer"));
        }

        let mut buf_resolution_table = &raw[offs_resolution_table..];
        let num_addresses = buf_resolution_table.try_get_usize_varint()?;

        let mut resolution_table = Vec::with_capacity(num_addresses);
        for _ in 0..num_addresses {
            resolution_table.push(NodeAddr::try_deser(&mut buf_resolution_table)?);
        }

        Ok(NodeAddrPoolDeserializer {
            resolution_table,
        })
    }

    pub fn try_get_node_addr(&self, buf: &mut impl Buf) -> anyhow::Result<NodeAddr> {
        let id = buf.try_get_usize_varint()?;
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
    resolution_table: BTreeMap<&'a str, usize>,
    reverse_resolution_table: Vec<&'a str>,
}
impl <'a> StringPoolSerializer<'a> {
    pub fn new<'b>(buf: &'b mut BytesMut) -> StringPoolSerializer<'a> {
        let offs_for_offs = buf.len();
        buf.put_u32(0); // placeholder for the offset of the resolution table we write at the end
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
            Entry::Vacant(e) => {
                let id = prev_len;
                self.reverse_resolution_table.push(s);
                *e.insert(id)
            }
        };
        buf.put_usize_varint(s_id);
    }

    pub fn put_string_set(&mut self, buf: &mut BytesMut, strings: impl ExactSizeIterator<Item = &'a String>) {
        buf.put_usize_varint(strings.len());
        for s in strings {
            self.put_string(buf, s);
        }
    }

    pub fn finalize(self, buf: &mut BytesMut) {
        // overwrite the placeholder with the actual offset of the resolution table
        let offs_resolution_table = (buf.len() - self.offs_for_offs) as u32; //TODO overflow
        (&mut buf[self.offs_for_offs..]).put_u32(offs_resolution_table);

        // write the resolution table
        buf.put_usize_varint(self.resolution_table.len());
        for s in self.reverse_resolution_table {
            // strings are serialized in the order of their ids, so there is no need to store the id explicitly
            put_string(buf, s);
        }
    }
}

struct StringPoolDeserializer {
    resolution_table: Vec<String>,
}
impl StringPoolDeserializer {
    pub fn new(buf: &mut impl Buf) -> anyhow::Result<StringPoolDeserializer> {
        let initial = buf.remaining();
        let offs_resolution_table = buf.try_get_u32()? as usize; //TODO overflow?
        let len_of_offset = initial - buf.remaining();

        let offs_resolution_table = offs_resolution_table.checked_sub(len_of_offset)
            .ok_or(anyhow!("string pool offset must point after the offset itself"))?;

        //NB: We want to consume the offset to the resolution table, but not the resolution table
        //     itself since it comes after the actual message
        let raw = buf.chunk();
        if offs_resolution_table >= raw.len() {
            return Err(anyhow!("offset of string pool resolution table points after the end of the buffer"));
        }

        let mut buf_resolution_table = &raw[offs_resolution_table..];
        let num_entries = buf_resolution_table.try_get_usize_varint()?;

        let mut resolution_table = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            resolution_table.push(try_get_string(&mut buf_resolution_table)?);
        }

        Ok(StringPoolDeserializer {
            resolution_table,
        })
    }

    pub fn try_get_string(&self, buf: &mut impl Buf) -> anyhow::Result<String> {
        let id = buf.try_get_usize_varint()?;
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

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use rstest::*;

    use GossipMessage::*;

    use super::*;

    #[rstest]
    #[case::gossip_summary(GossipSummaryDigest(GossipSummaryDigestData { full_sha256_digest: [0u8; 32] }), ID_GOSSIP_SUMMARY_DIGEST)]
    #[case::gossip_detail_empty(GossipDetailedDigest(GossipDetailedDigestData { nonce: 8, nodes: Default::default() }), ID_GOSSIP_DETAILED_DIGEST)]
    #[case::gossip_detail_nodes(GossipDetailedDigest(GossipDetailedDigestData { nonce: 8, nodes: BTreeMap::from_iter([(NodeAddr::localhost(123), 989)]) }), ID_GOSSIP_DETAILED_DIGEST)]
    #[case::gossip_differing_empty(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: Default::default(),
        missing: Default::default(),
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_differing_minimal(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: vec![NodeState {
            addr: NodeAddr::localhost(12),
            membership_state: MembershipState::WeaklyUp,
            reachability: BTreeMap::default(),
            roles: BTreeSet::default(),
            seen_by: BTreeSet::default(),
        }],
        missing: Default::default(),
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_differing_reachability(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: vec![NodeState {
            addr: NodeAddr::localhost(12),
            membership_state: MembershipState::WeaklyUp,
            reachability: BTreeMap::from_iter([
                (NodeAddr::localhost(12), NodeReachability { counter_of_reporter: 99, is_reachable: false, }),
                (NodeAddr::localhost(13), NodeReachability { counter_of_reporter: 0, is_reachable: true, }),
            ]),
            roles: BTreeSet::default(),
            seen_by: BTreeSet::default(),
        }],
        missing: Default::default(),
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_differing_roles(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: vec![NodeState {
            addr: NodeAddr::localhost(12),
            membership_state: MembershipState::WeaklyUp,
            reachability: BTreeMap::default(),
            roles: BTreeSet::from_iter(["abc".to_string(), "xyz".to_string()]),
            seen_by: BTreeSet::default(),
        }],
        missing: Default::default(),
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_differing_seen_by(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: vec![NodeState {
            addr: NodeAddr::localhost(12),
            membership_state: MembershipState::WeaklyUp,
            reachability: BTreeMap::default(),
            roles: BTreeSet::default(),
            seen_by: BTreeSet::from_iter([NodeAddr::localhost(6), NodeAddr::localhost(12)]),
        }],
        missing: Default::default(),
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_missing(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: Default::default(),
        missing: vec![NodeAddr::localhost(12345), NodeAddr::localhost(1234)],
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_differing_differing_full(GossipDifferingAndMissingNodes(GossipDifferingAndMissingNodesData {
        differing: vec![NodeState {
            addr: NodeAddr::localhost(5),
            membership_state: MembershipState::Leaving,
            reachability: BTreeMap::from_iter([
                (NodeAddr::localhost(5), NodeReachability { counter_of_reporter: 99, is_reachable: false, }),
                (NodeAddr::localhost(6), NodeReachability { counter_of_reporter: 0, is_reachable: true, }),
            ]),
            roles: BTreeSet::from_iter(["a".to_string(), "bc".to_string(), "".to_string()]),
            seen_by: BTreeSet::from_iter([NodeAddr::localhost(8), NodeAddr::localhost(5)]),
        }],
        missing: vec![NodeAddr::localhost(5), NodeAddr::localhost(6)],
    }), ID_GOSSIP_DIFFERING_AND_MISSING_NODES)]
    #[case::gossip_nodes_empty(GossipNodes(GossipNodesData {
        nodes: vec![],
    }), ID_GOSSIP_NODES)]
    #[case::gossip_nodes_data(GossipNodes(GossipNodesData {
        nodes: vec![NodeState {
            addr: NodeAddr::localhost(5),
            membership_state: MembershipState::Leaving,
            reachability: BTreeMap::from_iter([
                (NodeAddr::localhost(5), NodeReachability { counter_of_reporter: 99, is_reachable: false, }),
                (NodeAddr::localhost(6), NodeReachability { counter_of_reporter: 0, is_reachable: true, }),
            ]),
            roles: BTreeSet::from_iter(["a".to_string(), "bc".to_string(), "".to_string()]),
            seen_by: BTreeSet::from_iter([NodeAddr::localhost(8), NodeAddr::localhost(5)]),
        }],
    }), ID_GOSSIP_NODES)]
    #[case::down_yourself(DownYourself, ID_DOWN_YOURSELF)]
    fn test_ser_gossip_message(#[case] msg: GossipMessage, #[case] msg_id: u8) {
        assert_eq!(msg.id(), msg_id);

        let mut buf = BytesMut::new();
        msg.ser(&mut buf);
        println!("S {:?}", buf);
        let deser_msg = GossipMessage::deser(&buf).unwrap();
        assert_eq!(msg, deser_msg);
    }
}


