use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;
use crate::messaging::envelope::Checksum;
use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;

#[derive(Debug, Clone)]
pub struct PacketHeader {
    checksum: Checksum,  // covers everything after the checksum field, including the rest of the packet header
    pub packet_counter: u64, // with well-known value for 'fire and forget'
    pub from: NodeAddr,
    pub to: NodeAddr,
    /// The offset of the first message in th this packet, starting *after* the packet header:
    /// * 0000: starts with a new message, no continuation from previous packet
    /// * xxxx: offset to the first new message header - everything before that is the end of a message continued from the previous packet
    ///          NB: This can point to the end of the packet, meaning that this message is complete but the next message starts in the next packet
    /// * FFFF: long message, continued from previous packet and continued in the next packet
    pub new_message_offset: u16, //TODO not for 'fire and forget
}

impl PacketHeader {

    pub const FIRE_AND_FORGET_PACKET_COUNTER: u64 = u64::MAX;

    pub const OFFSET_CONTINUED_FRAGMENT_SEQUENCE: u16 = 0xFFFF;

    pub fn new(from: NodeAddr, to: NodeAddr, packet_counter: u64) -> Self {
        PacketHeader {
            checksum: Checksum(0),
            packet_counter,
            new_message_offset: 0,
            from,
            to,
        }
    }

    pub fn patch_message_offset(buf: &mut BytesMut, new_message_offset: u16) {
        (&mut buf[2*size_of::<u64>()..]).put_u16(new_message_offset);
    }

    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.checksum.0);
        buf.put_u64(self.packet_counter);
        buf.put_u16(self.new_message_offset);
        self.from.ser(buf);
        buf.put_u32(self.to.unique);
    }

    pub(crate) fn try_parse(p0: &mut impl Buf) -> anyhow::Result<PacketHeader> {
        todo!()
    }
}


pub struct MessageHeader {
    pub message_module_id: MessageModuleId,
    pub message_len: u32,
}
impl MessageHeader {
    pub const SERIALIZED_SIZE: usize = size_of::<u64>() + size_of::<u32>();

    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.message_module_id.0);
        buf.put_u32(self.message_len);
    }

    pub fn try_parse(buf: &mut impl Buf) -> anyhow::Result<MessageHeader> {
        let message_module_id = MessageModuleId(buf.try_get_u64()?);
        let message_len = buf.try_get_u32()?;
        Ok(MessageHeader {
           message_module_id,
            message_len
        })
    }
}
