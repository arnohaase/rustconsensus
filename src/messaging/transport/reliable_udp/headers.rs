use bytes::{BufMut, BytesMut};
use crate::messaging::envelope::Checksum;
use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;

pub struct PacketHeader {
    checksum: Checksum,  // covers everything after the checksum field, including the rest of the packet header
    packet_counter: u64, // with well-known value for 'fire and forget'
    /// 0000: starts with a new message, no continuation from previous packet
    /// xxxx: offset to the first new message header - everything before that is the end of a message continued from the previous packet
    ///        NB: This can point to the end of the packet, meaning that this message is complete but the next message starts in the next packet
    /// FFFF: long message, continued from previous packet and continued in the next packet
    new_message_offset: u16,
    from: NodeAddr,
    to: NodeAddr,
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

    pub fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.checksum.0);
        buf.put_u64(self.packet_counter);
        self.from.ser(buf);
        buf.put_u32(self.to.unique);
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
}
