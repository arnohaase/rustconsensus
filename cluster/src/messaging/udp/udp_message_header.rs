use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;
use bytes::{Buf, BufMut, BytesMut};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UdpMessageHeader {
    pub sender_addr: NodeAddr,
    //TODO ensure unique part cannot be 0 in NodeAddr
    pub recipient_unique_part: Option<u64>, //TODO u32?

    /// unique and (assumed) gap-less - used for receiver-side deduplication
    pub sequence_number: u64,
    
    pub message_module_id: MessageModuleId,
}

impl UdpMessageHeader {
    pub fn ser(&self, buf: &mut BytesMut) {
        self.sender_addr.ser(buf);
        buf.put_u64(self.recipient_unique_part.unwrap_or(0));
        buf.put_u64(self.sequence_number);
        self.message_module_id.ser(buf);
    }
    
    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<UdpMessageHeader> {
        let sender_addr = NodeAddr::try_deser(buf)?;
        let recpipient_unique_part = buf.try_get_u64()?;
        let recipient_unique_part = if recpipient_unique_part == 0 { None } else { Some(recpipient_unique_part) };
        let sequence_number = buf.try_get_u64()?;
        let message_module_id = MessageModuleId::deser(buf)?;
        
        Ok(UdpMessageHeader {
            sender_addr,
            recipient_unique_part,
            sequence_number,
            message_module_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use rstest::*;
    use super::*;
    use crate::test_util::node::*;
    
    #[rstest]
    #[case(UdpMessageHeader { sender_addr: test_node_addr_from_number(5), recipient_unique_part: None, sequence_number: 0, message_module_id: MessageModuleId(7)})]
    #[case(UdpMessageHeader { sender_addr: test_node_addr_from_number(5), recipient_unique_part: Some(347658), sequence_number: 324, message_module_id: MessageModuleId(88)})]
    #[case(UdpMessageHeader { sender_addr: test_node_addr_from_number_ipv6(55), recipient_unique_part: None, sequence_number: 345987978635, message_module_id: MessageModuleId(999)})]
    #[case(UdpMessageHeader { sender_addr: test_node_addr_from_number_ipv6(55), recipient_unique_part: Some(25876768923), sequence_number: 345987978635, message_module_id: MessageModuleId(1111)})]
    fn test_ser_deser(#[case] header: UdpMessageHeader) {
        let mut buf = BytesMut::new();
        header.ser(&mut buf);

        let mut b: &[u8] = &buf;
        let deser = UdpMessageHeader::deser(&mut b).unwrap();
        
        assert!(b.is_empty());
        assert_eq!(header, deser);
    }
}
