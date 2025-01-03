use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::{Buf, Bytes, BytesMut};
use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tokio::select;
use tracing::{debug, error, warn};
use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::transport::reliable_udp::buffer_pool::BufferPool;
use crate::messaging::transport::reliable_udp::headers::{MessageHeader, PacketHeader};


pub async fn receive_loop(buffer_pool: Arc<BufferPool>, myself: NodeAddr) -> anyhow::Result<()> {
    let mut packet_sequences = BTreeMap::default();

    let receive_socket = UdpSocket::bind(myself.socket_addr).await?; //TODO raw socket, DF=true

    loop {
        let mut receive_buffer = vec![0u8; 1493]; //TODO
        select! {
            recv_result = receive_socket.recv_from(&mut receive_buffer) => {
                match recv_result {
                    Ok((len, sender)) => {
                        if len > 1492 { //TODO configurable
                            error!("received packet exceeds configured MTU - skipping");
                            continue;
                        }

                        let buf = &mut &receive_buffer[..len];
                        let packet_header = PacketHeader::try_parse(buf)?;

                        if packet_header.to != myself {
                            warn!("received packet addressed to {:?}, myself is {:?} - skipping", packet_header.to, myself);
                            continue;
                        }

                        //TODO verify checksum

                        if !packet_sequences.contains_key(&packet_header.from) {
                            packet_sequences.insert(packet_header.from, ReceiveSequence::new(packet_header.from));
                        }

                        debug!("received message from {:?} to {:?} - forwarding to p2p packet sequence", packet_header.from, packet_header.to);
                        packet_sequences.get_mut(&packet_header.from)
                            .unwrap()
                            .on_packet(&packet_header, buf);
                    }
                    Err(e) => {
                        error!("Error receiving UDP packet: {}", e);
                    }
                }
            }
        }
    }
}

/// per target node address
struct ReceiveSequence {
    /// de-fragmentation buffer: packets that were received but not processed completely because
    /// subsequent packets have not arrived yet
    packets: BTreeMap<u64, (Vec<u8>, u16)>,

    /// This is where the first unprocessed message starts.
    /// NB: If a packet is consumed completely, this marker is moved to offset 0 of the next
    ///  packet, even if that packet is not received yet
    dispatched_up_to_packet: u64,
    /// NB: The offset point
    dispatched_up_to_offset: usize,

    from: NodeAddr,
}
impl ReceiveSequence {
    fn new(from: NodeAddr) -> Self {
        ReceiveSequence {
            packets: BTreeMap::default(),
            dispatched_up_to_packet: 0,
            dispatched_up_to_offset: 0,
            from,
        }
    }

    /// notification that there is no point in querying for packets before a given id - if packets
    ///  before this are missing, they are lost, and the receiver should skip messages in them
    fn set_earliest_available_packet_id(&mut self, earliest_id: u64) {
        // There is potential for salvaging more in corner cases, but this is good enough for a
        //  start, maybe permanently
        let to_be_dropped = self.packets
            .range(..earliest_id)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        if !to_be_dropped.is_empty() {
            warn!("skipping packets up to {} from {:?}", earliest_id, self.from);
        }

        for id in to_be_dropped {
            self.packets.remove(&id);
        }
    }

    fn on_packet(&mut self, packet_header: &PacketHeader, packet_body: &[u8]) {
        if self.packets.contains_key(&packet_header.packet_counter) {
            debug!("received duplicate of packet {:?}", packet_header);
            return;
        }

        self.packets.insert(packet_header.packet_counter, (packet_body.into(), packet_header.new_message_offset));

        while let Some((message_module_id, buf)) = self.try_parse_next_message() {
            //TODO dispatch message
        }

        //TODO send NAK after a delay - what criteria to use?
        //TODO send status update at intervals
    }

    /// parses the next message if possible, 'consuming' buffers and adjusting 'next' markers
    fn try_parse_next_message(&mut self, ) -> Option<(MessageModuleId, Vec<u8>)> {
        let (current_packet, _) = match self.packets.get_mut(&self.dispatched_up_to_packet) {
            None => return None,
            Some(packet) => packet,
        };

        let mut raw_buf = &current_packet[self.dispatched_up_to_offset..];
        let message_header = MessageHeader::try_parse(&mut raw_buf).unwrap(); //TODO handle unparseable header - this means inconsistent data, more precisely an end of the packet before the buffer was complete

        if message_header.message_len as usize <= raw_buf.remaining() { //TODO overflow (?)
            // current buffer contains complete message

            let message_buf = raw_buf[..message_header.message_len as usize].to_vec();
            self.advance_pointer_to_next(&message_header);
            return Some((message_header.message_module_id, message_buf));
        }

        // message continues in the next packet or packets
        self.try_defragment_next_message()
    }


    fn try_defragment_next_message(&mut self) -> Option<(MessageModuleId, Vec<u8>)> {
        if !self.are_all_fragments_available() {
            return None;
        }

        let (current_packet, _) = self.packets.get_mut(&self.dispatched_up_to_packet).unwrap();
        let mut raw_buf = &current_packet[self.dispatched_up_to_offset..];
        let message_header = MessageHeader::try_parse(&mut raw_buf).unwrap(); //TODO handle unparseable header - this means inconsistent data, more precisely an end of the packet before the buffer was complete

        let mut assembly_buffer = Vec::with_capacity(message_header.message_len as usize); //TODO overflow
        loop {
            assembly_buffer.extend_from_slice(raw_buf);

            self.packets.remove(&self.dispatched_up_to_packet);
            self.dispatched_up_to_packet += 1;

            let (current_packet, msg_offset) = self.packets.get_mut(&self.dispatched_up_to_packet).unwrap();
            raw_buf = &current_packet[self.dispatched_up_to_offset..];
            self.dispatched_up_to_offset = *msg_offset as usize;

            if *msg_offset != PacketHeader::OFFSET_CONTINUED_FRAGMENT_SEQUENCE {
                assembly_buffer.extend_from_slice(&raw_buf[..self.dispatched_up_to_offset]);
                self.normalize_next_pointer();
                return Some((message_header.message_module_id, assembly_buffer));
            }
        }
    }


    /// called for a fragmented message at the end of the current (i.e. 'dispatched_up_to_packet')
    ///  packet: checks if a seamless sequence of FFFF packets is available, i.e. concatenating the
    ///  buffers for parsing is possible
    fn are_all_fragments_available(&self) -> bool {
        let mut packet_id = self.dispatched_up_to_packet + 1;
        loop {
            match self.packets.get(&packet_id) {
                None => return false, // next packet was not received yet
                Some((_, message_offset)) => {
                    if *message_offset == PacketHeader::OFFSET_CONTINUED_FRAGMENT_SEQUENCE {
                        packet_id += 1;
                    }
                    else {
                        // next packet continues with a different message, so all packets for the
                        //  current message are available
                        return true;
                    }
                }
            }
        }
    }

    fn advance_pointer_to_next(&mut self, message_header: &MessageHeader) {
        self.dispatched_up_to_offset += MessageHeader::SERIALIZED_SIZE;
        self.dispatched_up_to_offset += message_header.message_len as usize;

        self.normalize_next_pointer()
    }

    fn normalize_next_pointer(&mut self) {
        if self.packets.get(&self.dispatched_up_to_packet)
            .expect("'advance' should be called only with existing packet")
            .0.len() == self.dispatched_up_to_offset
        {
            self.packets.remove(&self.dispatched_up_to_packet);
            self.dispatched_up_to_packet += 1;
            self.dispatched_up_to_offset = 0;
        }
    }
}
