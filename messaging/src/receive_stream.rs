use std::cmp::{max, min, Ordering};
use std::collections::btree_map::Entry;
use crate::control_messages::ControlMessageSendSync;
use crate::packet_id::PacketId;
use crate::send_socket::SendSocket;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use bytes_varint::{VarIntResult, VarIntSupport};
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::RwLock;
use tokio::time::{interval, Interval};
use tracing::{debug, trace};
use crate::MessageHeader::MessageHeader;
use crate::packet_header::PacketHeader;

struct ReceiveStreamConfig {
    nak_interval: Duration, // configure to roughly 2x RTT
    sync_interval: Duration, // configure on the order of seconds

    receive_window_size: u32,
    max_num_nak_packets: usize, //TODO limit so it fits into a single packet
}

struct ReceiveStreamInner {
    config: Arc<ReceiveStreamConfig>,

    stream_id: u16,
    send_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,

    /// the id *below which* the receiver will never be interested in receiving data. This is the
    ///  value that will be acknowledged in future SYNC messages.
    ///
    /// NB: There is no acknowledgement handshake for SYNC messages, so this is subtly different
    ///      from the threshold actually having been acknowledged; due to this and delayed packets
    ///      on the wire, packets below this threshold may arrive and will be dropped by the receiver
    ack_threshold: PacketId,

    /// This is the pointer of what is already dispatched.
    ///
    /// If this is `Some(low_water_mark, offs)`, it means that the next message to be dispatched
    ///  starts at that offset at the given packet if it is (still) the low-water mark, and start
    ///  with the first message in the low-water mark packet  otherwise.
    undispatched_marker: Option<(PacketId, u16)>,

    /// The buffer of all received packets that are not (fully) dispatched yet
    receive_buffer: BTreeMap<PacketId, (Option<u16>, Vec<u8>)>,

    /// A packet is considered 'missing' if all the following conditions hold:
    /// * No packet with this id was received
    /// * A packet with a higher id was received
    /// * The packet id is in the receive window
    ///
    /// The set of missing packets could be determined from the receive buffer, but it is
    ///  materialized and kept up-to-date for performance reasons and to store metadata for
    ///  each missing packet (see below).
    ///
    /// The missing packet buffer is polled periodically to assemble NAK packets. The interval
    ///  should be roughly twice the RTT between the nodes, to give NAK'ed packets time to arrive
    ///  after being requested.
    ///
    /// To give packets at least one such interval to arrive before being NAK'ed, there is a
    ///  'tick' counter that is incremented for each such polling interval, and the set of missing
    ///  packets stores the tick counter value of the time when its absence was noticed. Based on
    ///  this information, missing packets are *not* included in the first NAK packet
    ///  (potentially) sent after their absence is detected, but only from the second onward.
    missing_packet_buffer: BTreeMap<PacketId, u64>,
    missing_packet_tick_counter: u64,
}
impl ReceiveStreamInner {
    pub fn new() -> ReceiveStreamInner {
        //TODO init missing buffer with ID 0 to establish initial window - same as when receiving a 'jump' in packet IDs

        todo!()
    }

    async fn do_send_init(&self) {
        self.send_socket.send_control_init(self.self_reply_to_addr, self.peer_addr, self.stream_id)
            .await
    }

    async fn do_send_recv_sync(&self) {
        self.send_socket.send_recv_sync(
            self.self_reply_to_addr,
            self.peer_addr,
            self.stream_id,
            self.high_water_mark(),
            self.low_water_mark(),
            self.ack_threshold
        )
            .await
    }

    /// Send a NAK for the earliest N missing packets - the assumption is that if more packets are
    ///  missing, something is seriously wrong, and it is likely better to ask for the second batch
    ///  only when the first is re-delivered
    async fn do_send_nak(&mut self) {
        let mut nak_packets = Vec::new();

        for (&packet_id, &tick) in &self.missing_packet_buffer {
            if tick >= self.missing_packet_tick_counter {
                continue;
            }
            nak_packets.push(packet_id);
            if nak_packets.len() == self.config.max_num_nak_packets {
                break;
            }
        }

        if nak_packets.is_empty() {
            trace!("no missing packets to NAK");
            return;
        }

        self.send_socket.send_nak(self.self_reply_to_addr, self.peer_addr, self.stream_id, &nak_packets).await;
        self.missing_packet_tick_counter += 1;
    }

    /// This is the id *after* the highest packet that was received.
    fn high_water_mark(&self) -> PacketId {
        // This is the highest entry in the receive buffer, or the ack threshold if all received
        //  packets are acknowledged
        // NB: Missing packets are only stored when a higher-id packet is received, so it is
        //      irrelevant for the high-water mark
        self.receive_buffer.last_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(self.ack_threshold)
    }

    /// This is the id below which all packets will be discarded. Because all packets between
    ///  low-water mark and high-water mark are represented in the buffers (see below), the
    ///  low-water mark can be derived from the buffers' contents.
    ///
    /// NB: There is an invariant that *all* packets between low-water mark (incl) and high-water
    ///      mark (excl) exist either in the receive buffer or in the missing_packet buffer, no
    ///      packet exists in both buffers, and all packets in one of the buffers is in this
    ///      range
    fn low_water_mark(&self) -> PacketId {
        let received = self.receive_buffer.first_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(self.ack_threshold);
        let missing = self.missing_packet_buffer.first_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(self.ack_threshold);
        min(received, missing)
    }

    /// ensure consistency after buffers changed
    fn sanitize_after_update(&mut self) {
        // adjust the ack threshold, which may have changed either because a previously missing
        //  packet has now arrived, or because the high-water mark increased and there are no
        //  missing packets
        self.ack_threshold = self.missing_packet_buffer.keys().next()
            .cloned()
            //NB: high_water_mark() falls back to 'ack_threshold' :-)
            .unwrap_or(self.high_water_mark());

        // discard packets that are now outside the maximum receive window
        if let Some(lower_bound) = self.high_water_mark() - self.config.receive_window_size as u64 {
            while let Some((&packet_id, _)) = self.receive_buffer.first_key_value() {
                if packet_id > lower_bound {
                    break;
                }
                debug!("received packet #{} moved out of the receive window - discarding", packet_id);
                self.receive_buffer.remove(&packet_id);
            }

            while let Some((&packet_id, _)) = self.missing_packet_buffer.first_key_value() {
                if packet_id > lower_bound {
                    break;
                }
                debug!("missing packet #{} moved out of the receive window - discarding", packet_id);
                self.missing_packet_buffer.remove(&packet_id);
            }

            // As an optimization: If the receive window (NB: *not* just the receive buffer!)
            //  starts with continuation packets, we can safely discard them since they can
            //  never be dispatched without the start of the message.
            for packet_id in (lower_bound + 1).to(PacketId::MAX) {
                if let Some((first_msg_offs, buf)) = self.receive_buffer.get(&packet_id) {
                    if first_msg_offs.is_none() || *first_msg_offs == Some(buf.len() as u16) {
                        // continuation packet or ends a multi-packet message: drop the packet
                        self.receive_buffer.remove(&packet_id);
                    }
                    else {
                        // we hit a packet that starts a new message
                        self.undispatched_marker = Some((packet_id, first_msg_offs.unwrap()));
                        break;
                    }
                }
                else {
                    // We only discard an uninterrupted sequences of received packets.
                    // NB: we could discard missing packets as well based on the known message size
                    //      of a discarded initial message, but that may not be worth the complexity
                    break;
                }
            }
        }
    }

    fn consume_next_message(&mut self) -> Option<Vec<u8>> {
        //TODO there is a potential optimization here by using impl<Buf> to avoid copying
        //TODO optimization: call this only if the first missing packet was added, or the buffer was empty

        let low_water_mark = self.low_water_mark();

        let (next_offs, buf) = if let Some((lwm_offs, lwm_buf)) = self.receive_buffer.get(&low_water_mark) {
            // the low-water mark packet is received, not missing

            // assert that the lwm packet actually starts a message
            let lwm_offs = lwm_offs.expect("non-starting packets should have been skipped");
            assert!(lwm_offs < lwm_buf.len() as u16, "message-ending packets should have been skipped");

            match self.undispatched_marker {
                None => {
                    (lwm_offs, lwm_buf)
                }
                Some((undispatched_packet, undispatched_offset)) => {
                    if undispatched_packet == low_water_mark {
                        (undispatched_offset, lwm_buf)
                    }
                    else {
                        (lwm_offs, lwm_buf)
                    }
                }
            }
        }
        else {
            return None;
        };

        if next_offs as usize >= buf.len() {
            todo!("inconsistent packet - log, drop, sanitize, consume at new location");
        }

        let mut buf = &buf[next_offs as usize..];
        let header = match MessageHeader::deser(&mut buf) {
            Ok(header) => header,
            Err(_) => {
                todo!("inconsistent packet - log, drop, sanitize, consume at new location");
            }
        };

        match buf.len().cmp(&(header.message_len as usize)) {
            Ordering::Less => {
                // the message is contained in the packet
                self.undispatched_marker = Some((low_water_mark, next_offs + MessageHeader::SERIALIZED_LEN_U16 + header.message_len as u16));
                Some(buf[..header.message_len as usize].to_vec())
            }
            Ordering::Equal => {
                // the message terminates the packet

                let result_buf = buf[..header.message_len as usize].to_vec(); //TODO overflow

                self.undispatched_marker = None;
                self.receive_buffer.remove(&low_water_mark);

                Some(result_buf)
            }
            Ordering::Greater => {
                // start of a multi-packet message
                if self.is_complete_multipacket_message_received() {
                    let mut assembly_buffer = Vec::with_capacity(header.message_len as usize);

                    assembly_buffer.extend_from_slice(buf);
                    self.receive_buffer.remove(&low_water_mark);

                    for packet_id in self.low_water_mark().to(PacketId::MAX) {
                        //TODO check the assembly buffer against configured maximum message size and header.message_len

                        let (offs, buf) = self.receive_buffer.get(&packet_id)
                            .expect("we just checked that all parts are present");

                        match *offs {
                            None => {
                                assembly_buffer.extend_from_slice(buf);
                                self.receive_buffer.remove(&packet_id);
                            }
                            Some(offs) => {
                                if offs as usize > buf.len() {
                                    todo!("inconsistent packet - skip the entire thing")
                                }

                                assembly_buffer.extend_from_slice(&buf[..offs as usize]);
                                if offs as usize == buf.len() {
                                    self.receive_buffer.remove(&packet_id);
                                    self.undispatched_marker = None;
                                }
                                else {
                                    self.undispatched_marker = Some((packet_id, offs));
                                }
                                break;
                            }
                        }
                    }

                    // check buffer length against declared message length
                    if assembly_buffer.len() != header.message_len as usize {
                        todo!("inconsistent - skip the entire thing");
                    }

                    Some(assembly_buffer)
                }
                else {
                    None
                }
            }
        }
    }

    fn is_complete_multipacket_message_received(&self) -> bool {
        for packet_id in (self.low_water_mark() + 1).to(PacketId::MAX) {
            if let Some((offs, _)) = self.receive_buffer.get(&packet_id) {
                if offs.is_some() {
                    // end of message
                    return true;
                }
                // continuation packet
            }
            else {
                // missing packet
                return false;
            }
        }
        false // just for the compiler
    }
}

pub struct ReceiveStream {
    config: Arc<ReceiveStreamConfig>,
    inner: RwLock<ReceiveStreamInner>,
}

impl ReceiveStream {
    pub fn new() -> ReceiveStream {
        todo!()
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn do_send_init(&self) {
        self.inner.read().await
            .do_send_init().await;
    }

    pub async fn do_send_recv_sync(&self) {
        self.inner.read().await
            .do_send_recv_sync().await;
    }

    pub async fn do_send_nak(&self) {
        self.inner.write().await
            .do_send_nak().await;
    }

    pub async fn on_send_sync_message(&self, message: ControlMessageSendSync) {
        // This was sent in response to a recv_sync message, and no response is required. But
        //  we adjust the receive window based on the new information on the send window.
        //
        // NB: All updates here are robust against receiving an out-of-date SYNC message

        let mut inner = self.inner.write().await;

        // adjust ack threshold: There is no point in asking for packets that we know the server
        //  doesn't have in its send buffer any longer
        if inner.ack_threshold < message.send_buffer_low_water_mark {
            inner.undispatched_marker = None;
            inner.ack_threshold = message.send_buffer_low_water_mark;

            // discard missing packets up to the new ack threshold (and received packets below them)
            while let Some(&packet_id) = inner.missing_packet_buffer.keys().next() {
                if packet_id >= inner.ack_threshold {
                    break;
                }
                inner.missing_packet_buffer.remove(&packet_id);

                // discard all received packets below the discarded missing packet
                if let Some(&lowest_received) = inner.receive_buffer.keys().next() {
                    for recv_packet_id in lowest_received.to(packet_id) {
                        inner.receive_buffer.remove(&recv_packet_id);
                    }
                }
            }
        }

        // adjust the high-water mark by adding 'missing' packet ids up to it?
        for packet_id in inner.high_water_mark().to(message.send_buffer_high_water_mark) {
            let tick_counter = inner.missing_packet_tick_counter;
            inner.missing_packet_buffer.insert(packet_id, tick_counter);
        }
    }

    pub async fn on_packet(&self, sequence_number: PacketId, first_message_offset: Option<u16>, payload: &[u8]) {
        let mut inner = self.inner.write().await;

        //TODO unit test, especially off-by-one corner cases

        if sequence_number < inner.ack_threshold {
            debug!("received packet #{} which is below the ack threshold of #{} - ignoring", sequence_number, inner.ack_threshold);
            return;
        }

        // register all packets between the previous high-water mark and this packet as missing
        for missing_packet_id in inner.high_water_mark().to(sequence_number) {
            let tick_counter = inner.missing_packet_tick_counter;
            inner.missing_packet_buffer.insert(missing_packet_id, tick_counter);
        }

        // store the new packet as received
        inner.receive_buffer.insert(sequence_number, (first_message_offset, payload.to_vec()));

        // remove it from the 'missing' set
        inner.missing_packet_buffer.remove(&sequence_number);

        // clean up internal data structures
        inner.sanitize_after_update();

        while let Some(buf) = inner.consume_next_message() {
            //TODO dispatch message
        }
    }

    /// Active loop - this function never returns, it runs until it is taken out of dispatch
    pub async fn do_loop(&self) {
        let mut nak_interval = interval(self.config.nak_interval);
        let mut sync_interval = interval(self.config.sync_interval);

        loop {
            select! {
                _ = nak_interval.tick() => {
                    self.do_send_nak().await;
                }
                _ = sync_interval.tick() => {
                    //TODO or every N packets, if that is earlier?
                    self.do_send_recv_sync().await;
                }
            }
        }
    }
}
