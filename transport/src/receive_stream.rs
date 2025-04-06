use crate::buffers::buffer_pool::SendBufferPool;
use crate::config::EffectiveReceiveStreamConfig;
use crate::control_messages::{ControlMessageRecvSync, ControlMessageSendSync};
use crate::message_dispatcher::MessageDispatcher;
use crate::message_header::MessageHeader;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::packet_id::PacketId;
use crate::safe_converter::{PrecheckedCast, SafeCast};
use crate::send_pipeline::SendPipeline;
use bytes::BufMut;
use bytes_varint::VarIntSupportMut;
use std::cmp::{max, min, Ordering};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::select;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, trace, warn};

struct ReceiveStreamInner {
    config: Arc<EffectiveReceiveStreamConfig>,
    buffer_pool: Arc<SendBufferPool>,

    self_generation: u64,
    peer_generation: u64,

    stream_id: u16,
    send_pipeline: Arc<SendPipeline>,
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
    fn new(
        config: Arc<EffectiveReceiveStreamConfig>,
        buffer_pool: Arc<SendBufferPool>,
        self_generation: u64,
        peer_generation: u64,
        stream_id: u16,
        peer_addr: SocketAddr,
        send_pipeline: Arc<SendPipeline>,
        self_addr: SocketAddr,

    ) -> ReceiveStreamInner {
        //TODO document the assumption that querying a UdpSocket's local address cannot fail
        assert_eq!(peer_addr.is_ipv4(), send_pipeline.local_addr().is_ipv4());

        let self_reply_to_addr = if send_pipeline.local_addr() == self_addr {
            None
        }
        else {
            Some(self_addr)
        };

        ReceiveStreamInner {
            config,
            buffer_pool,
            self_generation,
            peer_generation,
            stream_id,
            send_pipeline,
            peer_addr,
            self_reply_to_addr,
            ack_threshold: PacketId::ZERO,
            undispatched_marker: None,
            receive_buffer: Default::default(),
            missing_packet_buffer: Default::default(),
            missing_packet_tick_counter: 0,
        }
    }

    async fn do_send_init(&self) {
        debug!("sending INIT");
        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlInit { stream_id: self.stream_id }, self.self_generation, Some(self.peer_generation));

        let mut send_buf = self.buffer_pool.get_from_pool();
        header.ser(&mut send_buf);

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await;
        self.buffer_pool.return_to_pool(send_buf);
    }

    async fn do_send_recv_sync(&self) {
        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlRecvSync { stream_id: self.stream_id }, self.self_generation, Some(self.peer_generation));

        let mut send_buf = self.buffer_pool.get_from_pool();
        header.ser(&mut send_buf);

        let msg = ControlMessageRecvSync {
            receive_buffer_high_water_mark: self.high_water_mark(),
            receive_buffer_low_water_mark: self.low_water_mark(),
            receive_buffer_ack_threshold: self.ack_threshold,
        };
        trace!("sending RECV_SYNC: {:?}", msg);
        msg.ser(&mut send_buf);

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await;
        self.buffer_pool.return_to_pool(send_buf);
    }

    /// Send a NAK for the earliest N missing packets - the assumption is that if more packets are
    ///  missing, something is seriously wrong, and it is likely better to ask for the second batch
    ///  only when the first is re-delivered
    async fn do_send_nak(&mut self) {
        self.missing_packet_tick_counter += 1;

        let mut nak_packets = Vec::new();

        for (&packet_id, &tick) in &self.missing_packet_buffer {
            if tick >= self.missing_packet_tick_counter-1 {
                continue;
            }
            nak_packets.push(packet_id);
            if nak_packets.len() == self.config.max_num_naks_per_packet {
                break;
            }
        }

        if nak_packets.is_empty() {
            trace!("no missing packets to NAK");
            return;
        }

        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlNak { stream_id: self.stream_id }, self.self_generation, Some(self.peer_generation));

        let mut send_buf = self.buffer_pool.get_from_pool();
        header.ser(&mut send_buf);

        send_buf.put_usize_varint(nak_packets.len());
        for packet_id in nak_packets {
            send_buf.put_u64(packet_id.to_raw());
        }

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await;
        self.buffer_pool.return_to_pool(send_buf);
    }

    /// This is the id *after* the highest packet that was received or is missing
    fn high_water_mark(&self) -> PacketId {
        match (self.receive_buffer.last_key_value(), self.missing_packet_buffer.last_key_value()) {
            (Some((&a, _)), None) => a+1,
            (None, Some((&b, _))) => b+1,
            (Some((&a, _)), Some((&b, _))) => max(a, b)+1,
            (None, None) => self.ack_threshold
        }
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
        match (self.receive_buffer.first_key_value(), self.missing_packet_buffer.first_key_value()) {
            (Some((&a, _)), None) => a,
            (None, Some((&b, _))) => b,
            (Some((&a, _)), Some((&b, _))) => min(a, b),
            (None, None) => self.ack_threshold
        }
    }

    /// ensure consistency after buffers changed
    fn sanitize_after_update(&mut self) {
        // NB: this function is called frequently enough that no details are lost - in particular,
        //  it is called after a packet is received but before messages are dispatched, so the
        //  ack threshold is not lost if the receive buffer is drained

        // NB: Within this function, ack_threshold may be outdated and inconsistent with buffer state

        let high_water_mark = self.high_water_mark();

        // discard packets that are now outside the maximum receive window
        if let Some(lower_bound) = high_water_mark - self.config.receive_window_size.safe_cast() {
            while let Some((&packet_id, _)) = self.receive_buffer.first_key_value() {
                if packet_id >= lower_bound {
                    break;
                }
                debug!("received packet #{} moved out of the receive window - discarding", packet_id);
                self.receive_buffer.remove(&packet_id);
            }

            while let Some((&packet_id, _)) = self.missing_packet_buffer.first_key_value() {
                if packet_id >= lower_bound {
                    break;
                }
                debug!("missing packet #{} moved out of the receive window - discarding", packet_id);
                self.missing_packet_buffer.remove(&packet_id);
            }
        }

        // As an optimization: If the receive window (NB: *not* just the receive buffer!)
        //  starts with continuation packets, we can safely discard them since they can
        //  never be dispatched without the start of the message.
        for packet_id in self.low_water_mark().to(PacketId::MAX) {
            if let Some((first_msg_offs, buf)) = self.receive_buffer.get(&packet_id) {
                if first_msg_offs.is_none() || *first_msg_offs == Some(buf.len().prechecked_cast()) {
                    // continuation packet or ends a multi-packet message: drop the packet
                    self.receive_buffer.remove(&packet_id);
                }
                else {
                    // we hit a packet that starts a new message
                    let undispatched_packet = self.undispatched_marker.map(|(p,_)| p);
                    if Some(packet_id) != undispatched_packet {
                        self.undispatched_marker = Some((packet_id, first_msg_offs.unwrap()));
                    }
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

        // remove non-starting packets at the start of the buffer
        loop {
            let low_water_mark = self.low_water_mark();
            match self.receive_buffer.get(&low_water_mark) {
                None => {
                    break;
                }
                Some((None, _)) => {
                    self.receive_buffer.remove(&low_water_mark);
                }
                Some((Some(offs), buf)) => {
                    if buf.len() <= (*offs).safe_cast() {
                        self.receive_buffer.remove(&low_water_mark);
                    }
                    else {
                        break;
                    }
                }
            }
        }

        let low_water_mark = self.low_water_mark();
        match self.undispatched_marker {
            None => {
                if let Some((Some(first_msg_offs), _)) = self.receive_buffer.get(&low_water_mark) {
                    let first_msg_offs = *first_msg_offs;
                    self.undispatched_marker = Some((low_water_mark, first_msg_offs));
                }
            }
            Some((packet_id, _)) => {
                if packet_id != low_water_mark {
                    self.undispatched_marker = None;
                }
                if self.receive_buffer.get(&low_water_mark).is_none() {
                    self.undispatched_marker = None;
                }
            }
        }

        // adjust the ack threshold, which may have changed either because a previously missing
        //  packet has now arrived, or because the high-water mark increased and there are no
        //  missing packets
        self.ack_threshold = self.missing_packet_buffer.keys().next()
            .cloned()
            //NB: high_water_mark() falls back to 'ack_threshold' :-)
            .unwrap_or(high_water_mark);
    }

    fn consume_next_message(&mut self) -> Option<Vec<u8>> {
        loop {
            match self._consume_next_message() {
                ConsumeResult::Message(buf) => {
                    trace!("consume next message: message of length {}", buf.len());
                    return Some(buf)
                },
                ConsumeResult::None => {
                    trace!("consume next message: no message");
                    return None
                },
                ConsumeResult::Retry => {
                    trace!("retry consuming next message");
                }
            }
        }
    }

    fn _consume_next_message(&mut self) -> ConsumeResult {
        let low_water_mark = self.low_water_mark();
        trace!("low water mark: {:?}", low_water_mark);

        let (next_offs, buf) = if let Some((lwm_offs, lwm_buf)) = self.receive_buffer.get(&low_water_mark) {
            // the low-water mark packet is received, not missing

            // assert that the lwm packet actually starts a message
            let lwm_offs = lwm_offs.expect("non-starting packets should have been skipped");
            assert!(lwm_offs.safe_cast() < lwm_buf.len(), "message-ending packets should have been skipped");

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
            trace!("low water mark packet is missing");
            return ConsumeResult::None;
        };

        if next_offs.safe_cast() >= buf.len() {
            warn!("packet #{} as low water mark with first message offset {} pointing after the end of the packet {} - skipping", low_water_mark, next_offs, buf.len());
            self.receive_buffer.remove(&low_water_mark);
            self.sanitize_after_update();
            return ConsumeResult::Retry;
        }

        let mut buf = &buf[next_offs.safe_cast()..];
        let header = match MessageHeader::deser(&mut buf) {
            Ok(header) => header,
            Err(_) => {
                //NB: This is the *first* packet of a message, so the 'first message' offset must point
                //     to an actual message header
                warn!("packet #{} as low water mark: first message offset {} does not point to a valid message header - skipping", low_water_mark, next_offs);
                self.receive_buffer.remove(&low_water_mark);
                self.sanitize_after_update();
                return ConsumeResult::Retry;
            }
        };

        if header.message_len > self.config.max_message_size {
            warn!("packet #{} declares a message length of {} which is longer than the configured maximum of {}", low_water_mark, header.message_len, self.config.max_message_size);

            self.receive_buffer.remove(&low_water_mark);
            self.sanitize_after_update();
            return ConsumeResult::Retry;
        }

        match <u32 as SafeCast<usize>>::safe_cast(header.message_len).cmp(&buf.len()) {
            Ordering::Less => {
                // the message is contained in the packet
                self.undispatched_marker = Some((low_water_mark, next_offs + MessageHeader::SERIALIZED_LEN_U16 + header.message_len.prechecked_cast()));
                ConsumeResult::Message(buf[..header.message_len.safe_cast()].to_vec())
            }
            Ordering::Equal => {
                // this packet terminates the packet

                let result_buf = buf[..header.message_len.safe_cast()].to_vec();

                self.undispatched_marker = None;
                self.receive_buffer.remove(&low_water_mark);

                ConsumeResult::Message(result_buf)
            }
            Ordering::Greater => {
                // start of a multi-packet message
                if self.is_complete_multipacket_message_received() {
                    let mut assembly_buffer = Vec::with_capacity(header.message_len.safe_cast());

                    assembly_buffer.extend_from_slice(buf);
                    self.receive_buffer.remove(&low_water_mark);

                    for packet_id in self.low_water_mark().to(PacketId::MAX) {
                        // iterate through follow-up packets of a multi-packet message

                        let (offs, buf) = self.receive_buffer.get(&packet_id)
                            .expect("we just checked that all parts are present");

                        match *offs {
                            None => {
                                if assembly_buffer.len() + buf.len() > header.message_len.safe_cast() {
                                    warn!("packet {}: message (started in packet {}) exceeds declared message length of {} - this is a bug on the sender side and may be a DoS attack", low_water_mark, packet_id, header.message_len);
                                    self.receive_buffer.remove(&packet_id);
                                    self.sanitize_after_update();
                                    return ConsumeResult::Retry;
                                }

                                assembly_buffer.extend_from_slice(buf);
                                self.receive_buffer.remove(&packet_id);
                            }
                            Some(offs) => {
                                if offs.safe_cast() > buf.len() {
                                    warn!("packet #{} has first message offset {} pointing after the end of the packet {} - skipping", packet_id, next_offs, buf.len());
                                    self.receive_buffer.remove(&packet_id);
                                    self.sanitize_after_update();
                                    return ConsumeResult::Retry;
                                }

                                if assembly_buffer.len() + offs.safe_cast() > header.message_len.safe_cast() {
                                    warn!("packet {}: message (started in packet {}) exceeds declared message size of {} - this is a bug on the sender side and may be a DoS attack", low_water_mark, packet_id, header.message_len);
                                    self.receive_buffer.remove(&packet_id);
                                    self.sanitize_after_update();
                                    return ConsumeResult::Retry;
                                }

                                assembly_buffer.extend_from_slice(&buf[..offs.safe_cast()]);
                                if offs.safe_cast() == buf.len() {
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
                    if assembly_buffer.len() != header.message_len.safe_cast() {
                        warn!("packet #{}: actual message length {} is different from length in messsage header {} - this is a sender-side bug. Skipping the message.", low_water_mark, header.message_len, assembly_buffer.len());
                        self.sanitize_after_update();
                        return ConsumeResult::Retry;
                    }

                    ConsumeResult::Message(assembly_buffer)
                }
                else {
                    ConsumeResult::None
                }
            }
        }
    }

    /// called after testing that the low-water mark packet ends with a message that continues
    ///  in following packets: returns true iff all packets for that message have been received
    fn is_complete_multipacket_message_received(&self) -> bool {
        for packet_id in (self.low_water_mark() + 1).to(PacketId::MAX) {
            if let Some((offs, _)) = self.receive_buffer.get(&packet_id) {
                if offs.is_some() {
                    // end of previous message - no need to check for 'after end of packet' etc.
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
    config: Arc<EffectiveReceiveStreamConfig>,
    inner: Arc<RwLock<ReceiveStreamInner>>,
    active_handle: Option<JoinHandle<()>>,
    message_dispatcher: Arc<dyn MessageDispatcher>,
}

impl Drop for ReceiveStream {
    fn drop(&mut self) {
        if let Some(handle) = self.active_handle.take() {
            handle.abort();
        }
    }
}

impl ReceiveStream {
    pub fn new(
        config: Arc<EffectiveReceiveStreamConfig>,
        buffer_pool: Arc<SendBufferPool>,
        self_generation: u64,
        peer_generation: u64,
        stream_id: u16,
        peer_addr: SocketAddr,
        send_pipeline: Arc<SendPipeline>,
        self_addr: SocketAddr,
        message_dispatcher: Arc<dyn MessageDispatcher>,
    ) -> ReceiveStream {
        let inner: Arc<RwLock<ReceiveStreamInner>> = Arc::new(RwLock::new(ReceiveStreamInner::new(
            config.clone(),
            buffer_pool,
            self_generation,
            peer_generation,
            stream_id,
            peer_addr,
            send_pipeline,
            self_addr,
        )));

        ReceiveStream {
            config,
            inner,
            active_handle: None,
            message_dispatcher,
        }
    }

    pub fn spawn_active_loop(&mut self) {
        if self.active_handle.is_some() {
            warn!("active loop already spawned");
            return;
        }
        self.active_handle = Some(tokio::spawn(Self::do_loop(self.config.clone(), self.inner.clone())));
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn do_send_init(&self) {
        self.inner.read().await
            .do_send_init().await;
    }

    pub async fn on_send_sync_message(&self, message: ControlMessageSendSync) {
        trace!("handling SEND_RECV message: {:?}", message);

        // This was sent in response to a recv_sync message, and no response is required. But
        //  we adjust the receive window based on the new information on the send window.
        //
        // NB: All updates here are robust against receiving an out-of-date SYNC message

        let mut inner = self.inner.write().await;

        // adjust ack threshold: There is no point in asking for packets that we know the server
        //  doesn't have in its send buffer any longer
        if inner.ack_threshold < message.send_buffer_low_water_mark {
            trace!("ack threshold below sender's low-water mark: discarding missing packets that are not available any more");

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

        let mut was_padding_added = false;
        // adjust the high-water mark by adding 'missing' packet ids up to it
        for packet_id in inner.high_water_mark().to(message.send_buffer_high_water_mark) {
            was_padding_added = true;
            let tick_counter = inner.missing_packet_tick_counter;
            inner.missing_packet_buffer.insert(packet_id, tick_counter);
        }
        if was_padding_added {
            trace!("sender's high-water mark is above local high-water mark: added padding");
        }

        inner.sanitize_after_update();
    }

    pub async fn on_packet(&self, sequence_number: PacketId, first_message_offset: Option<u16>, payload: &[u8]) {
        let mut inner = self.inner.write().await;

        trace!("received packet #{:?} with length {} from {:?} on stream {}, first msg offs {:?}", sequence_number.to_raw(), payload.len(), inner.peer_addr, inner.stream_id, first_message_offset);

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
            self.message_dispatcher.on_message(inner.peer_addr, Some(inner.stream_id), &buf).await;
        }
    }

    /// Active loop - this function never returns, it runs until it is taken out of dispatch
    async fn do_loop(config: Arc<EffectiveReceiveStreamConfig>, inner: Arc<RwLock<ReceiveStreamInner>>) {
        //TODO test this

        let mut nak_interval = interval(config.nak_interval);
        let mut sync_interval = interval(config.sync_interval);

        loop {
            select! {
                _ = nak_interval.tick() => {
                    //TODO sending NAKs at fixed intervals rather than every N received packets avoids thrashing, but
                    // it requires big send windows to avoid permanent message loss if a bulk of packets get
                    // lost rather than the sporadic more or less isolated packet - which may or may not be
                    // a good trade-off.
                    // --> this could do with some thinking and failure scenario modeling
                    inner.write().await
                       .do_send_nak().await;
                }
                _ = sync_interval.tick() => {
                    inner.write().await
                        .do_send_recv_sync().await;
                }
            }
        }
    }
}

enum ConsumeResult {
    Message(Vec<u8>),
    None,
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_dispatcher::MockMessageDispatcher;
    use crate::send_pipeline::MockSendSocket;
    use async_trait::async_trait;
    use mockall::Sequence;
    use rstest::rstest;
    use std::time::Duration;
    use tokio::runtime::Builder;
    use tokio::sync::Mutex;

    #[rstest]
    #[case::implicit_reply_to(SocketAddr::from(([1,2,3,4], 8)), vec![0,10, 0,0,0,0,0,3, 0,0,0,0,0,8, 0,25])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), vec![0,8, 0,0,0,0,0,3, 0,0,0,0,0,8, 1,2,3,4, 0,1, 0,25])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), vec![0,9, 0,0,0,0,0,3, 0,0,0,0,0,8, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25])]
    fn test_do_send_init(#[case] self_address: SocketAddr, #[case] expected_buf: Vec<u8>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));
            send_socket.expect_do_send_packet()
                .once()
                .withf(move |addr, buf|
                    addr == &SocketAddr::from(([1, 2, 3, 4], 9)) &&
                        buf == expected_buf.as_slice()
                )
                .returning(|_, _| ())
            ;

            let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(EffectiveReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 10,
                    max_message_size: 10,
                }),
                Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                3,
                8,
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                self_address,
                Arc::new(message_dispatcher),
            );

            receive_stream.do_send_init().await;
        });
    }

    #[rstest]
    #[case::implicit_reply_to(SocketAddr::from(([1,2,3,4], 8)), 0, 0, 0, vec![0,18, 0,0,0,0,0,4, 0,0,0,0,0,9, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), 0, 0, 0, vec![0,16, 0,0,0,0,0,4, 0,0,0,0,0,9, 1,2,3,4, 0,1, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), 0, 0, 0, vec![0,17, 0,0,0,0,0,4, 0,0,0,0,0,9, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::actual_values(SocketAddr::from(([1,2,3,4], 8)), 9, 3, 4, vec![0,18, 0,0,0,0,0,4, 0,0,0,0,0,9, 0,25, 0,0,0,0,0,0,0,9, 0,0,0,0,0,0,0,3, 0,0,0,0,0,0,0,4])]
    fn test_do_send_recv_sync(#[case] self_address: SocketAddr, #[case] high_water_mark: u64, #[case] low_water_mark: u64, #[case] ack_threshold: u64, #[case] expected_buf: Vec<u8>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));
            send_socket.expect_do_send_packet()
                .once()
                .withf(move |addr, buf|
                    addr == &SocketAddr::from(([1, 2, 3, 4], 9)) &&
                        buf == expected_buf.as_slice()
                )
                .returning(|_, _| ())
            ;

            let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(EffectiveReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 10,
                    max_message_size: 10,
                }),
                Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                4,
                9,
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                self_address,
                Arc::new(message_dispatcher),
            );

            let mut inner = receive_stream.inner.write().await;
            if high_water_mark > 0 {
                inner.receive_buffer.insert(PacketId::from_raw(high_water_mark-1), (None, vec![]));
            }
            if low_water_mark < high_water_mark {
                inner.receive_buffer.insert(PacketId::from_raw(low_water_mark), (None, vec![]));
            }
            inner.ack_threshold = PacketId::from_raw(ack_threshold);

            inner.do_send_recv_sync().await;
        });
    }

    #[rstest]
    #[case::implicit_reply_to(SocketAddr::from(([1,2,3,4], 8)), vec![(1,1)], vec![
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), vec![(1,1)], vec![
        vec![0,12, 0,0,0,0,0,11, 0,0,0,0,0,8, 1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0,12, 0,0,0,0,0,11, 0,0,0,0,0,8, 1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), vec![(1,1)], vec![
        vec![0,13, 0,0,0,0,0,11, 0,0,0,0,0,8, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0,13, 0,0,0,0,0,11, 0,0,0,0,0,8, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::empty(SocketAddr::from(([1,2,3,4], 8)), vec![], vec![])]
    #[case::two(SocketAddr::from(([1,2,3,4], 8)), vec![(1,1), (3,1)], vec![
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,1, 0,0,0,0,0,0,0,3],
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,1, 0,0,0,0,0,0,0,3]])]
    #[case::three(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,1), (5,1)], vec![ // cut off after configured limit of two NAKs per message
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3],
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
    #[case::one_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,2)], vec![ // send only after tick increase
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 1, 0,0,0,0,0,0,0,2]])]
    #[case::two_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,2)], vec![
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 1, 0,0,0,0,0,0,0,2],
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
    #[case::three_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,2), (5,2)], vec![
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 1, 0,0,0,0,0,0,0,2],
        vec![0,14, 0,0,0,0,0,11, 0,0,0,0,0,8, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
    fn test_do_send_nak(#[case] self_address: SocketAddr, #[case] missing: Vec<(u64, u64)>, #[case] expected_bufs: Vec<Vec<u8>>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));

            let mut sequence = Sequence::new();

            for b in expected_bufs {
                send_socket.expect_do_send_packet()
                    .once()
                    .in_sequence(&mut sequence)
                    .withf(move |addr, buf|
                        addr == &SocketAddr::from(([1, 2, 3, 4], 9)) &&
                            buf == b.as_slice()
                    )
                    .returning(|_, _| ())
                ;
            }

            let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(EffectiveReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 2,
                    max_message_size: 10,
                }),
                Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                11,
                8,
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                self_address,
                Arc::new(message_dispatcher),
            );

            let mut inner = receive_stream.inner.write().await;
            for (packet, counter) in missing {
                inner.missing_packet_buffer.insert(PacketId::from_raw(packet), counter);
            }
            inner.missing_packet_tick_counter = 2;

            inner.do_send_nak().await;
            inner.do_send_nak().await; // second call is with increased tick
        });
    }

    struct CollectingMessageDispatcher {
        messages: Mutex<Vec<(SocketAddr, Option<u16>, Vec<u8>)>>,
    }
    impl CollectingMessageDispatcher {
        fn new() -> CollectingMessageDispatcher {
            CollectingMessageDispatcher {
                messages: Default::default(),
            }
        }

        async fn assert_messages(&self, expected_sender: SocketAddr, expected_stream_id: u16, expected_messages: Vec<Vec<u8>>) {
            let expected = expected_messages.into_iter()
                .map(|buf| (expected_sender, Some(expected_stream_id), buf))
                .collect::<Vec<_>>();

            assert_eq!(self.messages.lock().await.clone(), expected);
        }
    }
    #[async_trait]
    impl MessageDispatcher for CollectingMessageDispatcher {
        async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, msg_buf: &[u8]) {
            self.messages.lock().await.push((sender, stream_id, msg_buf.to_vec()));
        }
    }

    #[rstest]
    #[case::first_packet_simple(vec![], vec![], None, (2, Some(0), vec![0,0,0,3,1,2,3]), vec![], vec![], None, vec![vec![1,2,3]])]
    #[case::first_packet_two(vec![], vec![], None, (2, Some(0), vec![0,0,0,3,1,2,3, 0,0,0,2,5,6]), vec![], vec![], None, vec![vec![1,2,3], vec![5,6]])]
    #[case::first_packet_three(vec![], vec![], None, (2, Some(0), vec![0,0,0,3,1,2,3, 0,0,0,1,4, 0,0,0,2,5,6]), vec![], vec![], None, vec![vec![1,2,3], vec![4], vec![5,6]])]

    #[case::message_continued(vec![(2, Some(0), vec![0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::message_continued_offs(vec![(2, Some(1), vec![8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::message_continued_offs2(vec![(2, Some(0), vec![8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::message_continued_offs3(vec![(2, Some(1), vec![8, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::message_continued_ooo(vec![(3, Some(2), vec![3,4])], vec![2], None, (2, Some(0), vec![0,0,0,4,1,2]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::message_continued_ooo_offs(vec![(3, Some(2), vec![3,4])], vec![2], None, (2, Some(2), vec![8,9, 0,0,0,4,1,2]), vec![], vec![], None, vec![vec![1,2,3,4]])]
    #[case::second_message_continued(vec![(2, Some(0), vec![0,0,0,1,5, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::second_message_continued_offs(vec![(2, Some(1), vec![8, 0,0,0,1,5, 0,0,0,4,1,2])], vec![], Some((2,1)), (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::second_message_continued_offs2(vec![(2, Some(0), vec![8, 0,0,0,1,5, 0,0,0,4,1,2])], vec![], Some((2,1)), (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::second_message_continued_offs3(vec![(2, Some(1), vec![8, 0,0,0,1,5, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::second_message_continued_ooo(vec![(3, Some(2), vec![3,4])], vec![2], None, (2, Some(0), vec![0,0,0,1,5, 0,0,0,4,1,2]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::second_message_continued_ooo_offs(vec![(3, Some(2), vec![3,4])], vec![2], None, (2, Some(2), vec![7,8, 0,0,0,1,5, 0,0,0,4,1,2]), vec![], vec![], None, vec![vec![5], vec![1,2,3,4]])]
    #[case::message_continued_with_continuation(vec![(2, Some(0), vec![0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::message_continued_with_continuation_offs(vec![(2, Some(1), vec![8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::message_continued_with_continuation_offs2(vec![(2, Some(0), vec![8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::message_continued_with_continuation_offs3(vec![(2, Some(1), vec![8, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::message_continued_with_continuation_ooo(vec![(3, Some(2), vec![3,4, 0,0,0,3,7])], vec![2], None, (2, Some(0), vec![0,0,0,4,1,2]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::message_continued_with_continuation_ooo_offs(vec![(3, Some(2), vec![3,4, 0,0,0,3,7])], vec![2], None, (2, Some(2), vec![7,8, 0,0,0,4,1,2]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation(vec![(2, Some(0), vec![0,0,0,1,8, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation_offs(vec![(2, Some(1), vec![8, 0,0,0,1,8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation_offs2(vec![(2, Some(0), vec![8, 0,0,0,1,8, 0,0,0,4,1,2])], vec![], Some((2, 1)), (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation_offs3(vec![(2, Some(1), vec![8, 0,0,0,1,8, 0,0,0,4,1,2])], vec![], None, (3, Some(2), vec![3,4, 0,0,0,3,7]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation_ooo(vec![(3, Some(2), vec![3,4, 0,0,0,3,7])], vec![2], None, (2, Some(0), vec![0,0,0,1,8, 0,0,0,4,1,2]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]
    #[case::second_message_continued_with_continuation_ooo_offs(vec![(3, Some(2), vec![3,4, 0,0,0,3,7])], vec![2], None, (2, Some(2), vec![7,8, 0,0,0,1,8, 0,0,0,4,1,2]), vec![(3, Some(2))], vec![], Some((3, 2)), vec![vec![8], vec![1,2,3,4]])]

    #[case::long_continued(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (3, None, vec![2])], vec![], None, (4, Some(1), vec![3]), vec![], vec![], None, vec![vec![1,2,3]])]
    #[case::long_continued_with_more(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (3, None, vec![2])], vec![], None, (4, Some(1), vec![3, 0,0,0,2,5,6]), vec![], vec![], None, vec![vec![1,2,3], vec![5,6]])]
    #[case::long_continued_with_more2(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (3, None, vec![2])], vec![], None, (4, Some(1), vec![3, 0,0,0,2,5,6, 0,0,0,1]), vec![(4, Some(1))], vec![], Some((4,7)), vec![vec![1,2,3], vec![5,6]])]
    #[case::long_continued_ooo1(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (4, Some(1), vec![3])], vec![3], None, (3, None, vec![2]), vec![], vec![], None, vec![vec![1,2,3]])]
    #[case::long_continued_ooo1_with_more(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (4, Some(1), vec![3, 0,0,0,2,5,6])], vec![3], None, (3, None, vec![2]), vec![], vec![], None, vec![vec![1,2,3], vec![5,6]])]
    #[case::long_continued_ooo1_with_more2(vec![(2, Some(2), vec![3,4, 0,0,0,3,1]), (4, Some(1), vec![3, 0,0,0,2,5,6, 0,0,0,1])], vec![3], None, (3, None, vec![2]), vec![(4, Some(1))], vec![], Some((4,7)), vec![vec![1,2,3], vec![5,6]])]
    #[case::long_continued_ooo2(vec![(3, None, vec![2]), (4, Some(1), vec![3])], vec![2], None, (2, Some(2), vec![3,4, 0,0,0,3,1]), vec![], vec![], None, vec![vec![1,2,3]])]
    #[case::long_continued_ooo2_with_more(vec![(3, None, vec![2]), (4, Some(1), vec![3, 0,0,0,2,5,6])], vec![2], None, (2, Some(2), vec![3,4, 0,0,0,3,1]), vec![], vec![], None, vec![vec![1,2,3], vec![5,6]])]
    #[case::long_continued_ooo2_with_more2(vec![(3, None, vec![2]), (4, Some(1), vec![3, 0,0,0,2,5,6, 0,0,0,1])], vec![2], None, (2, Some(2), vec![3,4, 0,0,0,3,1]), vec![(4, Some(1))], vec![], Some((4,7)), vec![vec![1,2,3], vec![5,6]])]

    #[case::gap_above_high_water_mark(vec![(1, Some(0), vec![0,0,0,5,1,2,3])], vec![], None, (3, Some(0), vec![0,0,0,1,5]), vec![(1,Some(0)), (3,Some(0))], vec![2], Some((1,0)), vec![])]
    #[case::below_ack_threshold(vec![(1, Some(0), vec![0,0,0,5,1,2,3])], vec![], None, (0, Some(0), vec![0,0,0,1,5]), vec![(1,Some(0))], vec![], None, vec![])]
    #[case::below_receive_window(vec![(8, Some(0), vec![0,0,0,5,1,2,3])], vec![5,6,7], None, (4, Some(0), vec![0,0,0,1,5]), vec![(8,Some(0))], vec![5,6,7], None, vec![])]
    #[case::overflow_received(vec![(5,Some(2),vec![5,6, 0,0,0,8,1,2,3]), (8,Some(0),vec![0,0,0,5,1,2,3])], vec![6,7], None, (9, Some(0), vec![0,0,0,1,5]), vec![(8,Some(0)), (9,Some(0))], vec![6,7], None, vec![])]
    #[case::overflow_received_discarded(vec![(5,Some(2),vec![5,6, 0,0,0,8,1,2,3]), (6,None,vec![4]), (8,Some(0),vec![0,0,0,5,1,2,3])], vec![7], None, (9, Some(0), vec![0,0,0,1,5]), vec![(8,Some(0)), (9,Some(0))], vec![7], None, vec![])]
    #[case::overflow_missing(vec![(8, Some(0), vec![0,0,0,5,1,2,3])], vec![5,6,7], None, (9, Some(0), vec![0,0,0,1,5]), vec![(8,Some(0)),((9,Some(0)))], vec![6,7], None, vec![])]

    #[case::offset_after_buf_standalone(vec![], vec![], None, (2,Some(5),vec![1,2,3,4]), vec![], vec![], None, vec![])]
    #[case::offset_after_buf_continuation(vec![(1,Some(0),vec![0,0,0,5,1,2,3])], vec![], None, (2,Some(5),vec![1,2,3,4]), vec![], vec![], None, vec![])]
    #[case::offset_incomplete_len_standalone(vec![], vec![], None, (2,Some(1),vec![1,2,3,4]), vec![], vec![], None, vec![])]
    #[case::offset_incomplete_len_continuation(vec![(1,Some(0),vec![0,0,0,5,1,2,3])], vec![], None, (2,Some(2),vec![1,2,3,4]), vec![], vec![], None, vec![vec![1,2,3,1,2]])]
    #[case::offset_to_end_standalone(vec![], vec![], None, (2,Some(4),vec![1,2,3,4]), vec![], vec![], None, vec![])]
    #[case::offset_to_end_continuation(vec![(1,Some(0),vec![0,0,0,5,1,2,3])], vec![], None, (2,Some(2), vec![1,2]), vec![], vec![], None, vec![vec![1,2,3,1,2]])]
    #[case::message_longer_than_declared(vec![(1,Some(0),vec![0,0,0,5,1,2,3])], vec![], None, (2,Some(3), vec![1,2,3]), vec![], vec![], None, vec![])]
    #[case::message_shorter_than_declared(vec![(1,Some(0),vec![0,0,0,5,1,2,3])], vec![], None, (2,Some(1), vec![1]), vec![], vec![], None, vec![])]

    #[case::message_max_len_simple(vec![], vec![], None, (2,Some(0),vec![0,0,0,10,1,2,3,4,5,6,7,8,9,10,10]), vec![], vec![], None, vec![vec![1,2,3,4,5,6,7,8,9,10]])]
    #[case::message_too_long_simple(vec![], vec![], None, (2,Some(0),vec![0,0,0,11,1,2,3,4,5,6,7,8,9,10,11]), vec![], vec![], None, vec![])]
    #[case::message_too_long_simple_continued(vec![], vec![], None, (2,Some(0),vec![0,0,0,11,1,2,3,4,5,6,7,8,9,10,11, 0,0,0,1,4]), vec![], vec![], None, vec![])]
    #[case::message_too_long_simple_continued_multi(vec![], vec![], None, (2,Some(0),vec![0,0,0,11,1,2,3,4,5,6,7,8,9,10,11, 0,0,0,5]), vec![], vec![], None, vec![])]
    #[case::message_body_too_long_two_packets(vec![(1,Some(2),vec![5,6, 0,0,0,10,1,2,3,4,5])], vec![], None, (2,Some(6),vec![1,2,3,4,5,6]), vec![], vec![], None, vec![])]
    #[case::message_decl_too_long_two_packets(vec![(1,Some(2),vec![5,6, 0,0,0,11,1,2,3,4,5])], vec![], None, (2,Some(6),vec![1,2,3,4,5,6]), vec![], vec![], None, vec![])]
    #[case::message_body_too_long_two_packets_continued(vec![(1,Some(2),vec![5,6, 0,0,0,10,1,2,3,4,5])], vec![], None, (2,Some(6),vec![1,2,3,4,5,6, 0,0,0,1,9]), vec![], vec![], None, vec![])]
    #[case::message_decl_too_long_two_packets_continued(vec![(1,Some(2),vec![5,6, 0,0,0,11,1,2,3,4,5])], vec![], None, (2,Some(6),vec![1,2,3,4,5,6, 0,0,0,1,9]), vec![], vec![], None, vec![vec![9]])]
    #[case::message_body_too_long_three_packets(vec![(0,Some(2),vec![5,6, 0,0,0,10,1,2,3,4,5]), (1,None,vec![6,7,8])], vec![], None, (2,Some(3),vec![9,10,11]), vec![], vec![], None, vec![])]
    #[case::message_decl_too_long_three_packets(vec![(0,Some(2),vec![5,6, 0,0,0,11,1,2,3,4,5]), (1,None,vec![6,7,8])], vec![], None, (2,Some(3),vec![9,10,11]), vec![], vec![], None, vec![])]
    #[case::message_body_too_long_three_packets_continued(vec![(0,Some(2),vec![5,6, 0,0,0,10,1,2,3,4,5]), (1,None,vec![6,7,8])], vec![], None, (2,Some(3),vec![9,10,11, 0,0,0,1,4]), vec![], vec![], None, vec![])]
    #[case::message_decl_too_long_three_packets_continued(vec![(0,Some(2),vec![5,6, 0,0,0,11,1,2,3,4,5]), (1,None,vec![6,7,8])], vec![], None, (2,Some(3),vec![9,10,11, 0,0,0,1,4]), vec![], vec![], None, vec![vec![4]])]
    fn test_on_packet(
        #[case] received: Vec<(u64, Option<u16>, Vec<u8>)>,
        #[case] missing: Vec<u64>,
        #[case] undispatched_marker: Option<(u64, u16)>,
        #[case] new_packet: (u64, Option<u16>, Vec<u8>),
        #[case] expected_received: Vec<(u64, Option<u16>)>,
        #[case] expected_missing: Vec<u64>,
        #[case] expected_undispatched_marker: Option<(u64, u16)>,
        #[case] expected_messages: Vec<Vec<u8>>,
    ) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));

            let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

            let message_dispatcher = Arc::new(CollectingMessageDispatcher::new());

            let receive_stream = ReceiveStream::new(
                Arc::new(EffectiveReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 4,
                    max_num_naks_per_packet: 10,
                    max_message_size: 10,
                }),
                Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                3,
                8,
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                SocketAddr::from(([1, 2, 3, 4], 8)),
                message_dispatcher.clone(),
            );

            {
                let mut inner = receive_stream.inner.write().await;
                for (packet, offs, buf) in received {
                    inner.receive_buffer.insert(PacketId::from_raw(packet), (offs, buf));
                }
                for packet in missing {
                    inner.missing_packet_buffer.insert(PacketId::from_raw(packet), 0);
                }
                inner.undispatched_marker = undispatched_marker.map(|(packet, offs)| (PacketId::from_raw(packet), offs));
                inner.ack_threshold = PacketId::from_raw(2);
            }
            let (sequence_number, offs, payload) = new_packet;
            receive_stream.on_packet(PacketId::from_raw(sequence_number), offs, &payload).await;

            message_dispatcher.assert_messages(SocketAddr::from(([1,2,3,4], 9)), 25, expected_messages).await;

            let inner = receive_stream.inner.read().await;
            assert_eq!(inner.receive_buffer.iter().map(|(packet, (offs, _))| (packet.to_raw(), *offs)).collect::<Vec<_>>(), expected_received);
            assert_eq!(inner.missing_packet_buffer.keys().cloned().collect::<Vec<_>>(), expected_missing.iter().map(|&packet| PacketId::from_raw(packet)).collect::<Vec<_>>());
            assert_eq!(inner.undispatched_marker, expected_undispatched_marker.map(|(packet, offs)| (PacketId::from_raw(packet), offs)));
        });
    }

    #[rstest]
    // no changes to the receiver
    #[case::empty(vec![], vec![], 0, 0, 0, vec![], vec![], 0, None)]
    #[case::exact_match(vec![(1,Some(0)), (3,Some(0))], vec![2], 2, 4, 2, vec![1,3], vec![2], 2, Some((1,1)))]
    #[case::sender_has_longer_history_1(vec![(1,Some(0)), (3,Some(0))], vec![2], 2, 4, 1, vec![1,3], vec![2], 2, Some((1,1)))]
    #[case::sender_has_longer_history_2(vec![(1,Some(0)), (3,Some(0))], vec![2], 2, 4, 0, vec![1,3], vec![2], 2, Some((1,1)))]

    // ack threshold is below sender's low-water mark -> move it up (and clean up)
    #[case::discard_single_missing(vec![(2,Some(0)), (3,Some(0)), (4,Some(0))], vec![1], 1, 5, 2, vec![2,3,4], vec![], 5, Some((2,0)))]
    #[case::discard_single_missing_keep_missing(vec![(3,Some(0)), (4,Some(0))], vec![1,2], 1, 5, 2, vec![3,4], vec![2], 2, None)]
    #[case::discard_double_missing(vec![(3,Some(0)), (4,Some(0))], vec![1,2], 1, 5, 3, vec![3,4], vec![], 5, Some((3,0)))]
    #[case::discard_until_missing(vec![(1,Some(0)), (3,Some(0)), (4,Some(0))], vec![2], 1, 5, 3, vec![3,4], vec![], 5, Some((3,0)))]
    #[case::discard_following_none(vec![(1,Some(0)), (3,None), (4,Some(2))], vec![2], 1, 5, 3, vec![4], vec![], 5, Some((4,2)))]
    #[case::discard_following_end(vec![(1,Some(0)), (3,Some(3)), (4,Some(2))], vec![2], 1, 5, 3, vec![4], vec![], 5, Some((4,2)))]

    // raise high-water mark with 'missing' padding
    #[case(vec![(1,Some(0)), (2,Some(0))], vec![], 3, 4, 2, vec![1,2], vec![3], 3, Some((1,1)))]
    #[case(vec![(1,Some(0)), (2,Some(0))], vec![], 3, 5, 1, vec![1,2], vec![3,4], 3, Some((1,1)))]
    #[case(vec![(1,Some(0))], vec![2], 2, 5, 1, vec![1], vec![2,3,4], 2, Some((1,1)))]
    #[case(vec![(2,Some(0))], vec![1], 1, 5, 1, vec![2], vec![1,3,4], 1, None)]

    // both - for completeness' sake
    #[case::discard_and_patch(vec![], vec![1,2], 1, 4, 2, vec![], vec![2,3], 2, None)]
    fn test_on_send_sync_message(
        #[case] received: Vec<(u64, Option<u16>)>,
        #[case] missing: Vec<u64>,
        #[case] ack_threshold: u64,
        #[case] sender_high_water_mark: u64,
        #[case] sender_low_water_mark: u64,
        #[case] expected_received: Vec<u64>,
        #[case] expected_missing: Vec<u64>,
        #[case] expected_ack_threshold: u64,
        #[case] expected_undispatched_marker: Option<(u64, u16)>,
    ) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));

            let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

            let receive_stream = ReceiveStream::new(
                Arc::new(EffectiveReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 4,
                    max_num_naks_per_packet: 10,
                    max_message_size: 10,
                }),
                Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                3,
                8,
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                SocketAddr::from(([1, 2, 3, 4], 8)),
                Arc::new(MockMessageDispatcher::new()).clone(),
            );

            {
                let mut inner = receive_stream.inner.write().await;
                for (packet_id, offs) in received {
                    inner.receive_buffer.insert(PacketId::from_raw(packet_id), (offs, vec![1,2,3]));
                }
                for packet_id in missing {
                    inner.missing_packet_buffer.insert(PacketId::from_raw(packet_id), 0);
                }
                inner.ack_threshold = PacketId::from_raw(ack_threshold);
                inner.undispatched_marker = if inner.receive_buffer.contains_key(&inner.low_water_mark()) {
                    Some((inner.low_water_mark(), 1))
                }
                else {
                    None
                }
            }

            receive_stream.on_send_sync_message(ControlMessageSendSync {
                send_buffer_high_water_mark: PacketId::from_raw(sender_high_water_mark),
                send_buffer_low_water_mark: PacketId::from_raw(sender_low_water_mark),
            }).await;

            let inner = receive_stream.inner.read().await;
            assert_eq!(inner.receive_buffer.keys().map(|k| k.to_raw()).collect::<Vec<_>>(), expected_received);
            assert_eq!(inner.missing_packet_buffer.keys().map(|k| k.to_raw()).collect::<Vec<_>>(), expected_missing);
            assert_eq!(inner.ack_threshold.to_raw(), expected_ack_threshold);
            assert_eq!(inner.undispatched_marker, expected_undispatched_marker.map(|(packet_id, offs)| (PacketId::from_raw(packet_id), offs)));
        });
    }

    #[rstest]
    #[case::empty(vec![], false)]
    #[case::ends_unterminated(vec![(2, None)], false)]
    #[case::start_gap(vec![(2, None), (4, Some(1))], false)]
    #[case::inner_gap(vec![(2, None), (3, None), (5, Some(1))], false)]
    #[case::complete_1(vec![(2, None), (3, Some(1))], true)]
    #[case::complete_2(vec![(2, None), (3, None), (4, Some(1))], true)]
    #[case::complete_3(vec![(2, None), (3, None), (4, None), (5, Some(1))], true)]
    #[case::offs_0(vec![(2, None), (3, Some(0))], true)]
    #[case::offs_len(vec![(2, None), (3, Some(3))], true)]
    #[case::trailing_continuation(vec![(2, None), (3, Some(1)), (4, None)], true)]
    fn test_is_complete_multi_packet_message_received(#[case] received: Vec<(u64, Option<u16>)>, #[case] expected: bool) {
        let mut send_socket = MockSendSocket::new();
        send_socket.expect_local_addr()
            .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));
        let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

        let mut inner = ReceiveStreamInner::new(
            Arc::new(EffectiveReceiveStreamConfig {
                nak_interval: Duration::from_millis(100),
                sync_interval: Duration::from_millis(100),
                receive_window_size: 32,
                max_num_naks_per_packet: 2,
                max_message_size: 10,
            }),
            Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            3,
            7,
            25,
            SocketAddr::from(([1, 2, 3, 4], 9)),
            Arc::new(send_pipeline),
            SocketAddr::from(([1, 2, 3, 4], 8)),
        );

        for (packet, offs) in received {
            inner.receive_buffer.insert(PacketId::from_raw(packet), (offs, vec![1,2,3]));
        }

        assert_eq!(inner.is_complete_multipacket_message_received(), expected)
    }

    #[rstest]
    #[case::empty(vec![], vec![], 0, 0, 0)]
    #[case::ack_threshold(vec![], vec![], 5, 5, 5)]
    #[case::one_present(vec![7], vec![], 5, 8, 7)]
    #[case::two_present(vec![7, 8], vec![], 5, 9, 7)]
    #[case::one_missing(vec![7], vec![6], 5, 8, 6)]
    #[case::two_missing(vec![7, 9], vec![6, 8], 5, 10, 6)]
    #[case::missing_only(vec![], vec![5], 5, 6, 5)]
    #[case::two_missing(vec![], vec![5, 6], 5, 7, 5)]
    #[case::missing_highest(vec![4], vec![5], 5, 6, 4)]
    fn test_high_low_water_mark(#[case] received: Vec<u64>, #[case] missing: Vec<u64>, #[case] ack_threshold: u64, #[case] expected_high: u64, #[case] expected_low: u64) {
        let mut send_socket = MockSendSocket::new();
        send_socket.expect_local_addr()
            .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));
        let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

        let mut inner = ReceiveStreamInner::new(
            Arc::new(EffectiveReceiveStreamConfig {
                nak_interval: Duration::from_millis(100),
                sync_interval: Duration::from_millis(100),
                receive_window_size: 32,
                max_num_naks_per_packet: 2,
                max_message_size: 10,
            }),
            Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            3,
            9,
            25,
            SocketAddr::from(([1, 2, 3, 4], 9)),
            Arc::new(send_pipeline),
            SocketAddr::from(([1, 2, 3, 4], 8)),
        );

        for packet in received {
            inner.receive_buffer.insert(PacketId::from_raw(packet), (Some(0), vec![]));
        }
        for packet in missing {
            inner.missing_packet_buffer.insert(PacketId::from_raw(packet), 1);
        }
        inner.ack_threshold = PacketId::from_raw(ack_threshold);

        assert_eq!(inner.high_water_mark(), PacketId::from_raw(expected_high));
        assert_eq!(inner.low_water_mark(), PacketId::from_raw(expected_low));
    }

    #[rstest]
    #[case::empty(vec![], vec![], 0, vec![], vec![], 0, None, None)]
    #[case::starting_with_offs_start(vec![(1, Some(0)), (2, None)], vec![], 3, vec![1, 2], vec![], 3, Some((1,1)), Some((1,1)))]
    #[case::starting_with_offs_later(vec![(11, Some(0)), (12, None)], vec![], 13, vec![11, 12], vec![], 13, Some((11,1)), Some((11,1)))]

    #[case::starting_1(vec![(1, Some(0)), (2, None)], vec![0], 0, vec![1, 2], vec![0], 0, Some((1, 2)), None)]
    #[case::starting_2(vec![(0, Some(0)), (1, Some(0)), (2, None)], vec![], 0, vec![0, 1, 2], vec![], 3, Some((0, 0)), Some((0,0)))]
    #[case::starting_3(vec![(1, Some(0)), (2, None)], vec![0], 0, vec![1, 2], vec![0], 0, Some((1, 0)), None)]
    #[case::starting_4(vec![(1, Some(0)), (2, None)], vec![0], 0, vec![1, 2], vec![0], 0, None, None)]
    #[case::starting_ack(vec![(2, None)], vec![1], 0, vec![2], vec![1], 1, None, None)]
    #[case::regular(vec![(11, Some(0)), (12, None)], vec![10], 10, vec![11, 12], vec![10], 10, None, None)]
    #[case::regular_ack_1(vec![(12, None)], vec![11], 10, vec![12], vec![11], 11, Some((10, 0)), None)]
    #[case::regular_ack_2(vec![(12, None)], vec![11], 9, vec![12], vec![11], 11, None, None)]
    #[case::regular_ack_3(vec![(12, None)], vec![11], 8, vec![12], vec![11], 11, None, None)]
    #[case::offs_at_end(vec![(2,Some(3))], vec![], 2, vec![], vec![], 3, None, None)]
    #[case::offs_after_end(vec![(2,Some(4))], vec![], 2, vec![], vec![], 3, None, None)]
    #[case::out_of_window_received(vec![(12, Some(0)), (9, Some(0)), (8, Some(0))], vec![10,11], 8, vec![9, 12], vec![10,11], 10, Some((8, 0)), Some((9, 0)))]
    #[case::out_of_window_missing(vec![(12, None)], vec![8,9,10,11], 8, vec![12], vec![9,10,11], 9, Some((8, 0)), None)]
    #[case::out_of_window_both(vec![(12, None),(7,Some(0))], vec![8,9,10,11], 7, vec![12], vec![9,10,11], 9, Some((7,0)), None)]

    #[case::missing_only_starting(vec![], vec![0,1,2], 0, vec![], vec![0,1,2], 0, Some((0, 0)), None)]
    #[case::missing_highest_starting(vec![(1, Some(0))], vec![0,2], 0, vec![1], vec![0,2], 0, Some((0,0)), None)]
    #[case::missing_highest_starting(vec![(0, Some(0)), (1, Some(0))], vec![2], 0, vec![0,1], vec![2], 2, None, Some((0,0)))]
    #[case::missing_only_starting_ack(vec![], vec![1,2], 0, vec![], vec![1,2], 1, Some((0,0)), None)]
    #[case::missing_highest_starting_ack(vec![(1, None)], vec![2], 0, vec![], vec![2], 2, Some((0,0)), None)]
    #[case::missing_only_regular(vec![], vec![10,11,12], 10, vec![], vec![10,11,12], 10, Some((10,0)), None)]
    #[case::missing_highest_regular(vec![(11, Some(0))], vec![10,12], 10, vec![11], vec![10,12], 10, Some((10,0)), None)]
    #[case::missing_highest_regular(vec![(10, Some(0)), (11, Some(0))], vec![12], 10, vec![10,11], vec![12], 12, Some((10,0)), Some((10,0)))]
    #[case::missing_only_regular_ack_1(vec![], vec![11,12], 10, vec![], vec![11,12], 11, Some((10,0)), None)]
    #[case::missing_highest_regular_ack_1(vec![(11, None)], vec![12], 10, vec![], vec![12], 12, Some((10,0)), None)]
    #[case::missing_only_regular_ack_2(vec![], vec![11,12], 9, vec![], vec![11,12], 11, Some((10,0)), None)]
    #[case::missing_only_regular_ack_2(vec![], vec![11,12], 9, vec![], vec![11,12], 11, Some((11,0)), None)]
    #[case::missing_highest_regular_ack_2(vec![(11, None)], vec![12], 9, vec![], vec![12], 12, Some((10,0)), None)]
    #[case::missing_highest_regular_ack_2(vec![(11, None)], vec![12], 9, vec![], vec![12], 12, Some((11,1)), None)]
    #[case::missing_only_regular_ack_3(vec![], vec![11,12], 8, vec![], vec![11,12], 11, Some((10,0)), None)]
    #[case::missing_only_regular_ack_3(vec![], vec![11,12], 8, vec![], vec![11,12], 11, Some((11,0)), None)]
    #[case::missing_highest_regular_ack_3(vec![(11, None)], vec![12], 8, vec![], vec![12], 12, Some((10,0)), None)]
    #[case::missing_highest_regular_ack_3(vec![(11, None)], vec![12], 8, vec![], vec![12], 12, Some((11,1)), None)]
    #[case::missing_only_out_of_window_received(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9, Some((8,0)), None)]
    #[case::missing_only_out_of_window_received(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9, Some((9,0)), None)]
    #[case::missing_highest_out_of_window_received(vec![(9, Some(0)), (8, Some(0))], vec![10,11,12], 8, vec![9], vec![10,11,12], 10, Some((8,0)), Some((9,0)))]
    #[case::missing_only_out_of_window_missing(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9, Some((8,0)), None)]
    #[case::missing_only_out_of_window_missing(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9, None, None)]
    #[case::missing_highest_out_of_window_missing(vec![(11, None)], vec![8,9,10,12], 8, vec![11], vec![9,10,12], 9, Some((8,0)), None)]
    #[case::missing_highest_out_of_window_missing(vec![(9, Some(0))], vec![8,10,11,12], 8, vec![9], vec![10,11,12], 10, Some((8,0)), Some((9,0)))]
    #[case::missing_highest_out_of_window_missing(vec![(9, Some(0))], vec![8,10,11,12], 8, vec![9], vec![10,11,12], 10, None, Some((9,0)))]
    #[case::missing_only_out_of_window_both(vec![], vec![7,8,9,10,11,12], 7, vec![], vec![9,10,11,12], 9, Some((7,8)), None)]
    #[case::missing_only_out_of_window_both(vec![], vec![7,8,9,10,11,12], 7, vec![], vec![9,10,11,12], 9, Some((8,2)), None)]
    #[case::missing_only_out_of_window_both(vec![], vec![7,8,9,10,11,12], 7, vec![], vec![9,10,11,12], 9, None, None)]
    #[case::missing_highest_out_of_window_both(vec![(7,Some(0)),(9,Some(1)),(10,None)], vec![8,11,12], 7, vec![9,10], vec![11,12], 11, Some((7,2)), Some((9,1)))]
    #[case::missing_highest_out_of_window_both(vec![(7,Some(0)),(9,Some(1)),(10,None)], vec![8,11,12], 7, vec![9,10], vec![11,12], 11, Some((8,2)), Some((9,1)))]
    #[case::missing_highest_out_of_window_both(vec![(7,Some(0)),(9,Some(1)),(10,None)], vec![8,11,12], 7, vec![9,10], vec![11,12], 11, None, Some((9,1)))]

    #[case::mid_of_msg_all(vec![(12, None),(11, None),(10, None),(9, None)], vec![8], 8, vec![], vec![], 13, None, None)]
    #[case::mid_of_msg_all(vec![(12, None),(11, None),(10, None),(9, None)], vec![8], 8, vec![], vec![], 13, Some((8,2)), None)]
    #[case::mid_of_msg_all(vec![(12, None),(11, None),(10, None),(9, None)], vec![8], 8, vec![], vec![], 13, Some((9,1)), None)]
    #[case::mid_of_msg_until_received(vec![(12, None),(11, Some(2)),(10, None),(9, None)], vec![8], 8, vec![11,12], vec![], 13, Some((8,1)), Some((11,2)))]
    #[case::mid_of_msg_until_received(vec![(12, None),(11, Some(2)),(10, None),(9, None)], vec![8], 8, vec![11,12], vec![], 13, Some((9,0)), Some((11,2)))]
    #[case::mid_of_msg_until_received(vec![(12, None),(11, Some(2)),(10, None),(9, None)], vec![8], 8, vec![11,12], vec![], 13, None,        Some((11,2)))]
    #[case::mid_of_msg_until_missing(vec![(12, None),(10, None),(9, None)], vec![11,8], 8, vec![12], vec![11], 11, Some((8,0)), None)]
    #[case::mid_of_msg_until_missing(vec![(12, None),(10, None),(9, None)], vec![11,8], 8, vec![12], vec![11], 11, Some((9,5)), None)]
    #[case::mid_of_msg_until_missing(vec![(12, None),(10, None),(9, None)], vec![11,8], 8, vec![12], vec![11], 11, None,        None)]
    #[case::mid_of_msg_buf_len(vec![(12, None),(11, Some(0)),(10, Some(3)),(9, None)], vec![8], 8, vec![11,12], vec![], 13, Some((8,0)), Some((11,0)))]
    #[case::mid_of_msg_buf_len(vec![(12, None),(11, Some(0)),(10, Some(3)),(9, None)], vec![8], 8, vec![11,12], vec![], 13, Some((9,0)), Some((11,0)))]
    #[case::mid_of_msg_buf_len(vec![(12, None),(11, Some(0)),(10, Some(3)),(9, None)], vec![8], 8, vec![11,12], vec![], 13, None, Some((11,0)))]
    fn test_sanitize_after_update(
        #[case] received: Vec<(u64, Option<u16>)>,
        #[case] missing: Vec<u64>,
        #[case] ack_threshold: u64,
        #[case] expected_received: Vec<u64>,
        #[case] expected_missing: Vec<u64>,
        #[case] expected_ack_threshold: u64,
        #[case] undispatched_marker: Option<(u64, u16)>,
        #[case] expected_undispatched_marker: Option<(u64, u16)>,
    ) {
        let expected_received = expected_received.iter()
            .map(|&n| PacketId::from_raw(n))
            .collect::<Vec<_>>();
        let expected_missing = expected_missing.iter()
            .map(|&n| PacketId::from_raw(n))
            .collect::<Vec<_>>();

        let mut send_socket = MockSendSocket::new();
        send_socket.expect_local_addr()
            .return_const(SocketAddr::from(([1, 2, 3, 4], 8)));
        let send_pipeline = SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}));

        let mut inner = ReceiveStreamInner::new(
            Arc::new(EffectiveReceiveStreamConfig {
                nak_interval: Duration::from_millis(100),
                sync_interval: Duration::from_millis(100),
                receive_window_size: 4,
                max_num_naks_per_packet: 2,
                max_message_size: 10,
            }),
            Arc::new(SendBufferPool::new(1000, 1, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            3,
            13,
            25,
            SocketAddr::from(([1, 2, 3, 4], 9)),
            Arc::new(send_pipeline),
            SocketAddr::from(([1, 2, 3, 4], 8)),
        );

        for (packet, offs_first) in received {
            inner.receive_buffer.insert(PacketId::from_raw(packet), (offs_first, vec![1,2,3]));
        }
        for packet in missing {
            inner.missing_packet_buffer.insert(PacketId::from_raw(packet), 1);
        }
        inner.ack_threshold = PacketId::from_raw(ack_threshold);
        inner.undispatched_marker = undispatched_marker.map(|(packet_id, offset)| (PacketId::from_raw(packet_id), offset));

        inner.sanitize_after_update();

        assert_eq!(inner.receive_buffer.keys().cloned().collect::<Vec<_>>(), expected_received);
        assert_eq!(inner.missing_packet_buffer.keys().cloned().collect::<Vec<_>>(), expected_missing);
        assert_eq!(inner.ack_threshold, PacketId::from_raw(expected_ack_threshold));
        assert_eq!(inner.undispatched_marker, expected_undispatched_marker.map(|(packet, offset)| (PacketId::from_raw(packet), offset)));
    }
}
