use crate::control_messages::{ControlMessageRecvSync, ControlMessageSendSync};
use crate::message_dispatcher::MessageDispatcher;
use crate::message_header::MessageHeader;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::packet_id::PacketId;
use crate::send_pipeline::SendPipeline;
use bytes::{BufMut, BytesMut};
use bytes_varint::VarIntSupportMut;
use std::cmp::{max, min, Ordering};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, trace, warn};

pub struct ReceiveStreamConfig {
    pub nak_interval: Duration, // configure to roughly 2x RTT
    pub sync_interval: Duration, // configure on the order of seconds

    pub receive_window_size: u32,
    pub max_num_naks_per_packet: usize, //TODO limit so it fits into a single packet
}

//TODO unit test

struct ReceiveStreamInner {
    config: Arc<ReceiveStreamConfig>,

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
        config: Arc<ReceiveStreamConfig>,
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
        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlInit { stream_id: self.stream_id });

        let mut send_buf = BytesMut::with_capacity(1400); //TODO from pool?
        header.ser(&mut send_buf);

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await
    }

    async fn do_send_recv_sync(&self) {
        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlRecvSync { stream_id: self.stream_id });

        let mut send_buf = BytesMut::with_capacity(1400); //TODO from pool?
        header.ser(&mut send_buf);

        ControlMessageRecvSync {
            receive_buffer_high_water_mark: self.high_water_mark(),
            receive_buffer_low_water_mark: self.low_water_mark(),
            receive_buffer_ack_threshold: self.ack_threshold,
        }.ser(&mut send_buf);

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await
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

        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlNak { stream_id: self.stream_id });

        let mut send_buf = BytesMut::with_capacity(1400); //TODO from pool?
        header.ser(&mut send_buf);

        send_buf.put_usize_varint(nak_packets.len());
        for packet_id in nak_packets {
            send_buf.put_u64(packet_id.to_raw());
        }

        self.send_pipeline.finalize_and_send_packet(self.peer_addr, &mut send_buf).await;
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
        if let Some(lower_bound) = high_water_mark - self.config.receive_window_size as u64 {
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

            // As an optimization: If the receive window (NB: *not* just the receive buffer!)
            //  starts with continuation packets, we can safely discard them since they can
            //  never be dispatched without the start of the message.
            for packet_id in lower_bound.to(PacketId::MAX) {
                if let Some((first_msg_offs, buf)) = self.receive_buffer.get(&packet_id) {
                    if first_msg_offs.is_none() || *first_msg_offs == Some(buf.len() as u16) {
                        println!("removing recv {}", packet_id);

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
        //TODO there is a potential optimization here by using impl<Buf> to avoid copying
        //TODO optimization: call this only if the first missing packet was added, or the buffer was empty

        let low_water_mark = self.low_water_mark();
        trace!("low water mark: {:?}", low_water_mark);

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
            trace!("low water mark packet is missing");
            return ConsumeResult::None;
        };

        if next_offs as usize >= buf.len() {
            warn!("packet #{} as low water mark with first message offset {} pointing after the end of the packet {} - skipping", low_water_mark, next_offs, buf.len());
            self.receive_buffer.remove(&low_water_mark);
            self.sanitize_after_update();
            return ConsumeResult::Retry;
        }

        let mut buf = &buf[next_offs as usize..];
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

        match (header.message_len as usize).cmp(&buf.len()) {
            Ordering::Less => {
                // the message is contained in the packet
                self.undispatched_marker = Some((low_water_mark, next_offs + MessageHeader::SERIALIZED_LEN_U16 + header.message_len as u16));
                ConsumeResult::Message(buf[..header.message_len as usize].to_vec())
            }
            Ordering::Equal => {
                // this packet terminates the packet

                let result_buf = buf[..header.message_len as usize].to_vec(); //TODO overflow

                self.undispatched_marker = None;
                self.receive_buffer.remove(&low_water_mark);

                ConsumeResult::Message(result_buf)
            }
            Ordering::Greater => {
                // start of a multi-packet message
                if self.is_complete_multipacket_message_received() {
                    let mut assembly_buffer = Vec::with_capacity(header.message_len as usize);

                    assembly_buffer.extend_from_slice(buf);
                    self.receive_buffer.remove(&low_water_mark);

                    for packet_id in self.low_water_mark().to(PacketId::MAX) {
                        // iterate through follow-up packets of a multi-packet message

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
                                    warn!("packet #{} has first message offset {} pointing after the end of the packet {} - skipping", packet_id, next_offs, buf.len());
                                    self.receive_buffer.remove(&packet_id);
                                    self.sanitize_after_update();
                                    return ConsumeResult::Retry;
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
                        warn!("packet #{}: actual message length {} is different from length in messsage header {} - skipping", low_water_mark, header.message_len, assembly_buffer.len());
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
        config: Arc<ReceiveStreamConfig>,
        stream_id: u16,
        peer_addr: SocketAddr,
        send_pipeline: Arc<SendPipeline>,
        self_addr: SocketAddr,
        message_dispatcher: Arc<dyn MessageDispatcher>,
    ) -> ReceiveStream {
        let inner: Arc<RwLock<ReceiveStreamInner>> = Arc::new(RwLock::new(ReceiveStreamInner::new(
            config.clone(),
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

        trace!("received packet #{:?} with length {} from {:?} on stream {}, first msg offs {:?}", sequence_number.to_raw(), payload.len(), inner.peer_addr, inner.stream_id, first_message_offset);

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
            self.message_dispatcher.on_message(inner.peer_addr, Some(inner.stream_id), &buf).await;
        }
    }

    /// Active loop - this function never returns, it runs until it is taken out of dispatch
    async fn do_loop(config: Arc<ReceiveStreamConfig>, inner: Arc<RwLock<ReceiveStreamInner>>) {
        let mut nak_interval = interval(config.nak_interval);
        let mut sync_interval = interval(config.sync_interval);

        loop {
            select! {
                _ = nak_interval.tick() => {
                    inner.write().await
                    .do_send_nak().await;
                }
                _ = sync_interval.tick() => {
                    //TODO or every N packets, if that is earlier?
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
    use mockall::{automock, Sequence};
    use super::*;
    use crate::message_dispatcher::MockMessageDispatcher;
    use crate::send_pipeline::MockSendSocket;
    use rstest::rstest;
    use tokio::runtime::Builder;

    #[rstest]
    #[case::implicit_reply_to(SocketAddr::from(([1,2,3,4], 8)), vec![0, 10, 0, 25])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), vec![0, 8, 1,2,3,4, 0,1, 0,25])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), vec![0, 9, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25])]
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

            let send_pipeline = SendPipeline::new(Arc::new(send_socket));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(ReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 10,
                }),
                25,
                SocketAddr::from(([1, 2, 3, 4], 9)),
                Arc::new(send_pipeline),
                self_address,
                Arc::new(message_dispatcher),
            );

            receive_stream.do_send_init().await;
        });
    }

    #[automock]
    trait Asdf {
        fn do_it(&self, n: u32);
    }


    #[rstest]
    #[case::implicit_reply_to(SocketAddr::from(([1,2,3,4], 8)), 0, 0, 0, vec![0, 18, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), 0, 0, 0, vec![0, 16, 1,2,3,4, 0,1, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), 0, 0, 0, vec![0, 17, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::actual_values(SocketAddr::from(([1,2,3,4], 8)), 9, 3, 4, vec![0, 18, 0,25, 0,0,0,0,0,0,0,9, 0,0,0,0,0,0,0,3, 0,0,0,0,0,0,0,4])]
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

            let send_pipeline = SendPipeline::new(Arc::new(send_socket));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(ReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 10,
                }),
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
        vec![0, 14, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0, 14, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::v4_reply_to(SocketAddr::from(([1,2,3,4], 1)), vec![(1,1)], vec![
        vec![0, 12, 1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0, 12, 1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::v6_reply_to(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 1)), vec![(1,1)], vec![
        vec![0, 13, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1],
        vec![0, 13, 1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4, 0,1, 0,25, 1, 0,0,0,0,0,0,0,1]])]
    #[case::empty(SocketAddr::from(([1,2,3,4], 8)), vec![], vec![])]
    #[case::two(SocketAddr::from(([1,2,3,4], 8)), vec![(1,1), (3,1)], vec![
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,1, 0,0,0,0,0,0,0,3],
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,1, 0,0,0,0,0,0,0,3]])]
    #[case::three(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,1), (5,1)], vec![ // cut off after configured limit of two NAKs per message
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3],
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
    #[case::one_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,2)], vec![ // send only after tick increase
        vec![0, 14, 0,25, 1, 0,0,0,0,0,0,0,2]])]
    #[case::two_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,2)], vec![
        vec![0, 14, 0,25, 1, 0,0,0,0,0,0,0,2],
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
    #[case::three_filtered(SocketAddr::from(([1,2,3,4], 8)), vec![(2,1), (3,2), (5,2)], vec![
        vec![0, 14, 0,25, 1, 0,0,0,0,0,0,0,2],
        vec![0, 14, 0,25, 2, 0,0,0,0,0,0,0,2, 0,0,0,0,0,0,0,3]])]
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

            let send_pipeline = SendPipeline::new(Arc::new(send_socket));

            let message_dispatcher = MockMessageDispatcher::new();

            let receive_stream = ReceiveStream::new(
                Arc::new(ReceiveStreamConfig {
                    nak_interval: Duration::from_millis(100),
                    sync_interval: Duration::from_millis(100),
                    receive_window_size: 32,
                    max_num_naks_per_packet: 2,
                }),
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

    #[tokio::test]
    async fn test_do_loop() {
        todo!()
    }

    #[tokio::test]
    async fn test_on_packet() {
        todo!()
    }

    #[tokio::test]
    async fn test_on_send_sync_message() {
        todo!()
    }

    #[tokio::test]
    async fn test_scheduled_nak() {
        todo!()
    }

    #[tokio::test]
    async fn test_scheduled_sync() {
        todo!()
    }

    #[tokio::test]
    async fn test_is_complete_multi_packet_mesage_received() {
        todo!()
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
        let send_pipeline = SendPipeline::new(Arc::new(send_socket));

        let mut inner = ReceiveStreamInner::new(
            Arc::new(ReceiveStreamConfig {
                nak_interval: Duration::from_millis(100),
                sync_interval: Duration::from_millis(100),
                receive_window_size: 32,
                max_num_naks_per_packet: 2,
            }),
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
    #[case::empty(vec![], vec![], 0, vec![], vec![], 0)]
    #[case::starting(vec![(1, Some(0)), (2, None)], vec![0], 0, vec![1, 2], vec![0], 0)]
    #[case::starting_ack(vec![(2, None)], vec![1], 0, vec![2], vec![1], 1)]
    #[case::regular(vec![(11, Some(0)), (12, None)], vec![10], 10, vec![11, 12], vec![10], 10)]
    #[case::regular_ack_1(vec![(12, None)], vec![11], 10, vec![12], vec![11], 11)]
    #[case::regular_ack_2(vec![(12, None)], vec![11], 9, vec![12], vec![11], 11)]
    #[case::regular_ack_3(vec![(12, None)], vec![11], 8, vec![12], vec![11], 11)]
    #[case::out_of_window_received(vec![(12, Some(0)), (9, Some(0)), (8, Some(0))], vec![10,11], 8, vec![9, 12], vec![10,11], 10)]
    #[case::out_of_window_missing(vec![(12, None)], vec![8,9,10,11], 8, vec![12], vec![9,10,11], 9)]
    #[case::out_of_window_both(vec![(12, None),(7,Some(0))], vec![8,9,10,11], 7, vec![12], vec![9,10,11], 9)]

    #[case::missing_only_starting(vec![], vec![0,1,2], 0, vec![], vec![0,1,2], 0)]
    #[case::missing_highest_starting(vec![(1, Some(0))], vec![0,2], 0, vec![1], vec![0,2], 0)]
    #[case::missing_only_starting_ack(vec![], vec![1,2], 0, vec![], vec![1,2], 1)]
    #[case::missing_highest_starting_ack(vec![(1, None)], vec![2], 0, vec![1], vec![2], 2)]
    #[case::missing_only_regular(vec![], vec![10,11,12], 10, vec![], vec![10,11,12], 10)]
    #[case::missing_highest_regular(vec![(11, Some(0))], vec![10,12], 10, vec![11], vec![10,12], 10)]
    #[case::missing_only_regular_ack_1(vec![], vec![11,12], 10, vec![], vec![11,12], 11)]
    #[case::missing_highest_regular_ack_1(vec![(11, None)], vec![12], 10, vec![11], vec![12], 12)]
    #[case::missing_only_regular_ack_2(vec![], vec![11,12], 9, vec![], vec![11,12], 11)]
    #[case::missing_highest_regular_ack_2(vec![(11, None)], vec![12], 9, vec![11], vec![12], 12)]
    #[case::missing_only_regular_ack_3(vec![], vec![11,12], 8, vec![], vec![11,12], 11)]
    #[case::missing_highest_regular_ack_3(vec![(11, None)], vec![12], 8, vec![11], vec![12], 12)]
    #[case::missing_only_out_of_window_received(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9)]
    #[case::missing_highest_out_of_window_received(vec![(9, Some(0)), (8, Some(0))], vec![10,11,12], 8, vec![9], vec![10,11,12], 10)]
    #[case::missing_only_out_of_window_missing(vec![], vec![8,9,10,11,12], 8, vec![], vec![9,10,11,12], 9)]
    #[case::missing_highest_out_of_window_missing(vec![(11, None)], vec![8,9,10,12], 8, vec![11], vec![9,10,12], 9)]
    #[case::missing_only_out_of_window_both(vec![], vec![7,8,9,10,11,12], 7, vec![], vec![9,10,11,12], 9)]
    #[case::missing_highest_out_of_window_both(vec![(7,Some(0)),(9,Some(0)),(10,None)], vec![8,11,12], 7, vec![9,10], vec![11,12], 11)]

    #[case::mid_of_msg_all(vec![(12, None),(11, None),(10, None),(9, None)], vec![8], 8, vec![], vec![], 13)]
    #[case::mid_of_msg_until_received(vec![(12, None),(11, Some(0)),(10, None),(9, None)], vec![8], 8, vec![11,12], vec![], 13)]
    #[case::mid_of_msg_until_missing(vec![(12, None),(10, None),(9, None)], vec![11,8], 8, vec![12], vec![11], 11)]
    #[case::mid_of_msg_buf_len(vec![(12, None),(11, Some(0)),(10, Some(3)),(9, None)], vec![8], 8, vec![11,12], vec![], 13)]
    fn test_sanitize_after_update(
        #[case] received: Vec<(u64, Option<u16>)>,
        #[case] missing: Vec<u64>,
        #[case] ack_threshold: u64,
        #[case] expected_received: Vec<u64>,
        #[case] expected_missing: Vec<u64>,
        #[case] expected_ack_threshold: u64,
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
        let send_pipeline = SendPipeline::new(Arc::new(send_socket));

        let mut inner = ReceiveStreamInner::new(
            Arc::new(ReceiveStreamConfig {
                nak_interval: Duration::from_millis(100),
                sync_interval: Duration::from_millis(100),
                receive_window_size: 4,
                max_num_naks_per_packet: 2,
            }),
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

        inner.sanitize_after_update();

        assert_eq!(inner.receive_buffer.keys().cloned().collect::<Vec<_>>(), expected_received);
        assert_eq!(inner.missing_packet_buffer.keys().cloned().collect::<Vec<_>>(), expected_missing);
        assert_eq!(inner.ack_threshold, PacketId::from_raw(expected_ack_threshold));
    }
}
