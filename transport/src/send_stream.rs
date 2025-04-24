use crate::buffers::atomic_map::AtomicMap;
use crate::buffers::buffer_pool::SendBufferPool;
use crate::buffers::fixed_buffer::FixedBuf;
use crate::config::EffectiveSendStreamConfig;
use crate::control_messages::{ControlMessageRecvSync, ControlMessageSendSync};
use crate::hs_congestion_control::HsCongestionControl;
use crate::message_header::MessageHeader;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::packet_id::PacketId;
use crate::safe_converter::{PrecheckedCast, SafeCast};
use crate::send_pipeline::SendPipeline;
use anyhow::bail;
use bytes::BufMut;
use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time;
use tracing::{debug, span, trace, Instrument, Level, Span};
use uuid::Uuid;

struct SendStreamInner {
    config: Arc<EffectiveSendStreamConfig>,
    self_generation: u64,
    peer_generations: Arc<AtomicMap<SocketAddr, u64>>,
    stream_id: u16,
    send_socket: Arc<SendPipeline>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,

    send_buffer: BTreeMap<PacketId, FixedBuf>,
    send_window_changed_notifier: mpsc::Sender<()>,

    congestion_control: HsCongestionControl,
    fed_to_congestion_control_marker: PacketId,

    /// first packet that was not actually sent over the network. This can be smaller than
    ///  work_in_progress_packet_id in case of congestion
    next_unsent_packet_id: PacketId,
    /// next packet to be buffered, either already in 'work in progress' or next one to go there
    work_in_progress_packet_id: PacketId,
    work_in_progress: Option<FixedBuf>,
    work_in_progress_late_send_handle: Option<tokio::task::JoinHandle<()>>,
    buffer_pool: Arc<SendBufferPool>,
}
impl SendStreamInner {
    fn packet_header_len(&self) -> usize {
        PacketHeader::serialized_len_for_stream_header(self.self_reply_to_addr)
    }

    /// tries to initialize the work in progress buffer, returning `true` on success and `false`
    ///  if the send buffer has reached its configured upper bound
    #[must_use]
    fn init_wip(&mut self, first_message_offset: Option<u16>) -> bool {
        assert!(self.work_in_progress.is_none());

        if self.work_in_progress_packet_id >= self.low_water_mark() + self.config.send_window_size.safe_cast() {
            // the send buffer has reached its configured capacity, so we wait for it to decrease
            //  before increasing it
            return false;
        }

        let mut new_buffer = self.buffer_pool.get_from_pool();
        PacketHeader::new(
            self.self_reply_to_addr, PacketKind::RegularSequenced {
                stream_id: self.stream_id,
                first_message_offset,
                packet_sequence_number: self.work_in_progress_packet_id,
            },
            self.self_generation,
            self.peer_generation(),
        ).ser(&mut new_buffer);

        self.work_in_progress = Some(new_buffer);
        true
    }

    fn peer_generation(&self) -> Option<u64> {
        self.peer_generations.load().get(&self.peer_addr).cloned()
    }

    async fn send_send_sync(&self) {
        debug!("Sending send sync");
        
        let header = PacketHeader::new(
            self.self_reply_to_addr,
            PacketKind::ControlSendSync { stream_id: self.stream_id },
            self.self_generation,
            self.peer_generation(),
        );

        let mut send_buf = self.buffer_pool.get_from_pool();

        header.ser(&mut send_buf);

        ControlMessageSendSync {
            send_buffer_high_water_mark: self.work_in_progress_packet_id,
            send_buffer_low_water_mark: self.low_water_mark(),
        }.ser(&mut send_buf);

        self.send_socket.finalize_and_send_packet(self.peer_addr, &mut send_buf).await;
        self.buffer_pool.return_to_pool(send_buf);
    }

    fn low_water_mark(&self) -> PacketId {
        self.send_buffer.keys().next()
            .cloned()
            .unwrap_or(self.work_in_progress_packet_id)
    }

    /// The congestion window may have grown or shrunk, or packets may have been acknowledged or
    ///  buffered - this function sends what can be sent. It assumes that all other state is up to
    ///  date when it is called.
    async fn do_send_what_congestion_window_allows(&mut self) {
        let cwnd = self.low_water_mark() + self.congestion_control.cwnd().safe_cast();

        while self.next_unsent_packet_id < self.work_in_progress_packet_id
            && self.next_unsent_packet_id < cwnd
        {
            self.send_socket.do_send_packet(
                self.peer_addr,
                self.send_buffer.get(&self.next_unsent_packet_id)
                    .unwrap()
                    .as_ref()
            ).await;
            self.next_unsent_packet_id += 1;
        }
    }

    fn on_send_window_changed_maybe(&self) {
        let _ = self.send_window_changed_notifier.try_send(());
    }

    async fn do_send_work_in_progress(&mut self) {
        let mut wip = self.work_in_progress.take()
            .expect("attempting to send uninitialized wip");

        trace!("actually sending packet to {:?} on stream {}: {:?}", self.peer_addr, self.stream_id, wip.as_ref());

        self.send_socket.finalize_packet(&mut wip)
            .instrument(Span::current())
            .await;
        self.send_buffer.insert(self.work_in_progress_packet_id, wip);

        if let Some(handle) = &self.work_in_progress_late_send_handle {
            handle.abort();
            self.work_in_progress_late_send_handle = None;
        }

        if let Some(out_of_window) = self.work_in_progress_packet_id - self.config.send_window_size.safe_cast() {
            if let Some(dropped_buf) = self.send_buffer.remove(&out_of_window) {
                debug!("unacknowledged packet moved out of the send window for stream {} with {:?}", self.stream_id, self.peer_addr);
                self.buffer_pool.return_to_pool(dropped_buf);
            }
        }

        self.work_in_progress_packet_id += 1;

        self.do_send_what_congestion_window_allows().await;
    }
}


pub struct SendStream {
    config: Arc<EffectiveSendStreamConfig>,
    inner: Arc<RwLock<SendStreamInner>>,
    /// A lock held by calls to send_message to prevent interleaving of messages. It protects
    ///  access to a receiver of notifications that the send window may have moved, facilitating
    ///  back pressure when the send buffer is filled up
    send_lock: Mutex<mpsc::Receiver<()>>,
}

impl SendStream {
    pub fn new(
        config: Arc<EffectiveSendStreamConfig>,
        self_generation: u64,
        peer_generations: Arc<AtomicMap<SocketAddr, u64>>,
        stream_id: u16,
        send_socket: Arc<SendPipeline>,
        peer_addr: SocketAddr,
        reply_to_addr: Option<SocketAddr>,
        buffer_pool: Arc<SendBufferPool>,
    ) -> SendStream {
        // for notifications that the send window changed
        let (send, recv) = mpsc::channel(1);

        let inner = SendStreamInner {
            config: config.clone(),
            buffer_pool,
            self_generation,
            peer_generations,
            stream_id,
            send_socket,
            peer_addr,
            self_reply_to_addr: reply_to_addr,
            send_buffer: BTreeMap::default(),
            send_window_changed_notifier: send,
            congestion_control: HsCongestionControl::new(config.send_window_size),
            fed_to_congestion_control_marker: PacketId::ZERO,
            next_unsent_packet_id: PacketId::ZERO,
            work_in_progress_packet_id: PacketId::ZERO,
            work_in_progress: None,
            work_in_progress_late_send_handle: None,
        };

        SendStream {
            config,
            inner: Arc::new(RwLock::new(inner)),
            send_lock: Mutex::new(recv),
        }
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn on_recv_sync_message(&self, message: ControlMessageRecvSync) {
        let mut inner = self.inner.write().await;
        trace!("received RECV_SYNC message from {:?} for stream {}: {:?}", inner.peer_addr, inner.stream_id, message);

        // update congestion control: we start by ACK'ing everything up to the ACK threshold the receiver sent
        let num_packets_in_flight = (inner.work_in_progress_packet_id.to_raw() - inner.low_water_mark().to_raw()).prechecked_cast();
        for _ in inner.fed_to_congestion_control_marker.to_raw()..message.receive_buffer_ack_threshold.to_raw() {
            inner.congestion_control.on_ack(num_packets_in_flight)
        }
        inner.fed_to_congestion_control_marker = max(inner.fed_to_congestion_control_marker, message.receive_buffer_ack_threshold);

        // then we NAK all packets for which a resend was requested
        for _ in 0..message.packet_id_resend_set.len() {
            inner.congestion_control.on_nak();
        }

        // resend packets requested by the receiver
        for packet_id in &message.packet_id_resend_set {
            if let Some(packet) = inner.send_buffer.get(packet_id) {
                inner.send_socket.do_send_packet(inner.peer_addr, packet.as_ref()).await;
            }
            else {
                debug!("NAK requested packet that is no longer in the send buffer: {}", packet_id.to_raw());
            }
        }

        // remove all packets 'up to' the threshold from the send buffer
        loop {
            if let Some(&key) = inner.send_buffer.keys().next() {
                if key < message.receive_buffer_ack_threshold {
                    if let Some(removed) = inner.send_buffer.remove(&key) {
                        inner.buffer_pool.return_to_pool(removed);
                    }
                    continue;
                }
            }
            break;
        }

        inner.next_unsent_packet_id = max(inner.next_unsent_packet_id, inner.low_water_mark());

        if message.receive_buffer_ack_threshold < inner.low_water_mark() {
            inner.send_send_sync().await;
        }

        inner.do_send_what_congestion_window_allows().await;
        inner.on_send_window_changed_maybe();
    }

    pub async fn send_message(&self, required_peer_generation: Option<u64>, mut message: &[u8]) -> anyhow::Result<()> {
        let mut send_lock = self.send_lock.lock().await;

        if message.len() > self.config.max_message_len {
            debug!("message has length {}, configured max length is {}", message.len(), self.config.max_message_len);
            bail!("message has length {}, configured max length is {}", message.len(), self.config.max_message_len);
        }

        let mut inner = self.inner.write().await;

        if required_peer_generation.is_some() && inner.peer_generation() != required_peer_generation {
            debug!("discarded message sent to {:?}: client requested generation {:?} but known generation was {:?}",
                inner.peer_addr,
                required_peer_generation.unwrap(),
                inner.peer_generation(),
            );

            return Ok(()); //TODO unit test
        }

        let correlation_id = Uuid::new_v4();
        let span = span!(Level::TRACE, "send_message", ?correlation_id);
        let _entered = span.enter();

        debug!("registering message of length {} for sending to {:?} on stream {:?}", message.len(), inner.peer_addr, inner.stream_id);

        if let Some(wip) = &mut inner.work_in_progress {
            // if the message header does not fit in wip, send and re-init wip
            if wip.len() + MessageHeader::SERIALIZED_LEN > self.config.max_payload_len {
                inner.do_send_work_in_progress()
                    .instrument(Span::current())
                    .await;

                loop {
                    if inner.init_wip(Some(0)) {
                        break;
                    }
                    // drain previous change notifications to avoid an unnecessary additional round in the loop
                    // NB: we do this *before* releasing the lock on 'inner': all changes to the
                    //  send buffer require the lock on inner to be held.
                    let _ = send_lock.try_recv();
                    drop(inner);
                    // Now we wait for some event - typically an ACK on some old packets - to free
                    //  up some of the send buffer.
                    // NB: It is critically important to do this wait *without* holding a lock
                    //      on inner because all freeing up of the send buffer requires that lock.
                    //      Waiting while holding that lock would create a deadlock.
                    // NB: It is important to keep holding the send_lock because we don't want other
                    //      calls to send_message to interleave
                    let _ = send_lock.recv().await;
                    inner = self.inner.write().await;
                }
            }
        }
        else {
            // if there is no previous wip, initialize it
            loop {
                if inner.init_wip(Some(0)) {
                    break;
                }
                let _ = send_lock.try_recv();
                drop(inner);
                let _ = send_lock.recv().await;
                inner = self.inner.write().await;
            }
        }

        let mut wip_buffer = inner.work_in_progress.as_mut().unwrap();

            // write message header
        MessageHeader::for_message(message)
            .ser(&mut wip_buffer);

        // while there is some part of the message left: copy it into WIP, sending and
        //  re-initializing as necessary
        loop {
            debug_assert!(wip_buffer.len() <= self.config.max_payload_len);

            let remaining_capacity = self.config.max_payload_len - wip_buffer.len();
            let slice_len = min(remaining_capacity, message.len());
            wip_buffer.put_slice(&message[..slice_len]);
            message = &message[slice_len..];

            if message.is_empty() {
                break;
            }

            // getting here means that the message continues in (at least) the next packet

            inner.do_send_work_in_progress()
                .instrument(Span::current())
                .await;

            let first_message_offset = if inner.buffer_pool.get_envelope_prefix_len() +
                message.len() +
                inner.packet_header_len()
                > self.config.max_payload_len
            {
                None
            }
            else {
                Some(message.len().prechecked_cast())
            };

            loop {
                if inner.init_wip(first_message_offset) {
                    break;
                }
                let _ = send_lock.try_recv();
                drop(inner);
                let _ = send_lock.recv().await;
                inner = self.inner.write().await;
            }

            wip_buffer = inner.work_in_progress.as_mut().unwrap();
        }

        if wip_buffer.len() + MessageHeader::SERIALIZED_LEN > self.config.max_payload_len {
            // there is no room for another message header in the buffer, so we send it
            inner.do_send_work_in_progress()
                .instrument(Span::current())
                .await
        }

        // start 'late send' timer
        if inner.work_in_progress.is_some() {
            if let Some(late_send_delay) = self.config.late_send_delay {
                let high_water_mark = inner.work_in_progress_packet_id;
                let inner_arc = self.inner.clone();

                inner.work_in_progress_late_send_handle = Some(tokio::spawn(async move {
                    time::sleep(late_send_delay).await;

                    let mut inner = inner_arc.write().await;
                    if inner.work_in_progress_packet_id != high_water_mark {
                        trace!("late send: packet {} already sent", high_water_mark);
                    }
                    else {
                        trace!("late send: sending packet {}", high_water_mark);
                        if inner.work_in_progress.is_some() {
                            inner.do_send_work_in_progress().await;
                        }
                        inner.work_in_progress_late_send_handle = None;
                    }
                }));
            }
            else {
                inner.do_send_work_in_progress()
                    .instrument(Span::current())
                    .await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safe_converter::SafeCast;
    use crate::send_pipeline::MockSendSocket;
    use mockall::predicate::eq;
    use rstest::*;
    use std::time::Duration;
    use tokio::runtime::Builder;

    fn create_peer_generations(with_peer_data: bool) -> Arc<AtomicMap<SocketAddr, u64>> {
        if !with_peer_data {
            return Default::default();
        }

        let map: AtomicMap<SocketAddr, u64> = Default::default();
        map.update(|m| { m.insert(([1,2,3,4], 9).into(), 4); });
        Arc::new(map)
    }

    #[rstest]
    #[case::thresholds_empty(true, 4, 3, 4, 3, vec![], 3, 3, 1, 2, vec![], vec![], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0]])]
    #[case::thresholds_empty(false, 4, 3, 4, 3, vec![], 3, 3, 1, 2, vec![], vec![], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,0, 0,7, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0]])]
    #[case::thresholds_filled(true, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 4, vec![], vec![4,5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,4]])]
    #[case::thresholds_filled(false, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 4, vec![], vec![4,5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,0, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,4]])]
    #[case::thresholds_filled_too_low_1(true, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 3, vec![], vec![4,5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,4]])]
    #[case::thresholds_filled_too_low_2(true, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 1, vec![], vec![4,5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,4]])]
    #[case::thresholds_ack_truncate(true, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 5, vec![], vec![5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,5]])]
    #[case::thresholds_ack_truncate_all(true, 4, 7, 4, 7, vec![4,5,6], 7, 7, 4, 7, vec![], vec![], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,7]])]
    #[case::thresholds_ack_truncate_all_too_high_1(true, 4, 7, 4, 8, vec![4,5,6], 7, 7, 4, 8, vec![], vec![], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,7]])]
    #[case::thresholds_ack_truncate_all_too_high_2(true, 4, 7, 4, 888, vec![4,5,6], 7, 7, 4, 888, vec![], vec![], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,7]])]
    #[case::thresholds_ignore_high_low_1(true, 4, 5, 4, 5, vec![4,5,6], 7, 3, 9, 5, vec![], vec![5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,5]])]
    #[case::thresholds_ignore_high_low_2(true, 4, 5, 4, 5, vec![4,5,6], 7, 5, 5, 5, vec![], vec![5,6], vec![vec![0,22, 0,0,0,0,0,4, 0,0,0,0,0,4, 0,7, 0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,5]])]

    #[case::nak_empty(true, 4, 4, 4, 4, vec![1,2,3], 4, 4, 1, 1, vec![], vec![1,2,3], vec![])]
    #[case::nak_all_1(true, 4, 5, 2, 5, vec![5], 6, 6, 5, 5, vec![5], vec![5], vec![vec![5]])]
    #[case::nak_all_2(true, 4, 5, 2, 5, vec![5,6], 7, 7, 5, 5, vec![5,6], vec![5,6], vec![vec![5], vec![6]])]
    #[case::nak_select_1(true, 4, 6, 2, 6, vec![5,6,7], 8, 8, 5, 5, vec![6], vec![5,6,7], vec![vec![6]])]
    #[case::nak_select_2(true, 4, 5, 2, 5, vec![5,6,7], 8, 8, 5, 5, vec![5,7], vec![5,6,7], vec![vec![5],vec![7]])]
    #[case::nak_some_out_of_range(true, 4, 4, 2, 5, vec![5,6,7], 8, 8, 5, 5, vec![4,5], vec![5,6,7], vec![vec![5]])]
    #[case::nak_all_out_of_range(true, 4, 3, 2, 5, vec![5,6,7], 8, 8, 5, 5, vec![3,4], vec![5,6,7], vec![])]
    #[case::nak_some_above_range(true, 4, 5, 2, 5, vec![5,6,7], 8, 8, 5, 5, vec![5,8], vec![5,6,7], vec![vec![5]])]
    #[case::nak_all_above_range(true, 4, 8, 2, 8, vec![5,6,7], 8, 8, 5, 5, vec![8,9], vec![5,6,7], vec![])]

    #[case::send_unsent(true, 3, 11, 3, 11, vec![8,9,10,11], 11, 10, 7, 8, vec![], vec![8,9,10,11], vec![vec![11]])]
    #[case::send_unsent(true, 2, 10, 2, 10, vec![8,9,10,11], 10, 10, 7, 8, vec![], vec![8,9,10,11], vec![vec![10]])]
    #[case::send_unsent_cwnd_increase(true, 2, 8, 3, 9, vec![8,9,10,11], 10, 10, 7, 9, vec![], vec![9,10,11], vec![vec![10], vec![11]])]
    
    #[case::cwnd_nak_already_fed(true, 4, 11, 2, 11, vec![8,9,10,11], 12, 10, 7, 8, vec![10], vec![8,9,10,11], vec![vec![10]])]
    #[case::cwnd_two_naks(true, 20, 11, 5, 11, vec![8,9,10,11], 12, 10, 7, 8, vec![9,10], vec![8,9,10,11], vec![vec![9], vec![10]])]
    #[case::cwnd_not_limited(true, 5, 11, 5, 12, vec![10,11], 12, 12, 7, 12, vec![], vec![], vec![])] //NB: slow start -> half utilized would be enough
    fn test_on_recv_sync(
        #[case] with_peer_generation: bool,
        #[case] initial_cwnd: u32,
        #[case] initial_fed_to_congestion_control_marker: u64,
        #[case] expected_cwnd: u32,
        #[case] expected_fed_to_congestion_control_marker: u64,
        #[case] initial_send_buffer_ids: Vec<u8>,
        #[case] next_unsent_packet_id: u64,
        #[case] msg_high_water_mark: u64,
        #[case] msg_low_water_mark: u64,
        #[case] msg_ack_threshold: u64,
        #[case] msg_nak_ids: Vec<u64>,
        #[case] expected_send_buffer_ids: Vec<u64>,
        #[case] expected_messages: Vec<Vec<u8>>,
    ) {
        let msg = ControlMessageRecvSync {
            receive_buffer_high_water_mark: PacketId::from_raw(msg_high_water_mark),
            receive_buffer_low_water_mark: PacketId::from_raw(msg_low_water_mark),
            receive_buffer_ack_threshold: PacketId::from_raw(msg_ack_threshold),
            packet_id_resend_set: msg_nak_ids.into_iter().map(PacketId::from_raw).collect(),
        };

        let rt = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build().unwrap();
        rt.block_on(async move {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1,2,3,4], 8)));
            for msg in expected_messages {
                send_socket.expect_do_send_packet()
                    .with(
                        eq(SocketAddr::from(([1,2,3,4], 9))),
                        eq(FixedBuf::from_slice(40, msg.as_slice())),
                    )
                    .return_const(())
                ;
            }

            let send_stream = SendStream::new(
                Arc::new(EffectiveSendStreamConfig {
                    max_payload_len: 30,
                    late_send_delay: None,
                    send_window_size: 4,
                    max_message_len: 1024*1024,
                }),
                4,
                create_peer_generations(with_peer_generation),
                7,
                Arc::new(SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}))),
                SocketAddr::from(([1,2,3,4], 9)),
                None,
                Arc::new(SendBufferPool::new(40, 10, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            );

            let expected_send_buffer_ids = expected_send_buffer_ids
                .into_iter()
                .map(PacketId::from_raw)
                .collect::<Vec<_>>();

            {
                let mut inner = send_stream.inner.write().await;

                for packet_id in initial_send_buffer_ids {
                    let mut buf = FixedBuf::new(40);
                    buf.put_u8(packet_id);

                    let buf2 = FixedBuf::from_slice(40, vec![packet_id].as_slice());

                    assert_eq!(buf, buf2);

                    inner.send_buffer.insert(PacketId::from_raw(packet_id.safe_cast()), buf);
                    inner.work_in_progress_packet_id = PacketId::from_raw(packet_id.safe_cast() + 1);
                }

                inner.congestion_control.set_internals(0, initial_cwnd, 0);
                inner.fed_to_congestion_control_marker = PacketId::from_raw(initial_fed_to_congestion_control_marker);
                inner.next_unsent_packet_id = PacketId::from_raw(next_unsent_packet_id);
            }

            send_stream.on_recv_sync_message(msg).await;

            let inner = send_stream.inner.read().await;
            assert_eq!(inner.send_buffer.keys().cloned().collect::<Vec<_>>(), expected_send_buffer_ids);
            assert_eq!(inner.congestion_control.cwnd(), expected_cwnd);
            assert_eq!(inner.fed_to_congestion_control_marker, PacketId::from_raw(expected_fed_to_congestion_control_marker));
        });
    }


    #[rstest]
    #[case::single_late_send_nowait    (true,  Some(5), 42, 0, 0, vec![vec![1,2,3]], vec![], 0, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3]))]
    #[case::single_late_send_nowait    (false, Some(5), 42, 0, 0, vec![vec![1,2,3]], vec![], 0, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3]))]
    #[case::single_late_send_wait_short(true,  Some(5), 42, 4, 0, vec![vec![1,2,3]], vec![], 0, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3]))]
    #[case::single_late_send_wait_short(false, Some(5), 42, 4, 0, vec![vec![1,2,3]], vec![], 0, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3]))]
    #[case::single_late_send_times_out (true,  Some(5), 42, 6, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::single_late_send_times_out (false, Some(5), 42, 6, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::single_no_late_send(true,  None, 42, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::single_no_late_send(false, None, 42, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]

    #[case::single_non_zero_packet_id(true, Some(5), 41, 0, 16385, vec![vec![1,2,3]], vec![], 16385, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,64,1, 0,0,0,3,1,2,3]))]
    #[case::two_messages_one_packet_no_late_send(true, Some(5), 52, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![], 7, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7]))]
    #[case::two_messages_one_packet_late_send   (true, Some(5), 52, 4, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7])], 8, None)]

    #[case::one_message_full_enough0(true, Some(5), 33, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::one_message_full_enough1(true, Some(5), 34, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::one_message_full_enough2(true, Some(5), 35, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::one_message_full_enough3(true, Some(5), 36, 0, 0, vec![vec![1,2,3]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3])], 1, None)]
    #[case::one_message_message_header_fits(true, Some(5), 37, 0, 0, vec![vec![1,2,3]], vec![], 0, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3]))]
    #[case::two_messages_one_packet_full_enough0(true, Some(5), 41, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7])], 8, None)]
    #[case::two_messages_one_packet_full_enough1(true, Some(5), 42, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7])], 8, None)]
    #[case::two_messages_one_packet_full_enough2(true, Some(5), 43, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7])], 8, None)]
    #[case::two_messages_one_packet_full_enough3(true, Some(5), 44, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7])], 8, None)]
    #[case::two_messages_one_packet_message_header_fits (true, Some(5), 45, 0, 7, vec![vec![1,2,3], vec![4,5,6,7]], vec![], 7, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3, 0,0,0,4,4,5,6,7]))]

    #[case::one_message_spans_two_packets3(true,  None, 30, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3, 0,0,0,0,0,0,0,8, 1,2,3])], 9, None)]
    #[case::one_message_spans_two_packets3(false, None, 30, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,3, 0,0,0,0,0,0,0,8, 1,2,3])], 9, None)]
    #[case::one_message_spans_two_packets2(true, None, 31, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,2, 0,0,0,0,0,0,0,8, 2,3])], 9, None)]
    #[case::one_message_spans_two_packets1(true, None, 32, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,1, 0,0,0,0,0,0,0,8, 3])], 9, None)]
    #[case::one_message_spans_two_packets0(true, None, 33, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3])], 8, None)]
    #[case::one_message_spans_three_packets(true,  None, 30, 0, 6, vec![vec![1,2,3,4,5]], vec![(6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,6, 0,0,0,5]), (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 255,255, 0,0,0,0,0,0,0,7, 1,2,3,4]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,1, 0,0,0,0,0,0,0,8, 5])], 9, None)]
    #[case::one_message_spans_three_packets(false, None, 30, 0, 6, vec![vec![1,2,3,4,5]], vec![(6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,0, 0,0,0,0,0,0,0,6, 0,0,0,5]), (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 255,255, 0,0,0,0,0,0,0,7, 1,2,3,4]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,0, 0,4, 0,1, 0,0,0,0,0,0,0,8, 5])], 9, None)]

    #[case::one_message_spans_two_packets3_late_send(true, Some(3), 30, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3, 0,0,0,0,0,0,0,8, 1,2,3])], 9, None)]
    #[case::one_message_spans_two_packets2_late_send(true, Some(3), 31, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,2, 0,0,0,0,0,0,0,8, 2,3])], 9, None)]
    #[case::one_message_spans_two_packets1_late_send(true, Some(3), 32, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2])], 8, Some(vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,1, 0,0,0,0,0,0,0,8, 3]))]
    #[case::one_message_spans_two_packets0_late_send(true, Some(3), 33, 0, 7, vec![vec![1,2,3]], vec![(7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,7, 0,0,0,3,1,2,3])], 8, None)]
    #[case::one_message_spans_three_packets_late_send(true, Some(3), 30, 0, 6, vec![vec![1,2,3,4,5]], vec![(6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,6, 0,0,0,5]), (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 255,255, 0,0,0,0,0,0,0,7, 1,2,3,4]), (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,1, 0,0,0,0,0,0,0,8, 5])], 9, None)]

    #[case::one_message_spans_two_packets_start_middle(true, Some(3), 33, 0, 6, vec![vec![1,2,3,4,5,6], vec![7,6,5,4]], vec![
        (6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,6, 0,0,0,6,1,2,3]),
        (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3, 0,0,0,0,0,0,0,7, 4,5,6,0,0,0,4]), //NB: This tests handling of a continuation message with only the message header fitting
        (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,4, 0,0,0,0,0,0,0,8, 7,6,5,4]),
    ], 9, None)]
    #[case::one_message_spans_two_packets_start_middle_delay(true, Some(3), 33, 4, 6, vec![vec![1,2,3,4,5,6], vec![7,6]], vec![
        (6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,6, 0,0,0,6,1,2,3]),
        (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3, 0,0,0,0,0,0,0,7, 4,5,6,0,0,0,2]),
        (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,2, 0,0,0,0,0,0,0,8, 7,6]),
    ], 9, None)]
    #[case::one_message_spans_three_packets_start_middle(true, Some(3), 33, 4, 6, vec![vec![1,2,3,4,5,6], vec![7,6,5,4,3,2,1,0]], vec![
        (6, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0,     0,0,0,0,0,0,0,6, 0,0,0,6,1,2,3]),
        (7, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3,     0,0,0,0,0,0,0,7, 4,5,6,0,0,0,8]),
        (8, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 255,255, 0,0,0,0,0,0,0,8, 7,6,5,4,3,2,1]),
        (9, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,1,     0,0,0,0,0,0,0,9, 0]),
    ], 10, None)]

    #[case::cancel_late_send_no_overspill  (true, Some(10), 42, 100, 0, vec![vec![1,2,3], vec![4,5,6]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3, 0,0,0,3,4,5,6])], 1, None)]
    #[case::cancel_late_send_with_overspill(true, Some(10), 37, 100, 0, vec![vec![1,2,3], vec![4,5,6]], vec![(0, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,0, 0,0,0,0,0,0,0,0, 0,0,0,3,1,2,3, 0,0,0,3]), (1, vec![0,2, 0,0,0,0,0,5, 0,0,0,0,0,4, 0,4, 0,3, 0,0,0,0,0,0,0,1, 4,5,6])], 2, None)]
    fn test_send_message(
        #[case] with_peer_generation: bool,
        #[case] late_send_delay: Option<u64>,
        #[case] max_payload_len: usize,
        #[case] delay_millis_after_messages: u64,
        #[case] previous_wip_packet_id: u64,
        #[case] messages: Vec<Vec<u8>>,
        #[case] expected_buffers: Vec<(u64, Vec<u8>)>,
        #[case] expected_wip_packet_id: u64,
        #[case] expected_wip: Option<Vec<u8>>,
    ) {
        let late_send_delay = late_send_delay.map(Duration::from_millis);

        let mut send_socket = MockSendSocket::new();
        send_socket.expect_local_addr()
            .return_const(SocketAddr::from(([1,2,3,4], 8)));
        for (_, expected_buf) in expected_buffers.clone() {
            send_socket.expect_do_send_packet()
                .with(
                    eq(SocketAddr::from(([1,2,3,4], 9))),
                    eq(expected_buf),
                )
                .return_const(())
            ;
        }

        let send_stream = SendStream::new(
            Arc::new(EffectiveSendStreamConfig {
                max_payload_len: max_payload_len,
                late_send_delay,
                send_window_size: 4,
                max_message_len: 1024*1024,
            }),
            5,
            create_peer_generations(with_peer_generation),
            4,
            Arc::new(SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}))),
            SocketAddr::from(([1,2,3,4], 9)),
            None,
            Arc::new(SendBufferPool::new(max_payload_len, 10, Arc::new(crate::buffers::encryption::NoEncryption {}))),
        );

        let rt = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build().unwrap();
        rt.block_on(async move {
            {
                let mut inner = send_stream.inner.write().await;
                inner.work_in_progress_packet_id = PacketId::from_raw(previous_wip_packet_id);
                inner.next_unsent_packet_id = inner.work_in_progress_packet_id; //TODO unit test congestion window
            }

            for message in messages {
                time::sleep(Duration::from_millis(2)).await;
                send_stream.send_message(None, &message).await.unwrap();
            }

            time::sleep(Duration::from_millis(delay_millis_after_messages)).await;

            let inner = send_stream.inner.read().await;
            let actual_bufs = inner.send_buffer.iter()
                .map(|(id, buf)| (id.to_raw(), buf.as_ref().to_vec()))
                .collect::<Vec<_>>();
            assert_eq!(actual_bufs, expected_buffers);
            assert_eq!(inner.work_in_progress_packet_id, PacketId::from_raw(expected_wip_packet_id));
            assert_eq!(inner.work_in_progress.as_ref().map(|buf| buf.as_ref().to_vec()), expected_wip);
        });
    }

    #[rstest]
    #[case::initial(vec![], 0, 0)]
    #[case::just_wip(vec![], 5, 5)]
    #[case::just_wip_2(vec![], 9, 9)]
    #[case::send_buffer_1(vec![2], 3, 2)]
    #[case::send_buffer_2(vec![5, 6], 7, 5)]
    fn test_low_water_mark(#[case] send_buffer: Vec<u64>, #[case] wip_packet_id: u64, #[case] expected_low_water_mark: u64) {
        let (send, _recv) = mpsc::channel(1);
        let mut inner = SendStreamInner {
            config: Arc::new(EffectiveSendStreamConfig {
                max_payload_len: 0,
                late_send_delay: None,
                send_window_size: 4,
                max_message_len: 1024*1024,
            }),
            buffer_pool: Arc::new(SendBufferPool::new(0, 10, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            self_generation: 3,
            peer_generations: Default::default(),
            stream_id: 4,
            send_socket: Arc::new(SendPipeline::new(Arc::new(MockSendSocket::new()), Arc::new(crate::buffers::encryption::NoEncryption {}))),
            send_window_changed_notifier: send,
            peer_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            self_reply_to_addr: None,
            send_buffer: Default::default(),
            congestion_control: HsCongestionControl::new(100),
            fed_to_congestion_control_marker: PacketId::ZERO,
            next_unsent_packet_id: PacketId::from_raw(wip_packet_id),
            work_in_progress_packet_id: PacketId::from_raw(wip_packet_id),
            work_in_progress: None,
            work_in_progress_late_send_handle: None,
        };
        for packet in send_buffer {
            inner.send_buffer.insert(PacketId::from_raw(packet), FixedBuf::new(10));
        }

        assert_eq!(inner.low_water_mark(), PacketId::from_raw(expected_low_water_mark));
    }

    #[rstest]
    #[case::same(None)]
    #[case::v4(Some(SocketAddr::from(([127,0,0,1], 5))))]
    #[case::v6(Some(SocketAddr::from(([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4], 5))))]
    fn test_packet_header_len(#[case] reply_to_addr: Option<SocketAddr>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let send_stream = SendStream::new(
                Arc::new(EffectiveSendStreamConfig {
                    max_payload_len: 0,
                    late_send_delay: None,
                    send_window_size: 10,
                    max_message_len: 1024*1024,
                }),
                3,
                Default::default(),
                4,
                Arc::new(SendPipeline::new(Arc::new(MockSendSocket::default()), Arc::new(crate::buffers::encryption::NoEncryption {}))),
                SocketAddr::from(([127, 0, 0, 1], 0)),
                reply_to_addr,
                Arc::new(SendBufferPool::new(0, 10, Arc::new(crate::buffers::encryption::NoEncryption {}))),
            );

            let actual = send_stream.inner.read().await
                .packet_header_len();

            let expected = PacketHeader::serialized_len_for_stream_header(reply_to_addr);
            assert_eq!(actual, expected);
        });
    }

    #[rstest]
    #[case::empty(true, vec![], 0, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,4, 0,4, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::empty(false, vec![], 0, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,0, 0,4, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::just_wip(true, vec![], 6, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,4, 0,4, 0,0,0,0,0,0,0,6, 0,0,0,0,0,0,0,6])]
    #[case::just_wip(false, vec![], 6, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,0, 0,4, 0,0,0,0,0,0,0,6, 0,0,0,0,0,0,0,6])]
    #[case::buf(true, vec![3,4], 5, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,4, 0,4, 0,0,0,0,0,0,0,5, 0,0,0,0,0,0,0,3])]
    #[case::buf(false, vec![3,4], 5, vec![0,22, 0,0,0,0,0,3, 0,0,0,0,0,0, 0,4, 0,0,0,0,0,0,0,5, 0,0,0,0,0,0,0,3])]
    fn test_send_send_sync(#[case] with_peer_generation: bool, #[case] send_buffer: Vec<u64>, #[case] wip_packet_id: u64, #[case] expected_buf: Vec<u8>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut send_socket = MockSendSocket::new();
            send_socket.expect_local_addr()
                .return_const(SocketAddr::from(([1,2,3,4], 8)));
            send_socket.expect_do_send_packet()
                .once()
                .withf(move |addr, buf|
                    addr == &SocketAddr::from(([1,2,3,4], 9)) &&
                        buf == expected_buf.as_slice()
                )
                .returning(|_, _| ())
            ;

            let (send, _recv) = mpsc::channel(1);

            let mut inner = SendStreamInner {
                config: Arc::new(EffectiveSendStreamConfig {
                    max_payload_len: 0,
                    late_send_delay: None,
                    send_window_size: 4,
                    max_message_len: 1024*1024,
                }),
                buffer_pool: Arc::new(SendBufferPool::new(100, 10, Arc::new(crate::buffers::encryption::NoEncryption {}))),
                self_generation: 3,
                peer_generations: create_peer_generations(with_peer_generation),
                stream_id: 4,
                send_socket: Arc::new(SendPipeline::new(Arc::new(send_socket), Arc::new(crate::buffers::encryption::NoEncryption {}))),
                peer_addr: SocketAddr::from(([1,2,3,4], 9)),
                self_reply_to_addr: None,
                send_buffer: Default::default(),
                send_window_changed_notifier: send,
                congestion_control: HsCongestionControl::new(100),
                fed_to_congestion_control_marker: PacketId::ZERO,
                next_unsent_packet_id: PacketId::from_raw(wip_packet_id),
                work_in_progress_packet_id: PacketId::from_raw(wip_packet_id),
                work_in_progress: None,
                work_in_progress_late_send_handle: None,
            };
            for packet in send_buffer {
                inner.send_buffer.insert(PacketId::from_raw(packet), FixedBuf::new(10));
            }

            inner.send_send_sync().await;
        });
    }
}
