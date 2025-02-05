use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync};
use crate::packet_header::{PacketHeader, PacketKind};
use crate::send_socket::SendSocket;
use bytes::{BufMut, BytesMut};
use std::cmp::min;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, trace};
use crate::message_header::MessageHeader;
use crate::packet_id::PacketId;

pub struct SendStreamConfig {
    pub max_packet_len: usize, //TODO calculated from MTU, encryption wrapper, ...
    pub late_send_delay: Option<Duration>,
    pub send_window_size: u64, //TODO ensure that this is <= u32::MAX / 4 (or maybe a far smaller upper bound???)
}

struct SendStreamInner {
    config: Arc<SendStreamConfig>,
    stream_id: u16,
    send_socket: Arc<SendSocket>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,

    send_buffer: BTreeMap<PacketId, BytesMut>,
    /// next unsent packet, either already in 'work in progress' or next one to go there
    work_in_progress_packet_id: PacketId,
    work_in_progress: Option<BytesMut>,
    work_in_progress_late_send_handle: Option<tokio::task::JoinHandle<()>>,
}
impl SendStreamInner {
    fn packet_header_len(&self) -> usize {
        PacketHeader::serialized_len_for_stream_header(self.self_reply_to_addr)
    }

    //TODO unit test

    fn init_wip(&mut self, first_message_offset: Option<u16>) -> &mut BytesMut {
        assert!(self.work_in_progress.is_none());

        let mut new_buffer = BytesMut::with_capacity(self.config.max_packet_len); //TODO pool
        PacketHeader::new(self.self_reply_to_addr, PacketKind::RegularSequenced {
            stream_id: self.stream_id,
            first_message_offset,
            packet_sequence_number: self.work_in_progress_packet_id,
        }).ser(&mut new_buffer);

        self.work_in_progress = Some(new_buffer);
        self.work_in_progress.as_mut().unwrap()
    }

    async fn do_send_work_in_progress(&mut self) {
        let mut wip = self.work_in_progress.take()
            .expect("attempting to send uninitialized wip");

        trace!("actually sending packet to {:?} on stream {}", self.peer_addr, self.stream_id);

        self.send_socket.finalize_and_send_packet(self.peer_addr, &mut wip).await;
        //NB: we don't handle a send error but keep the (potentially) unsent packet in our send buffer

        self.send_buffer.insert(self.work_in_progress_packet_id, wip);

        if let Some(handle) = &self.work_in_progress_late_send_handle {
            handle.abort();
            self.work_in_progress_late_send_handle = None;
        }

        //TODO check for off-by-one
        if let Some(out_of_window) = self.work_in_progress_packet_id - self.config.send_window_size {
            if let Some(dropped_buf) = self.send_buffer.remove(&out_of_window) {
                //TODO return buf to pool
                debug!("unacknowledged packet moved out of the send window for stream {} with {:?}", self.stream_id, self.peer_addr);
            }
        }

        self.work_in_progress_packet_id += 1;
    }
}


pub struct SendStream {
    config: Arc<SendStreamConfig>,
    inner: Arc<RwLock<SendStreamInner>>,
}

impl SendStream {
    pub fn new(
        config: Arc<SendStreamConfig>,
        stream_id: u16,
        send_socket: Arc<SendSocket>,
        peer_addr: SocketAddr,
        reply_to_addr: Option<SocketAddr>,
    ) -> SendStream {
        let inner = SendStreamInner {
            config: config.clone(),
            stream_id,
            send_socket,
            peer_addr,
            self_reply_to_addr: reply_to_addr,
            send_buffer: BTreeMap::default(),
            work_in_progress_packet_id: PacketId::ZERO,
            work_in_progress: None,
            work_in_progress_late_send_handle: None,
        };

        SendStream {
            config,
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn on_init_message(&self) {
        let mut inner = self.inner.write().await;
        debug!("received INIT message from {:?} for stream {}", inner.peer_addr, inner.stream_id);

        // discard all previously sent packets
        inner.work_in_progress = None;
        inner.send_buffer.clear();

        // reply with a SEND_SYNC so the client can adjust its receive window
        inner.send_socket.send_send_sync(
            inner.self_reply_to_addr,
            inner.peer_addr,
            inner.stream_id,
            inner.work_in_progress_packet_id,
            inner.work_in_progress_packet_id,
        ).await;
    }

    pub async fn on_recv_sync_message(&self, message: ControlMessageRecvSync) {
        let mut inner = self.inner.write().await;
        trace!("received RECV_SYNC message from {:?} for stream {}: {:?}", inner.peer_addr, inner.stream_id, message);

        // remove all packets 'up to' the threshold from the send buffer - wrap-around semantics
        //  add some boilerplate

        loop {
            if let Some(&key) = inner.send_buffer.keys().next() {
                if key < message.receive_buffer_ack_threshold {
                    inner.send_buffer.remove(&key);
                    continue;
                }
            }
            break;
        }

        //TODO handle / evaluate the client's packet ids

        let low_water_mark = inner.send_buffer.keys().next()
            .map(|k| *k + 1)
            .unwrap_or(inner.work_in_progress_packet_id);

        inner.send_socket.send_send_sync(inner.self_reply_to_addr, inner.peer_addr, inner.stream_id, inner.work_in_progress_packet_id, low_water_mark).await;
    }

    pub async fn on_nak_message(&self, message: ControlMessageNak) {
        let inner = self.inner.read().await;
        trace!("received NAK message from {:?} for stream {} - resending packets {:?}", inner.peer_addr, inner.stream_id, message.packet_id_resend_set);

        for packet_id in &message.packet_id_resend_set {
            if let Some(packet) = inner.send_buffer.get(packet_id) {
                inner.send_socket.do_send_packet(inner.peer_addr, packet).await;
            }
            else {
                debug!("NAK requested packet that is no longer in the send buffer");
            }
        }
    }

    //TODO add some tracing here for correlation

    /// NB: This function does not return Result because all retry / recovery handling is expected
    ///      to be done here
    pub async fn send_message(&self, mut message: &[u8]) {
        //TODO ensure message max length - configurable upper limit?

        let mut inner = self.inner.write().await;

        debug!("registering message of length {} for sending to {:?} on stream {:?}", message.len(), inner.peer_addr, inner.stream_id);

        let mut wip_buffer = if let Some(wip) = &mut inner.work_in_progress {
            // if the message header does not fit in wip, send and re-init wip
            if wip.len() + MessageHeader::SERIALIZED_LEN <= self.config.max_packet_len {
                wip
            }
            else {
                inner.do_send_work_in_progress().await;
                inner.init_wip(Some(0))
            }
        }
        else {
            // if there is no previous wip, initialize it
            inner.init_wip(Some(0))
        };

        // write message header
        MessageHeader::for_message(message)
            .ser(&mut wip_buffer);

        // while there is some part of the message left: copy it into WIP, sending and
        //  re-initializing as necessary
        loop {
            debug_assert!(wip_buffer.len() <= self.config.max_packet_len);

            let remaining_capacity = self.config.max_packet_len - wip_buffer.len();
            let slice_len = min(remaining_capacity, message.len());
            wip_buffer.put_slice(&message[..slice_len]);
            message = &message[slice_len..];

            if message.is_empty() {
                break;
            }

            // getting here means that the message continues in (at least) the next packet

            inner.do_send_work_in_progress().await;

            let first_message_offset = if message.len() + inner.packet_header_len() > self.config.max_packet_len {
                None
            }
            else {
                Some(message.len() as u16) //TODO overflow
            };
            wip_buffer = inner.init_wip(first_message_offset);
        }

        if wip_buffer.len() + MessageHeader::SERIALIZED_LEN > self.config.max_packet_len {
            // there is no room for another message header in the buffer, so we send it
            inner.do_send_work_in_progress().await
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
        }
    }
}
