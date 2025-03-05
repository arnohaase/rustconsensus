use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync, ControlMessageSendSync};
use crate::packet_header::{PacketHeader, PacketKind};
use crate::send_pipeline::SendPipeline;
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
    send_socket: Arc<SendPipeline>,
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

    async fn send_send_sync(&self) {
        let header = PacketHeader::new(self.self_reply_to_addr, PacketKind::ControlSendSync { stream_id: self.stream_id });

        let mut send_buf = BytesMut::with_capacity(1400); //TODO from pool?
        header.ser(&mut send_buf);

        ControlMessageSendSync {
            send_buffer_high_water_mark: self.work_in_progress_packet_id,
            send_buffer_low_water_mark: self.low_water_mark(),
        }.ser(&mut send_buf);

        self.send_socket.finalize_and_send_packet(self.peer_addr, &mut send_buf).await
    }

    fn low_water_mark(&self) -> PacketId {
        self.send_buffer.keys().next()
            .cloned()
            .unwrap_or(self.work_in_progress_packet_id)
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
        send_socket: Arc<SendPipeline>,
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
        inner.send_send_sync().await;
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
        inner.send_send_sync().await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use tokio::runtime::Builder;
    use crate::send_pipeline::MockSendSocket;

    #[rstest]
    fn test_on_init() {
        todo!()
    }

    #[rstest]
    fn test_on_nak() {
        todo!()
    }

    #[rstest]
    fn test_on_recv_sync() {
        todo!()
    }

    #[rstest]
    #[case::simple(vec![1,2,3], vec![], Some(vec![0,0,0,0,0,0,0,0,1,2,3]))]

    //TODO with / without pre-existing wip
    //TODO start / middle of packet
    //TODO fits / spills over / spans several
    //TODO pre-existing timer / new timer
    //TODO with / without late send delay
    #[case::todo(vec![1,2,3], vec![], Some(vec![0]))]
    fn test_send_message(
        #[case] message: Vec<u8>,
        #[case] expected_buffer: Vec<(u64, Vec<u8>)>,
        #[case] expected_wip: Option<Vec<u8>>,
    ) {
        let mut send_socket = MockSendSocket::new();
        send_socket.expect_local_addr()
            .return_const(SocketAddr::from(([1,2,3,4], 8)));
        // send_socket.expect_do_send_packet()
        //     .once()
        //     .withf(move |addr, buf|
        //         addr == &SocketAddr::from(([1,2,3,4], 9)) &&
        //             buf == expected_buf.as_slice()
        //     )
        //     .returning(|_, _| ())
        // ;

        let send_stream = SendStream::new(
            Arc::new(SendStreamConfig {
                max_packet_len: 20,
                late_send_delay: Some(Duration::from_millis(5)),
                send_window_size: 4,
            }),
            4,
            Arc::new(SendPipeline::new(Arc::new(send_socket))),
            SocketAddr::from(([1,2,3,4], 8)),
            None,
        );

        let rt = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build().unwrap();
        rt.block_on(async move {
            send_stream.send_message(&message).await;

            let inner = send_stream.inner.read().await;
            let actual_buf = inner.send_buffer.iter()
                .map(|(id, buf)| (id.to_raw(), buf.to_vec()))
                .collect::<Vec<_>>();
            assert_eq!(actual_buf, expected_buffer);
            assert_eq!(inner.work_in_progress.as_ref().map(|buf| buf.to_vec()), expected_wip);


        });
    }

    #[rstest]
    fn test_do_send_work_in_progress() {
        todo!()
    }

    #[rstest]
    fn test_init_wip() {
        todo!()
    }

    #[rstest]
    #[case::initial(vec![], 0, 0)]
    #[case::just_wip(vec![], 5, 5)]
    #[case::just_wip_2(vec![], 9, 9)]
    #[case::send_buffer_1(vec![2], 3, 2)]
    #[case::send_buffer_2(vec![5, 6], 7, 5)]
    fn test_low_water_mark(#[case] send_buffer: Vec<u64>, #[case] wip_packet_id: u64, #[case] expected_low_water_mark: u64) {
        let mut inner = SendStreamInner {
            config: Arc::new(SendStreamConfig {
                max_packet_len: 0,
                late_send_delay: None,
                send_window_size: 4,
            }),
            stream_id: 4,
            send_socket: Arc::new(SendPipeline::new(Arc::new(MockSendSocket::new()))),
            peer_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            self_reply_to_addr: None,
            send_buffer: Default::default(),
            work_in_progress_packet_id: PacketId::from_raw(wip_packet_id),
            work_in_progress: None,
            work_in_progress_late_send_handle: None,
        };
        for packet in send_buffer {
            inner.send_buffer.insert(PacketId::from_raw(packet), BytesMut::new());
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
                Arc::new(SendStreamConfig {
                    max_packet_len: 0,
                    late_send_delay: None,
                    send_window_size: 0,
                }),
                4,
                Arc::new(SendPipeline::new(Arc::new(MockSendSocket::default()))),
                SocketAddr::from(([127, 0, 0, 1], 0)),
                reply_to_addr,
            );

            let actual = send_stream.inner.read().await
                .packet_header_len();

            let expected = PacketHeader::serialized_len_for_stream_header(reply_to_addr);
            assert_eq!(actual, expected);
        });
    }

    #[rstest]
    #[case::empty(vec![], 0, vec![0, 22, 0,4, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0])]
    #[case::just_wip(vec![], 6, vec![0, 22, 0,4, 0,0,0,0,0,0,0,6, 0,0,0,0,0,0,0,6])]
    #[case::buf(vec![3,4], 5, vec![0, 22, 0,4, 0,0,0,0,0,0,0,5, 0,0,0,0,0,0,0,3])]
    fn test_send_send_sync(#[case] send_buffer: Vec<u64>, #[case] wip_packet_id: u64, #[case] expected_buf: Vec<u8>) {
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

            let mut inner = SendStreamInner {
                config: Arc::new(SendStreamConfig {
                    max_packet_len: 0,
                    late_send_delay: None,
                    send_window_size: 4,
                }),
                stream_id: 4,
                send_socket: Arc::new(SendPipeline::new(Arc::new(send_socket))),
                peer_addr: SocketAddr::from(([1,2,3,4], 9)),
                self_reply_to_addr: None,
                send_buffer: Default::default(),
                work_in_progress_packet_id: PacketId::from_raw(wip_packet_id),
                work_in_progress: None,
                work_in_progress_late_send_handle: None,
            };
            for packet in send_buffer {
                inner.send_buffer.insert(PacketId::from_raw(packet), BytesMut::new());
            }

            inner.send_send_sync().await;
        });
    }
}