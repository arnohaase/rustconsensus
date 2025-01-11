use std::cmp::min;
use std::collections::BTreeMap;
use crate::control_messages::{ControlMessageNak, ControlMessageRecvSync};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use bytes_varint::VarIntSupportMut;
use tokio::time;
use tracing::{debug, error, warn};
use tracing::field::debug;
use crate::packet_header::{PacketHeader, PacketKind};
use crate::send_socket::SendSocket;

pub struct SendStreamConfig {
    max_packet_len: usize, //TODO calculated from MTU, encryption wrapper, ...
    late_send_delay: Option<Duration>,
}

struct SendStreamInner {
    config: Arc<SendStreamConfig>,
    stream_id: u16,
    send_socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    self_reply_to_addr: Option<SocketAddr>,

    send_buffer: BTreeMap<u32, BytesMut>,
    /// first unsent packet, either already in 'work in progress' or next one to go there
    high_water_mark: u32,
    work_in_progress: Option<BytesMut>,
    work_in_progress_late_send_handle: Option<tokio::task::JoinHandle<()>>,
}
impl SendStreamInner {
    fn packet_header_len(&self) -> usize {
        todo!() // depends on reply-to address and stream id
    }

    fn init_wip(&mut self, first_message_offset: u16) -> &mut BytesMut {
        assert!(self.work_in_progress.is_none());

        let mut new_buffer = BytesMut::with_capacity(self.config.max_packet_len); //TODO pool
        PacketHeader::new(self.self_reply_to_addr, PacketKind::RegularSequenced {
            stream_id: self.stream_id,
            first_message_offset,
            packet_sequence_number: self.high_water_mark,
        }).ser(&mut new_buffer);

        self.work_in_progress = Some(new_buffer);
        self.work_in_progress.as_mut().unwrap()
    }

    async fn do_send_work_in_progress(&mut self) {
        let mut wip = self.work_in_progress.take()
            .expect("attempting to send uninitialized wip");

        if let Err(e) = self.send_socket.finalize_and_send_packet(self.peer_addr, &mut wip).await {
            error!("error sending packet to {:?} - will retry on NAK", self.peer_addr);
            //NB: we do *not* return here, but keep the (potentially) unsent packet in our send buffer
        }
        self.send_buffer.insert(self.high_water_mark, wip);

        if let Some(handle) = &self.work_in_progress_late_send_handle {
            handle.abort();
            self.work_in_progress_late_send_handle = None;
        }

        self.high_water_mark = self.high_water_mark.wrapping_add(1);
    }
}


pub struct SendStream {
    config: Arc<SendStreamConfig>,
    inner: Arc<RwLock<SendStreamInner>>,
}

impl SendStream {
    pub fn new() -> SendStream {
        todo!()
    }

    pub async fn peer_addr(&self) -> SocketAddr {
        self.inner.read().await.peer_addr
    }

    pub async fn on_init_message(&self) {
        todo!()
    }

    pub async fn on_recv_sync_message(&self, message: ControlMessageRecvSync) {
        todo!()
    }

    pub async fn on_nak_message(&self, message: ControlMessageNak) {
        todo!()
    }

    //TODO add some tracing here for correlation
    /// NB: This function does not return Result because all retry / recovery handling is expected
    ///      to be done here
    pub async fn send_message(&mut self, mut message: &[u8]) {
        //TODO ensure message max length - configurable upper limit?

        let mut data = self.inner.write().await;
        let data: &mut SendStreamInner = &mut data;

        let mut wip_buffer = if let Some(wip) = &mut data.work_in_progress {
            // if the message header does not fit in wip, send and re-init wip
            if wip.len() + 5 <= self.config.max_packet_len { //TODO actual varint len of message header instead of 5
                wip
            }
            else {
                data.do_send_work_in_progress().await;
                data.init_wip(0)
            }
        }
        else {
            // if there is no previous wip, initialize it
            data.init_wip(0)
        };

        // write message header
        wip_buffer.put_usize_varint(message.len());

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

            data.do_send_work_in_progress().await;

            let first_message_offset = if message.len() + data.packet_header_len() > self.config.max_packet_len {
                PacketHeader::OFFSET_MESSAGE_CONTINUES
            }
            else {
                message.len() as u16 //TODO overflow
            };
            wip_buffer = data.init_wip(first_message_offset);
        }

        if wip_buffer.len() + 5 > self.config.max_packet_len { //TODO actual varint len of message header instead of 5
            data.do_send_work_in_progress().await
        }

        // start 'late send' timer
        if data.work_in_progress.is_some() {
            if let Some(late_send_delay) = self.config.late_send_delay {
                let high_water_mark = data.high_water_mark;
                let data_arc = self.inner.clone();

                data.work_in_progress_late_send_handle = Some(tokio::spawn(async move {
                    time::sleep(late_send_delay).await;

                    let mut data = data_arc.write().await;
                    if data.high_water_mark != high_water_mark {
                        debug!("late send: packet {} already sent", high_water_mark);
                    }
                    else {
                        debug!("late send: sending packet {}", high_water_mark);
                        if data.work_in_progress.is_some() {
                            data.do_send_work_in_progress().await;
                        }
                        data.work_in_progress_late_send_handle = None;
                    }
                }));
            }
        }

        //TODO send window: discard messages with a sequence id sufficiently 'before' high water mark
    }
}
