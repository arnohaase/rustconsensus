use std::cmp::min;
use crate::messaging::envelope::Checksum;
use crate::messaging::message_module::{Message, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::anyhow;
use crc::Crc;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::error;
use crate::messaging::transport::reliable_udp::buffer_pool::BufferPool;


struct PacketHeader {
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
    const FIRE_AND_FORGET_PACKET_COUNTER: u64 = u64::MAX;

    const OFFSET_CONTINUED_FRAGMENT_SEQUENCE: u16 = 0xFFFF;

    fn new(from: NodeAddr, to: NodeAddr, packet_counter: u64) -> Self {
        PacketHeader {
            checksum: Checksum(0),
            packet_counter,
            new_message_offset: 0,
            from,
            to,
        }
    }

    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.checksum.0);
        buf.put_u64(self.packet_counter);
        self.from.ser(buf);
        buf.put_u32(self.to.unique);
    }
}

const SERIALIZED_MESSAGE_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>();

struct MessageHeader {
    message_module_id: MessageModuleId,
    message_len: u32,
}
impl MessageHeader {
    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.message_module_id.0);
        buf.put_u32(self.message_len);
    }
}


trait PacketBuffer {
    fn available_for_message(&self) -> usize;

    fn try_append_ser_message(&mut self, msg_buf: &[u8], message_module_id: MessageModuleId) -> anyhow::Result<()>;

    /// does what is necessary after message data is complete:
    ///  * calculates and patches the checksum
    ///  * (eventually) adds random padding and encrypts
    fn finalize(&mut self);
}
impl PacketBuffer for BytesMut {
    fn available_for_message(&self) -> usize {
        self.capacity().checked_sub(SERIALIZED_MESSAGE_HEADER_SIZE).unwrap_or(0)
    }

    fn try_append_ser_message(&mut self, msg_buf: &[u8], message_module_id: MessageModuleId) -> anyhow::Result<()> {
        if msg_buf.len() > self.available_for_message() {
            return Err(anyhow!("message does not fit into buffer")); //TODO error enum
        }

        let header = MessageHeader {
            message_module_id,
            message_len: msg_buf.len() as u32, //TODO overflow
        };
        header.ser(self);
        self.put_slice(msg_buf);
        Ok(())
    }

    fn finalize(&mut self) {
        let hasher = Crc::<u64>::new(&crc::CRC_64_REDIS);
        let mut digest = hasher.digest();
        digest.update(&self[size_of::<Checksum>()..]);
        let checksum = digest.finalize();

        self[0..size_of::<Checksum>()].copy_from_slice(&checksum.to_be_bytes());
    }
}


#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SendMode {
    FireAndForget,
    ReliableNoWait,
    ReliableCollect,
}


struct SendPoolState {
    packets: BTreeMap<u64, BytesMut>,
    current_packet_id: u64,
    delay_sending_handle: Option<JoinHandle<()>>,
}
impl SendPoolState {
    fn provision_new_buffer(&mut self, myself: NodeAddr, to: NodeAddr) {
        let mut buf = BytesMut::with_capacity(1492); //TODO from pool
        PacketHeader::new(myself, to, self.current_packet_id) // TODO extract to PacketBuffer
            .ser(&mut buf);

        let prev = self.packets.insert(self.current_packet_id, buf);
        assert!(prev.is_none(), "second buffer for id {}", self.current_packet_id);
    }

    fn current_packet_buf(&mut self) -> &mut BytesMut {
        self.packets.get_mut(&self.current_packet_id)
            .expect("the current buffer should be initialized")
    }
}

struct SocketSendItem(u64);

/// per peer / target address
/// NB: different trade-offs than message queue / pub/sub implementations like HazelCast or Aeron
struct SendSequence {
    buffer_pool: Arc<BufferPool>,
    myself: NodeAddr,
    to: NodeAddr,
    state: Arc<RwLock<SendPoolState>>,
    socket_send_channel: mpsc::Sender<SocketSendItem>,
    send_socket: Arc<UdpSocket>,
    message_header_len: usize,
}
impl SendSequence {
    async fn new(buffer_pool: Arc<BufferPool>, myself: NodeAddr, to: NodeAddr, send_socket: Arc<UdpSocket>) -> Self {
        let (send, recv) = mpsc::channel::<SocketSendItem>(8); //TODO constant? configurable?

        let mut state = SendPoolState {
            packets: BTreeMap::default(),
            current_packet_id: 0,
            delay_sending_handle: None,
        };
        state.provision_new_buffer(myself, to);

        let state = Arc::new(RwLock::new(state));

        tokio::spawn(Self::socket_send_loop(to, state.clone(), recv, send_socket.clone())); // terminates when the last sender goes out of scope

        SendSequence {
            buffer_pool,
            myself,
            to,
            state,
            socket_send_channel: send,
            send_socket,
            message_header_len: todo!(),
        }
    }

    async fn socket_send_loop(to: NodeAddr, state: Arc<RwLock<SendPoolState>>, mut channel: mpsc::Receiver<SocketSendItem>, send_socket: Arc<UdpSocket>) {
        loop {
            match channel.recv().await {
                None => break,
                Some(SocketSendItem(packet_id)) => {
                    let state = state.read().await;
                    if let Some(packet) = state.packets.get(&packet_id) {
                        if let Err(e) = send_socket.send_to(packet, to.socket_addr).await {
                            error!("failed to send packet: {}", e);   //TODO error handling; extract to 'SendSocket' struct?
                        }
                    }
                }
            }
        }
    }

    async fn send_message<M: Message>(&mut self, send_mode: SendMode, msg: &M) {
        let mut buf = BytesMut::with_capacity(1024);
        msg.ser(&mut buf);
        self.send_message_buffer(send_mode, &buf, msg.module_id()).await;
    }

    async fn do_send_fire_and_forget(&self, send_mode: SendMode, msg_buf: &[u8], message_module_id: MessageModuleId) {
        let mut buf = self.buffer_pool.get_buffer().await;

        PacketHeader::new(self.myself, self.to, PacketHeader::FIRE_AND_FORGET_PACKET_COUNTER)
            .ser(&mut buf);

        buf.try_append_ser_message(msg_buf, message_module_id)
            .expect("fire & forget messages must fit into a single packet"); //TODO panic or return error?

        buf.finalize();
        if let Err(e) = self.send_socket.send_to(&buf, self.to.socket_addr).await {
            error!("failed to send message: {}", e); //TODO error handling
        }

        self.buffer_pool.return_buffer(buf).await;
    }


    async fn send_message_buffer(&self, send_mode: SendMode, msg_buf: &[u8], message_module_id: MessageModuleId) {
        if send_mode == SendMode::FireAndForget {
            return self.do_send_fire_and_forget(SendMode::FireAndForget, msg_buf, message_module_id);
        }

        //TODO return error if too many un-acked packets, oldest un-acked packet is too old

        let mut state = self.state.write().await;

        {
            let current_buf = state.current_packet_buf();
            assert!(current_buf.capacity() >= SERIALIZED_MESSAGE_HEADER_SIZE, "a message header fits into the current packet, otherwise it would have been flushed with the previous message");
            MessageHeader {
                message_module_id,
                message_len: msg_buf.len() as u32, //TODO overflow
            }.ser(current_buf);
        }

        let mut msg_buf = msg_buf;
        while !msg_buf.is_empty() {
            let num_to_write = min(self.capacity(), msg_buf.len());
            self.put_slice(&msg_buf[..num_to_write]);
            msg_buf = &msg_buf[num_to_write..];

            self.send_current_packet_eventually(send_mode, &mut state).await;

            if msg_buf.len() > state.current_packet_buf().available_for_message() {
                //TODO patch FFFF to 'start of first message'
            }
            else if msg_buf.len() > 0 {
                //TODO patch msg_buf.len to 'start of first message'
            }
        }
    }

    async fn send_current_packet_eventually(&self, send_mode: SendMode, state: &mut SendPoolState) {
        if send_mode == SendMode::ReliableNoWait || state.current_packet_buf().capacity() < SERIALIZED_MESSAGE_HEADER_SIZE {
            self.complete_current_packet().await;
            return;
        }

        let cloned_state = self.state.clone();
        let cloned_packet_id = state.current_packet_id;
        let cloned_sender = self.socket_send_channel.clone();

        if state.delay_sending_handle.is_none() {
            let handle = tokio::spawn(async move {
                time::sleep(Duration::from_millis(1)).await; //TODO configurable

                let state = cloned_state.write().await;
                if state.current_packet_id == cloned_packet_id {
                    let _ = cloned_sender.send(SocketSendItem(cloned_packet_id)).await; // receiver loops endlessly
                }
            });
            state.delay_sending_handle = Some(handle);
        }
    }

    async fn ack_single(&mut self, packet_id: u64) {
        let buf = self.state.write().await
            .packets.remove(&packet_id);
        if let Some(buf) = buf {
            self.buffer_pool.return_buffer(buf).await;
        }
    }

    async fn ack_up_to(&mut self, up_to_packet_id: u64) {
        let ack_ids = {
            let state = self.state.write().await;
            state.packets.range(..=up_to_packet_id)
                .map(|(&id,_)| id)
                .collect::<Vec<_>>()
        };
        for id in ack_ids {
            self.ack_single(id).await;
        }
    }

    async fn complete_current_packet(&self) {
        let mut state = self.state.write().await;

        let current_buf = state.current_packet_buf();
        current_buf.finalize();

        let _ = self.socket_send_channel.send(SocketSendItem(state.current_packet_id)).await;

        state.current_packet_id += 1;
        state.provision_new_buffer(self.myself, self.to);

        if let Some(handle) = state.delay_sending_handle.take() {
            handle.abort();
        }
    }

    async fn resend_packet(&mut self, packet_id: u64) {
        let _ = self.socket_send_channel.send(SocketSendItem(packet_id)).await;
    }
}
