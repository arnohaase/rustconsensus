use crate::messaging::envelope::Checksum;
use crate::messaging::message_module::{Message, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use bytes::{BufMut, BytesMut};
use std::collections::BTreeMap;
use anyhow::anyhow;
use crc::Crc;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::task::JoinHandle;
/*

* separate kinds of frames for control messages and regular messages:
    * control messages are sent immediately, fire-and-forget, each message in its own frame
        * heartbeat as control messages as well
    * regular messages are sent in regular frames
        * consecutive numbers for frames -> receiver notices when frames are missing
            * NAK after some grace period, and 'status' messages
            * optional back pressure -> ?!?!
        * fragmentation of large messages to MTU-sized buffers
            * serialize small messages directly into the send buffers, zero-copy
    * receive buffers from a global pool
        * assemble parts of a fragmented message into a pre-allocated buffer until all parts are present
* Frame headers, message headers

* join protocol?

 */


struct PacketHeader {
    checksum: Checksum,  // covers everything after the checksum field, including the rest of the packet header
    packet_counter: u64, // with well-known value for 'fire and forget'
    from: NodeAddr,
    to: NodeAddr,
}
impl PacketHeader {
    const FIRE_AND_FORGET_PACKET_COUNTER: u64 = u64::MAX;

    fn new(from: NodeAddr, to: NodeAddr, packet_counter: u64) -> Self {
        PacketHeader {
            checksum: Checksum(0),
            packet_counter,
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

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
enum MessageFlags {
    MIDDLE = 0,
    START = 1,
    END = 2,
    COMPLETE = 3,
}

const SERIALIZED_MESSAGE_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>();

struct MessageHeader {
    message_module_id: MessageModuleId,
    message_len: u32,
    flags: MessageFlags,
}
impl MessageHeader {
    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u64(self.message_module_id.0);
        buf.put_u8(self.flags.into());
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
            flags: MessageFlags::COMPLETE,
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


#[derive(Debug, Clone, Eq, PartialEq)]
enum SendMode {
    FireAndForget,
    ReliableImmediately,
    ReliableCollect,
}


/// per peer / target address
/// NB: different trade-offs than message queue / pub/sub implementations like HazelCast or Aeron
struct SendPool {
    myself: NodeAddr,
    to: NodeAddr,
    packets: BTreeMap<u64, BytesMut>,
    current_packet_id: u64,
    delay_sending_handle: Option<JoinHandle<()>>,
}
impl SendPool {
    const MIN_AVAILABLE_SPACE_FOR_DELAYING: usize = 40; //TODO configurable

    fn new(myself: NodeAddr, to: NodeAddr) -> Self {
        let mut pool = SendPool {
            myself,
            to,
            packets: BTreeMap::default(),
            current_packet_id: 0,
            delay_sending_handle: None,
        };
        pool.provision_new_buffer();
        pool
    }

    fn provision_new_buffer(&mut self) {
        let mut buf = BytesMut::with_capacity(1492); //TODO from pool
        PacketHeader::new(self.myself, self.to, self.current_packet_id) // TODO extract to PacketBuffer
            .ser(&mut buf);

        let prev = self.packets.insert(self.current_packet_id, buf);
        assert!(prev.is_none(), "second buffer for id {}", self.current_packet_id);
    }

    async fn send_message<M: Message>(&mut self, send_mode: SendMode, msg: &M) {
        let mut buf = BytesMut::with_capacity(1024);
        msg.ser(&mut buf);
        self.send_message_buffer(send_mode, &buf, msg.module_id()).await;
    }

    async fn send_message_buffer(&mut self, send_mode: SendMode, msg_buf: &[u8], message_module_id: MessageModuleId) {
        if send_mode == SendMode::FireAndForget {
            let mut buf = BytesMut::with_capacity(1492); //TODO from pool
            PacketHeader::new(self.myself, self.to, PacketHeader::FIRE_AND_FORGET_PACKET_COUNTER)
                .ser(&mut buf);

            buf.try_append_ser_message(msg_buf, message_module_id)
                .expect("fire & forget messages must fit into a single packet");

            buf.finalize();
            Self::_send_packet(&mut buf).await;
            //TODO return buf to pool
        }

        //TODO return error if too many un-acked packets, oldest un-acked packet is too old

        {
            let current_buf = self.packets.get_mut(&self.current_packet_id)
                .expect("the current buffer should be initialized");

            if current_buf.try_append_ser_message(&msg_buf, message_module_id).is_ok() {
                if send_mode == SendMode::ReliableImmediately || current_buf.available_for_message() < Self::MIN_AVAILABLE_SPACE_FOR_DELAYING {
                    self.complete_current_packet().await;
                    return;
                }

                tokio::spawn(async move { todo!() });

                //TODO send after timeout
                return;
            }
        }

        self.complete_current_packet().await;

        {
            let current_buf = self.packets.get_mut(&self.current_packet_id)
                .expect("the current buffer should be initialized");

            // try again if the message fits into a fresh, empty packet
            if current_buf.try_append_ser_message(&msg_buf, message_module_id).is_ok() {
                if send_mode == SendMode::ReliableImmediately || current_buf.available_for_message() < Self::MIN_AVAILABLE_SPACE_FOR_DELAYING {
                    self.complete_current_packet().await;
                    return;
                }
                //TODO send after timeout
                return;
            }
        }

        todo!("split message across packets");
    }

    fn ack_single(&mut self, packet_id: u64) {
        self.packets.remove(&packet_id); //TODO return to pool
    }

    fn ack_up_to(&mut self, up_to_packet_id: u64) {
        self.packets.retain(|&id, _| id > up_to_packet_id); //TODO return to pool
    }

    async fn complete_current_packet(&mut self) {
        let current_buf = self.packets.get_mut(&self.current_packet_id)
            .expect("the current buffer should be initialized");

        current_buf.finalize();

        Self::_send_packet(current_buf).await;

        self.current_packet_id += 1;
        self.provision_new_buffer();

        if let Some(handle) = self.delay_sending_handle.take() {
            handle.abort();
        }
    }

    async fn resend_packet(&mut self, packet_id: u64) {
        if let Some(buf) = self.packets.get_mut(&packet_id) {
            Self::_send_packet(buf).await;
        }
        //TODO else, logging
    }

    async fn _send_packet(packet_buf: &[u8]) {
        todo!()
    }
}





