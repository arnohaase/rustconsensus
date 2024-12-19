use std::time::Duration;
use bytes::{Buf, BytesMut};
use tracing::trace;
use crate::messaging::message_module::Message;
use crate::messaging::transport::reliable_udp::config::ReliableUdpTransportConfig;

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




pub struct SendBuffer {
    buf: BytesMut,
}
impl SendBuffer {
    pub fn new(config: &ReliableUdpTransportConfig) -> Self {
        SendBuffer {
            buf: BytesMut::with_capacity(config.max_datagram_size()),
        }
    }

    pub fn available(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    pub fn add_message_unchunked<M: Message>(&mut self, message: &M) {
        //TODO envelope

        message.ser(&mut self.buf);
        todo!()
    }
}

pub enum MessagePriority {
    Immediately,
    CollectFor(Duration),
}

pub struct MessageSender {
    /// The buffer ID at 0, or rather the offset so that `current_buf_index` points to buffer number
    ///  `buf_id_offset + current_buf_index`. The offset for `highest_acknowledged` may be the
    ///  same or one less due to the nature of the ring buffer.
    current_buf_id_offset: u64,
    /// used as a ring buffer
    buffers: Vec<SendBuffer>,
    current_buf_index: usize,
    highest_acknowledged: usize,
}
impl MessageSender {
    pub fn new(config: &ReliableUdpTransportConfig) -> Self {
        let mut buffers = Vec::with_capacity(config.buffer_cache_size);
        for _ in 0..config.buffer_cache_size {
            buffers.push(SendBuffer::new(config));
        }

        MessageSender {
            current_buf_id_offset: 0,
            buffers,
            current_buf_index: 0,
            highest_acknowledged: config.buffer_cache_size - 1, //TODO ???
        }
    }

    pub async fn add_message<M: Message>(&mut self, message: &M, priority: MessagePriority) {
        //TODO priority

        if let Some(max_len) = message.guaranteed_upper_bound_for_serialized_size() {
            let cur_buf = &mut self.buffers[self.current_buf_index];
            if cur_buf.available() <= max_len {
                cur_buf.add_message_unchunked(message);

                //TODO priority; combine messages
                self.do_send_current_frame().await;
            }
            else {
                todo!()
            }
        }
        else {
            todo!()
        }
    }

    async fn do_send_current_frame(&mut self) {
        trace!("sending frame {}", self.current_buf_id_offset + self.current_buf_index as u64);
        //TODO network send

        self.current_buf_index += 1;
        if self.current_buf_index == self.buffers.len() {
            self.current_buf_index = 0;
            self.current_buf_id_offset += self.buffers.len() as u64;
        }
    }
}

