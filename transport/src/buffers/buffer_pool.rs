use std::sync::atomic::{AtomicU64, Ordering};
use bytes::{BufMut, BytesMut};
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};
use crate::buffers::fixed_buffer::{FixedBuf, FixedBuffer};
use crate::buffers::encryption::RudpEncryption;
use crate::packet_header::PacketHeader;

pub struct SendBufferPool {
    buf_size: usize,
    buffers: Mutex<Vec<FixedBuf>>,
    encryption: Arc<dyn RudpEncryption>,
}

impl SendBufferPool {
    pub fn new(buf_size: usize, max_pool_size: usize, encryption: Arc<dyn RudpEncryption>) -> Self {
        SendBufferPool {
            buf_size,
            buffers: Mutex::new(Vec::with_capacity(max_pool_size)),
            encryption
        }
    }

    pub fn get_envelope_prefix_len(&self) -> usize {
        self.encryption.encryption_overhead()
    }

    pub fn get_from_pool(&self) -> FixedBuf {
        let mut result = self._get_from_pool();
        self.encryption.init_buffer(&mut result);
        result
    }

    fn _get_from_pool(&self) -> FixedBuf {
        {
            let mut buffers = self.buffers.lock().unwrap();
            if let Some(buffer) = buffers.pop() {
                trace!("returning buffer from pool");
                return buffer;
            }
        }

        debug!("no buffer in pool: creating new buffer");
        FixedBuf::new(self.buf_size)
    }

    pub fn return_to_pool(&self, mut buffer: FixedBuf) {
        assert_eq!(buffer.capacity(), self.buf_size,
                   "returned buffer does not have the regular capacity of {} bytes, maybe a packet exceeding configured packet size was sent"
                   , self.buf_size);

        buffer.clear();

        let mut buffers = self.buffers.lock().unwrap();
        if buffers.capacity() > buffers.len() {
            trace!("returning buffer to pool");
            buffers.push(buffer);
        }
        else {
            debug!("pool is full: discarding returned buffer");
        }
    }
}

#[cfg(test)]
mod tests {
    use aead::Buffer;
    use bytes::BufMut;
    use crate::buffers::encryption::NoEncryption;
    use crate::message_header::MessageHeader;
    use super::*;

    #[test]
    fn test_clear() {
        let mut pool = SendBufferPool::new(10, 10, Arc::new(NoEncryption{}));

        let mut buf = FixedBuf::new(10);
        buf.put_u8(1);
        buf.put_u8(2);

        pool.return_to_pool(buf);

        assert_eq!(pool.get_from_pool().as_ref(), vec![PacketHeader::PROTOCOL_VERSION_1]);
    }
}