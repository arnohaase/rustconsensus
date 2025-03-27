use bytes::BytesMut;
use std::sync::Mutex;
use tracing::{debug, trace};

pub struct BufferPool {
    buf_size: usize,
    buffers: Mutex<Vec<BytesMut>>,
}

impl BufferPool {
    pub fn new(buf_size: usize, max_pool_size: usize) -> Self {
        BufferPool {
            buf_size,
            buffers: Mutex::new(Vec::with_capacity(max_pool_size)),
        }
    }

    pub fn get_from_pool(&self) -> BytesMut {
        {
            let mut buffers = self.buffers.lock().unwrap();
            if let Some(buffer) = buffers.pop() {
                trace!("returning buffer from pool");
                return buffer;
            }
        }

        debug!("no buffer in pool: creating new buffer");
        BytesMut::with_capacity(self.buf_size)
    }

    pub fn return_to_pool(&self, mut buffer: BytesMut) {
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
    use bytes::BufMut;
    use super::*;

    #[test]
    fn test_clear() {
        let mut pool = BufferPool::new(10, 10);

        let mut buf = BytesMut::with_capacity(10);
        buf.put_u8(1);

        pool.return_to_pool(buf);

        assert!(pool.get_from_pool().is_empty());
    }
}