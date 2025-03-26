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

    pub fn return_to_pool(&self, buffer: BytesMut) {
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

