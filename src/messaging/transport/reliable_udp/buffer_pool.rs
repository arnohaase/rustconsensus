use std::sync::Arc;
use bytes::BytesMut;
use tokio::sync::RwLock;

pub struct BufferPool {
    buffers: Arc<RwLock<Vec<BytesMut>>>,
    max_size: usize,
    buf_capacity: usize,
}

impl BufferPool {
    pub fn new(max_size: usize, buf_capacity: usize) -> BufferPool {
        BufferPool {
            buffers: Arc::new(RwLock::new(Vec::with_capacity(max_size))),
            max_size,
            buf_capacity,
        }
    }

    pub async fn get_buffer(&self) -> BytesMut {
        self.buffers.write().await
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buf_capacity))
    }

    pub async fn return_buffer(&self, buffer: BytesMut) {
        let mut buffers = self.buffers.write().await;
        if buffers.len() < self.max_size {
            let mut buffer = buffer;
            buffer.clear();
            buffers.push(buffer);
        }
    }
}

