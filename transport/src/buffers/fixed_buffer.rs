//! This is an implementation of fixed-length buffers for reuse. Their main purpose is to
//!  minimize copying and allow reuse in the scope of RUDP.
//!
//! Their salient points are:
//!
//! * backed by a fixed-length, pre-allocated buffer
//! * limited-lifetime 'views' backed by a slice of the outer buffer
//! * implement `BufMut` to fit into the `bytes` ecosystem
//! * implement `aead::Buffer` to support in-place AES encoding
//!
//! [FixedBuffer] is the data structure for use by the application, the rest of this file is
//!  supporting infrastructure.
//!

use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use bytes::buf::UninitSlice;
use bytes::BufMut;

/// A type alias for the most widely used kind of fixed buffer
pub type FixedBuf = FixedBuffer<VecFixed>;


pub struct VecFixed {
    raw: Vec<u8>,
}
impl FixedBufferInternal for VecFixed {
    fn raw_buf(&self) -> &[u8] {
        self.raw.as_slice()
    }

    fn raw_buf_mut(&mut self) -> &mut [u8] {
        self.raw.as_mut_slice()
    }
}

pub struct SliceFixed<'a> {
    raw: &'a mut [u8],
}
impl <'a> FixedBufferInternal for SliceFixed<'a> {
    fn raw_buf(&self) -> &[u8] {
        &self.raw
    }

    fn raw_buf_mut(&mut self) -> &mut [u8] {
        &mut self.raw
    }
}


pub trait FixedBufferInternal {
    fn raw_buf(&self) -> &[u8];
    fn raw_buf_mut(&mut self) -> &mut [u8];
}


/// This is a fixed-length, pre-allocated buffer that allows partial in-place encryption.
#[derive(Eq)]
pub struct FixedBuffer<T: FixedBufferInternal> {
    internal: T,
    /// the offset up to which the buffer contains data
    len: usize
}

impl FixedBuffer<VecFixed> {
    /// create a new FixedBuffer instance with the given buffer capacity
    pub fn new(capacity: usize) -> FixedBuffer<VecFixed> {
        FixedBuffer {
            // in this particular use case, there is no real benefit in lazily initializing the
            //  buffer since buffers are reused aggressively, and we trade the overhead of
            //  initial initialization for simplicity
            internal: VecFixed { raw: vec![0; capacity] },
            len: 0,
        }
    }
}

impl <T: FixedBufferInternal> FixedBuffer<T> {
    /// create a limited-lifetime fixed buffer backed by a slice of self's buffer
    pub fn create_view(&mut self, start_offs: usize) -> FixedBuffer<SliceFixed> {
        FixedBuffer {
            internal: SliceFixed { raw: &mut self.internal.raw_buf_mut()[start_offs..] },
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.internal.raw_buf().len()
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[cfg(test)]
    pub fn from_slice(data: &[u8]) -> FixedBuf {
        let mut result = FixedBuf::new(data.len());
        result.put_slice(data);
        result
    }
}

impl<T: FixedBufferInternal> PartialEq for FixedBuffer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<T: FixedBufferInternal> Debug for FixedBuffer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<T: FixedBufferInternal> Borrow<[u8]> for FixedBuffer<T> {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<T: FixedBufferInternal> AsRef<[u8]> for FixedBuffer<T> {
    fn as_ref(&self) -> &[u8] {
        &self.internal.raw_buf()[..self.len]
    }
}
impl<T: FixedBufferInternal> AsMut<[u8]> for FixedBuffer<T> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.internal.raw_buf_mut()[..self.len]
    }
}

unsafe impl<T: FixedBufferInternal> bytes::BufMut for FixedBuffer<T> {
    fn remaining_mut(&self) -> usize {
        self.internal.raw_buf().len() - self.len
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(self.len + cnt <= self.capacity());
        self.len += cnt;
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        UninitSlice::new(&mut self.internal.raw_buf_mut()[self.len..])
    }
}

impl <T: FixedBufferInternal> aead::Buffer for FixedBuffer<T> {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        &mut self.internal.raw_buf_mut()[self.len..].copy_from_slice(other);
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }
}
