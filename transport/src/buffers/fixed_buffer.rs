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

use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use bytes::buf::UninitSlice;

/// A fixed-length dynamically allocated buffer
#[derive(Eq)]
pub struct FixedBuf {
    buf: Vec<u8>,
    len: usize,
}
impl FixedBuf {
    /// create a new FixedBuffer instance with the given buffer capacity
    pub fn new(capacity: usize) -> FixedBuf {
        FixedBuf {
            // in this particular use case, there is no real benefit in lazily initializing the
            //  buffer since buffers are reused aggressively, and we trade the overhead of
            //  initial initialization for simplicity
            buf: vec![0; capacity],
            len: 0,
        }
    }

    ///create a limited-lifetime fixed buffer backed by a slice of self's buffer
    pub fn slice(&mut self, start_offs: usize) -> SliceBuf {
        SliceBuf {
            inner: self,
            start_offset: start_offs,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// make the entire underlying buffer available through as_ref() etc.
    pub fn maximize_len(&mut self) {
        self.len = self.capacity();
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// This is a convenience function for test code. It derives the buffer's capacity from the
    ///  slice used for initialization, which is a shortcut not intended for production usage.
    #[cfg(test)]
    pub fn from_slice(len: usize, data: &[u8]) -> FixedBuf {
        let mut result = FixedBuf::new(len);
        bytes::BufMut::put_slice(&mut result, data);
        result
    }
}

impl PartialEq for FixedBuf {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Debug for FixedBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Borrow<[u8]> for FixedBuf {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for FixedBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}
impl AsMut<[u8]> for FixedBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..self.len]
    }
}

unsafe impl bytes::BufMut for FixedBuf {
    fn remaining_mut(&self) -> usize {
        self.buf.len() - self.len
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(self.len + cnt <= self.capacity());
        self.len += cnt;
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        UninitSlice::new(&mut self.buf[self.len..])
    }
}

impl aead::Buffer for FixedBuf {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        self.buf[self.len..self.len+other.len()].copy_from_slice(other);
        self.len += other.len();
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }
}


#[derive(Eq)]
pub struct SliceBuf<'a> {
    inner: &'a mut FixedBuf,
    start_offset: usize,
}
impl<'a> SliceBuf<'a> {
    pub fn len(&self) -> usize {
        self.inner.len - self.start_offset
    }
    pub fn maximize_len(&mut self) {
        self.inner.maximize_len();
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity() - self.start_offset
    }

    pub fn clear(&mut self) {
        self.inner.len = self.start_offset;
    }
}

impl <'a> PartialEq for SliceBuf<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl <'a> Debug for SliceBuf<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl <'a> Borrow<[u8]> for SliceBuf<'a> {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl <'a> AsRef<[u8]> for SliceBuf<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.inner.as_ref()[self.start_offset..]
    }
}
impl <'a> AsMut<[u8]> for SliceBuf<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.inner.as_mut()[self.start_offset..]
    }
}

unsafe impl <'a> bytes::BufMut for SliceBuf<'a> {
    fn remaining_mut(&self) -> usize {
        self.inner.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.inner.advance_mut(cnt);
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.inner.chunk_mut()
    }
}

impl <'a> aead::Buffer for SliceBuf<'a> {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        self.inner.extend_from_slice(other)
    }

    fn truncate(&mut self, len: usize) {
        self.inner.truncate(self.start_offset + len);
    }
}

#[derive(Eq)]
pub struct ArrayBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> ArrayBuf<N> {
    pub fn new() -> ArrayBuf<N> {
        ArrayBuf {
            buf: [0;N],
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn maximize_len(&mut self) {
        self.len = self.capacity();
    }

    pub fn capacity(&self) -> usize {
        N
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl <const N: usize> PartialEq for ArrayBuf<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl <const N: usize> Debug for ArrayBuf<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl <const N: usize> Borrow<[u8]> for ArrayBuf<N> {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl <const N: usize> AsRef<[u8]> for ArrayBuf<N> {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}
impl <const N: usize> AsMut<[u8]> for ArrayBuf<N> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..self.len]
    }
}

unsafe impl <const N: usize> bytes::BufMut for ArrayBuf<N> {
    fn remaining_mut(&self) -> usize {
        self.capacity() - self.len
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.len += cnt;
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        UninitSlice::new(&mut self.buf[self.len..])
    }
}

impl <const N: usize> aead::Buffer for ArrayBuf<N> {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        self.buf[self.len..self.len+other.len()].copy_from_slice(other);
        self.len += other.len();
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        self.len = len;
    }
}


#[cfg(test)]
mod tests {
    use aead::Buffer;
    use bytes::BufMut;
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case::empty(new_fixed_buf(100, b""), 0)]
    #[case::simple(new_fixed_buf(100, b"abc"), 3)]
    fn test_len(#[case] buf: FixedBuf, #[case] expected: usize) {
        assert_eq!(buf.len(), expected);
    }

    #[rstest]
    #[case::empty(new_fixed_buf(100, b""), 0, 0)]
    #[case::simple(new_fixed_buf(100, b"abc"), 0, 3)]
    #[case::simple_offs(new_fixed_buf(100, b"abc"), 1, 2)]
    #[case::simple_max_offs(new_fixed_buf(100, b"abc"), 3, 0)]
    fn test_len_slice(#[case] mut buf: FixedBuf, #[case] offset: usize, #[case] expected: usize) {
        assert_eq!(buf.slice(offset).len(), expected);
    }

    #[rstest]
    #[case::empty(new_array_buf(b""), 0)]
    #[case::simple(new_array_buf(b"abc"), 3)]
    fn test_len_array(#[case] buf: ArrayBuf<100>, #[case] expected: usize) {
        assert_eq!(buf.len(), expected);
    }

    #[rstest]
    #[case::empty(new_fixed_buf(3, b""), b"\0\0\0")]
    #[case::data(new_fixed_buf(4, b"abc"), b"abc\0")]
    #[case::full(new_fixed_buf(5, b"abcde"), b"abcde")]
    fn test_maximize_len(#[case] mut buf: FixedBuf, #[case] expected: &[u8]) {
        buf.maximize_len();
        assert_eq!(buf.as_ref(), expected);
    }

    #[rstest]
    #[case::empty(new_array_buf(b""), b"\0\0\0")]
    #[case::data(new_array_buf(b"ab"), b"ab\0")]
    #[case::full(new_array_buf(b"abc"), b"abc")]
    fn test_maximize_len_array(#[case] mut buf: ArrayBuf<3>, #[case] expected: &[u8]) {
        buf.maximize_len();
        assert_eq!(buf.as_ref(), expected);
    }

    #[rstest]
    #[case::empty_100(new_fixed_buf(100, b""), 100)]
    #[case::empty_200(new_fixed_buf(200, b""), 200)]
    #[case::data_100(new_fixed_buf(100, b"abc"), 100)]
    #[case::data_200(new_fixed_buf(200, b"abc"), 200)]
    #[case::full(new_fixed_buf(5, b"abcde"), 5)]
    fn test_capacity(#[case] buf: FixedBuf, #[case] expected: usize) {
        assert_eq!(buf.capacity(), expected);
    }

    #[rstest]
    #[case::empty_100(new_fixed_buf(100, b""), 0, 100)]
    #[case::empty_200(new_fixed_buf(200, b""), 0, 200)]
    #[case::data_100(new_fixed_buf(100, b"abc"), 0, 100)]
    #[case::data_100_1(new_fixed_buf(100, b"abc"), 1, 99)]
    #[case::data_100_2(new_fixed_buf(100, b"abc"), 2, 98)]
    #[case::data_100_3(new_fixed_buf(100, b"abc"), 3, 97)]
    #[case::data_200(new_fixed_buf(200, b"abc"), 0, 200)]
    #[case::data_200_1(new_fixed_buf(200, b"abc"), 1, 199)]
    #[case::full(new_fixed_buf(5, b"abcde"), 0, 5)]
    #[case::full_1(new_fixed_buf(5, b"abcde"), 1, 4)]
    #[case::full_5(new_fixed_buf(5, b"abcde"), 5, 0)]
    fn test_capacity_slice(#[case] mut buf: FixedBuf, #[case] offset: usize, #[case] expected: usize) {
        assert_eq!(buf.slice(offset).capacity(), expected);
    }

    #[rstest]
    #[case::empty(new_array_buf(b""))]
    #[case::data(new_array_buf(b"abc"))]
    #[case::full(new_array_buf(b"abcdef"))]
    fn test_capacity_array(#[case] buf: ArrayBuf<6>) {
        assert_eq!(buf.capacity(), 6);
    }

    #[rstest]
    #[case::empty(new_fixed_buf(100, b""))]
    #[case::data(new_fixed_buf(200, b"123"))]
    #[case::full(new_fixed_buf(5, b"12345"))]
    fn test_clear(#[case] mut buf: FixedBuf) {
        let capacity = buf.capacity();

        buf.clear();

        assert_eq!(0, buf.len());
        assert_eq!(b"", buf.as_ref());
        assert_eq!(capacity, buf.capacity());
    }

    #[rstest]
    #[case::empty(new_fixed_buf(100, b""), 0, b"")]
    #[case::data(new_fixed_buf(200, b"123"), 0, b"")]
    #[case::data_1(new_fixed_buf(200, b"123"), 1, b"1")]
    #[case::data_3(new_fixed_buf(200, b"123"), 3, b"123")]
    #[case::full(new_fixed_buf(5, b"12345"), 0, b"")]
    #[case::full_2(new_fixed_buf(5, b"12345"), 2, b"12")]
    #[case::full_5(new_fixed_buf(5, b"12345"), 5, b"12345")]
    fn test_clear_slice(#[case] mut buf: FixedBuf, #[case] offset: usize, #[case] expected: &[u8]) {
        let capacity = buf.capacity();

        {
            let mut buf = buf.slice(offset);
            let capacity = buf.capacity();

            buf.clear();

            assert_eq!(0, buf.len());
            assert_eq!(buf.as_ref(), b"");
            assert_eq!(capacity, buf.capacity());
        }

        assert_eq!(buf.capacity(), capacity);
        assert_eq!(buf.as_ref(), expected);
    }

    #[rstest]
    #[case::empty(new_array_buf(b""))]
    #[case::data(new_array_buf(b"123"))]
    #[case::full(new_array_buf(b"12345"))]
    fn test_clear_array(#[case] mut buf: ArrayBuf<5>) {
        let capacity = buf.capacity();

        buf.clear();

        assert_eq!(0, buf.len());
        assert_eq!(b"", buf.as_ref());
        assert_eq!(capacity, buf.capacity());
    }

    #[test]
    fn test_from_slice() {
        let buf = FixedBuf::from_slice(20, b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_ref(), b"hello");
        assert_eq!(buf.capacity(), 20);
    }

    #[rstest]
    #[case::empty                    (new_fixed_buf(100, b""),   new_fixed_buf(100, b""),   true)]
    #[case::empty_different_capacity (new_fixed_buf(100, b""),   new_fixed_buf(200, b""),   true)]
    #[case::simple                   (new_fixed_buf(100, b"hi"), new_fixed_buf(200, b"hi"), true)]
    #[case::simple_different_capacity(new_fixed_buf(100, b"hi"), new_fixed_buf(200, b"hi"), true)]
    #[case::different                (new_fixed_buf(100, b"hi"), new_fixed_buf(100, b"yo"), false)]
    #[case::prefix                   (new_fixed_buf(100, b"h"),  new_fixed_buf(100, b"hi"), false)]
    #[case::empty_non_empty          (new_fixed_buf(100, b""),   new_fixed_buf(100, b"hi"), false)]
    fn test_eq(#[case] buf1: FixedBuf, #[case] buf2: FixedBuf, #[case] expected: bool) {
        assert_eq!(buf1.eq(&buf2), expected);
        assert_eq!(buf2.eq(&buf1), expected);
    }

    fn new_fixed_buf(capacity: usize, content: &[u8]) -> FixedBuf {
        let mut result = FixedBuf::new(capacity);
        result.put_slice(content);
        result
    }

    fn new_array_buf<const N: usize>(content: &[u8]) -> ArrayBuf<N> {
        let mut result = ArrayBuf::<N>::new();
        result.put_slice(content);
        result
    }

    #[rstest]
    #[case::empty                    (new_fixed_buf(100, b""),    0, new_fixed_buf(100, b""),    0, true)]
    #[case::empty_different_capacity (new_fixed_buf(100, b""),    0, new_fixed_buf(200, b""),    0, true)]
    #[case::simple                   (new_fixed_buf(100, b"hi"),  0, new_fixed_buf(200, b"hi"),  0, true)]
    #[case::simple_different_capacity(new_fixed_buf(100, b"hi"),  0, new_fixed_buf(200, b"hi"),  0, true)]
    #[case::different                (new_fixed_buf(100, b"hi"),  0, new_fixed_buf(100, b"yo"),  0, false)]
    #[case::prefix                   (new_fixed_buf(100, b"h"),   0, new_fixed_buf(100, b"hi"),  0, false)]
    #[case::empty_non_empty          (new_fixed_buf(100, b""),    0, new_fixed_buf(100, b"hi"),  0, false)]
    #[case::equal_with_offset        (new_fixed_buf(100, b"hi"),  1, new_fixed_buf(100, b"hi"),  1, true)]
    #[case::equal_slice              (new_fixed_buf(100, b"ax"),  1, new_fixed_buf(100, b"bx"),  1, true)]
    #[case::equal_slice_diff_offsets (new_fixed_buf(100, b"abx"), 2, new_fixed_buf(100, b"cx"),  1, true)]
    #[case::equal_buf_diff_offsets   (new_fixed_buf(100, b"abc"), 1, new_fixed_buf(100, b"abc"), 2, false)]
    fn test_eq_slice(#[case] mut buf1: FixedBuf, #[case] offset_1: usize, #[case] mut buf2: FixedBuf, #[case] offset_2: usize, #[case] expected: bool) {
        let buf1 = buf1.slice(offset_1);
        let buf2 = buf2.slice(offset_2);

        assert_eq!(buf1.eq(&buf2), expected);
        assert_eq!(buf2.eq(&buf1), expected);
    }

    #[rstest]
    #[case::empty          (new_array_buf(b""),   new_array_buf(b""),   true)]
    #[case::simple         (new_array_buf(b"hi"), new_array_buf(b"hi"), true)]
    #[case::different      (new_array_buf(b"hi"), new_array_buf(b"yo"), false)]
    #[case::prefix         (new_array_buf(b"h"),  new_array_buf(b"hi"), false)]
    #[case::empty_non_empty(new_array_buf(b""),   new_array_buf(b"hi"), false)]
    fn test_eq_array(#[case] buf1: ArrayBuf<100>, #[case] buf2: ArrayBuf<100>, #[case] expected: bool) {
        assert_eq!(buf1.eq(&buf2), expected);
        assert_eq!(buf2.eq(&buf1), expected);
    }

    #[rstest]
    #[case::empty(new_fixed_buf(20, b""), b"")]
    #[case::data(new_fixed_buf(45, b"abc"), b"abc")]
    #[case::full(new_fixed_buf(5, b"abcde"), b"abcde")]
    fn test_borrow(#[case] mut buf: FixedBuf, #[case] expected: &[u8]) {
        let borrowed: &[u8] = buf.borrow();
        assert_eq!(borrowed, expected);
        assert_eq!(buf.as_ref(), expected);
        assert_eq!(buf.as_mut(), expected);
    }

    #[rstest]
    #[case::empty(new_fixed_buf(20, b""), 0, b"")]
    #[case::data(new_fixed_buf(45, b"abc"), 0, b"abc")]
    #[case::data_1(new_fixed_buf(45, b"abc"), 1, b"bc")]
    #[case::data_3(new_fixed_buf(45, b"abc"), 3, b"")]
    #[case::full(new_fixed_buf(5, b"abcde"), 0, b"abcde")]
    #[case::full_1(new_fixed_buf(5, b"abcde"), 1, b"bcde")]
    #[case::full_5(new_fixed_buf(5, b"abcde"), 5, b"")]
    fn test_borrow_slice(#[case] mut buf: FixedBuf, #[case] offset: usize, #[case] expected: &[u8]) {
        let mut slice = buf.slice(offset);
        let borrowed: &[u8] = slice.borrow();
        assert_eq!(borrowed, expected);
        assert_eq!(slice.as_ref(), expected);
        assert_eq!(slice.as_mut(), expected);
    }

    #[rstest]
    #[case::empty(new_array_buf(b""), b"")]
    #[case::data(new_array_buf(b"abc"), b"abc")]
    #[case::full(new_array_buf(b"abcde"), b"abcde")]
    fn test_borrow_array(#[case] mut buf: ArrayBuf<5>, #[case] expected: &[u8]) {
        let borrowed: &[u8] = buf.borrow();
        assert_eq!(borrowed, expected);
        assert_eq!(buf.as_ref(), expected);
        assert_eq!(buf.as_mut(), expected);
    }

    #[rstest]
    #[case::data(new_fixed_buf(20, b"abc"), b"Abc")]
    #[case::full(new_fixed_buf(5, b"qrstu"), b"Arstu")]
    fn test_as_mut_modification(#[case] mut buf: FixedBuf, #[case] expected: &[u8]) {
        buf.as_mut()[0] = 65;
        assert_eq!(buf.as_ref(), expected);
    }

    #[rstest]
    #[case::data(new_fixed_buf(20, b"abc"), 0, b"Abc")]
    #[case::data_1(new_fixed_buf(20, b"abc"), 1, b"aAc")]
    #[case::data_2(new_fixed_buf(20, b"abc"), 2, b"abA")]
    #[case::full(new_fixed_buf(5, b"qrstu"), 0, b"Arstu")]
    #[case::full_2(new_fixed_buf(5, b"qrstu"), 2, b"qrAtu")]
    #[case::full_4(new_fixed_buf(5, b"qrstu"), 4, b"qrstA")]
    fn test_as_mut_modification_slice(#[case] mut buf: FixedBuf, #[case] offset: usize, #[case] expected: &[u8]) {
        buf.slice(offset).as_mut()[0] = 65;
        assert_eq!(buf.as_ref(), expected);
    }

    #[rstest]
    #[case::data(new_array_buf(b"abc"), b"Abc")]
    #[case::full(new_array_buf(b"qrstu"), b"Arstu")]
    fn test_as_mut_modification_array(#[case] mut buf: ArrayBuf<5>, #[case] expected: &[u8]) {
        buf.as_mut()[0] = 65;
        assert_eq!(buf.as_ref(), expected);
    }

    #[test]
    fn test_buf_mut_chunk_mut() {
        let mut buffer = FixedBuf::new(1000);
        buffer.put_slice(b"hello");

        assert_eq!(buffer.remaining_mut(), 1000 - 5);

        let chunk = buffer.chunk_mut();
        assert_eq!(chunk.len(), 1000 - 5);

        chunk[..7].copy_from_slice(b" world!");
        assert_eq!(buffer.remaining_mut(), 1000 - 5);

        assert_eq!(buffer.as_ref(), b"hello");
        unsafe { buffer.advance_mut(6); }
        assert_eq!(buffer.remaining_mut(), 1000 - 11);

        assert_eq!(buffer.as_ref(), b"hello world");
    }

    #[test]
    fn test_buf_mut_chunk_mut_slice() {
        let mut buffer = FixedBuf::new(1000);
        buffer.put_slice(b"hello");

        assert_eq!(buffer.remaining_mut(), 1000 - 5);

        {
            let mut buffer = buffer.slice(3);
            assert_eq!(buffer.remaining_mut(), 1000 - 5);

            let chunk = buffer.chunk_mut();
            assert_eq!(chunk.len(), 1000 - 5);

            chunk[..7].copy_from_slice(b" world!");
            assert_eq!(buffer.remaining_mut(), 1000 - 5);

            assert_eq!(buffer.as_ref(), b"lo");
            unsafe { buffer.advance_mut(6); }
            assert_eq!(buffer.as_ref(), b"lo world");
            assert_eq!(buffer.remaining_mut(), 1000 - 11);
        }

        assert_eq!(buffer.as_ref(), b"hello world");
        assert_eq!(buffer.remaining_mut(), 1000 - 11);
    }

    #[test]
    fn test_buf_mut_chunk_mut_array() {
        let mut buffer = ArrayBuf::<1000>::new();
        buffer.put_slice(b"hello");

        assert_eq!(buffer.remaining_mut(), 1000 - 5);

        let chunk = buffer.chunk_mut();
        assert_eq!(chunk.len(), 1000 - 5);

        chunk[..7].copy_from_slice(b" world!");
        assert_eq!(buffer.remaining_mut(), 1000 - 5);

        assert_eq!(buffer.as_ref(), b"hello");
        unsafe { buffer.advance_mut(6); }
        assert_eq!(buffer.remaining_mut(), 1000 - 11);

        assert_eq!(buffer.as_ref(), b"hello world");
    }

    #[test]
    fn test_buffer_extend_from_slice() {
        let mut buffer = FixedBuf::new(1000);
        assert_eq!(buffer.as_ref(), b"");

        buffer.extend_from_slice(b"hello").unwrap();
        assert_eq!(buffer.as_ref(), b"hello");

        buffer.extend_from_slice(b" world").unwrap();
        assert_eq!(buffer.as_ref(), b"hello world");
    }

    #[test]
    fn test_buffer_extend_from_slice_slice() {
        let mut buffer = FixedBuf::new(1000);
        assert_eq!(buffer.as_ref(), b"");

        buffer.extend_from_slice(b"hello ").unwrap();
        assert_eq!(buffer.as_ref(), b"hello ");

        let mut slice_buf = buffer.slice(5);
        assert_eq!(slice_buf.as_ref(), b" ");

        slice_buf.extend_from_slice(b"world").unwrap();
        assert_eq!(slice_buf.as_ref(), b" world");

        assert_eq!(buffer.as_ref(), b"hello world");
    }

    #[test]
    fn test_buffer_extend_from_slice_array() {
        let mut buffer = ArrayBuf::<1000>::new();
        assert_eq!(buffer.as_ref(), b"");

        buffer.extend_from_slice(b"hello").unwrap();
        assert_eq!(buffer.as_ref(), b"hello");

        buffer.extend_from_slice(b" world").unwrap();
        assert_eq!(buffer.as_ref(), b"hello world");
    }

    #[rstest]
    #[case::l5(5, b"hello", b"hello\0")]
    #[case::l3(3, b"hel", b"hell")]
    #[case::l1(1, b"h", b"he")]
    #[case::l0(0, b"", b"h")]
    fn test_buffer_truncate(#[case] len: usize, #[case] expected: &[u8], #[case] expected_plus_1: &[u8]) {
        let mut buffer = FixedBuf::new(1000);
        buffer.put_slice(b"hello");

        buffer.truncate(len);
        assert_eq!(buffer.as_ref(), expected);

        buffer.truncate(len+1);
        assert_eq!(buffer.as_ref(), expected_plus_1);
    }

    #[rstest]
    #[case::l5(5, b"hello")]
    #[case::l3(3, b"hel")]
    #[case::l1(1, b"h")]
    #[case::l0(0, b"")]
    fn test_buffer_truncate_slice(#[case] len: usize, #[case] expected: &[u8]) {
        let mut outer_buffer = FixedBuf::new(1000);
        outer_buffer.put_slice(b"yo hello");

        let mut buffer = outer_buffer.slice(3);
        assert_eq!(buffer.as_ref(), b"hello");

        buffer.truncate(len);
        assert_eq!(buffer.as_ref(), expected);

        let mut expected_outer = b"yo ".to_vec();
        expected_outer.extend_from_slice(expected);
        assert_eq!(outer_buffer.as_ref(), expected_outer);
    }

    #[rstest]
    #[case::l5(5, b"hello", b"hello\0")]
    #[case::l3(3, b"hel", b"hell")]
    #[case::l1(1, b"h", b"he")]
    #[case::l0(0, b"", b"h")]
    fn test_buffer_truncate_arraybuf(#[case] len: usize, #[case] expected: &[u8], #[case] expected_plus_1: &[u8]) {
        let mut buffer = ArrayBuf::<1000>::new();
        buffer.put_slice(b"hello");

        buffer.truncate(len);
        assert_eq!(buffer.as_ref(), expected);

        buffer.truncate(len+1);
        assert_eq!(buffer.as_ref(), expected_plus_1);
    }
}
