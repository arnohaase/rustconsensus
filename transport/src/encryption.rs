use std::sync::atomic::{AtomicU64, Ordering};
use aead::{AeadInPlace, Nonce};
use aes_gcm::Aes256Gcm;
use bytes::BufMut;
use tracing::error;
use crate::buffers::fixed_buffer::{ArrayFixedBuf, FixedBuf, FixedBuffer};
use crate::packet_header::PacketHeader;


pub trait RudpEncryption: Send + Sync {
    fn encryption_overhead(&self) -> usize;

    fn init_buffer(&self, buffer: &mut FixedBuf);

    fn encrypt_buffer(&self, plaintext: &[u8], ciphertext: &mut FixedBuf);
}

pub struct NoEncryption;
impl RudpEncryption for NoEncryption {
    fn encryption_overhead(&self) -> usize {
        1 // protocol version
    }

    fn init_buffer(&self, buffer: &mut FixedBuf) {
        buffer.put_u8(PacketHeader::PROTOCOL_VERSION_1);
    }

    fn encrypt_buffer(&self, _plaintext: &[u8], _ciphertext: &mut FixedBuf) {
        // nothing to be done
    }
}


pub struct AesEncryption {
    cipher: Aes256Gcm,
    nonce_fixed: u32,
    nonce_incremented: AtomicU64,
}
impl AesEncryption {
    const INIT_PREFIX_LEN: usize = 13; // version + nonce
}


impl RudpEncryption for AesEncryption {
    fn encryption_overhead(&self) -> usize {
        1           // protocol version
            + 12    // nonce
            + 16    // tag / hash
    }


    /// write the encryption header (with a new, unique nonce) to an empty buffer
    fn init_buffer(&self, buffer: &mut FixedBuf) {
        buffer.put_u8(PacketHeader::PROTOCOL_VERSION_1);
        buffer.put_u32(self.nonce_fixed);
        buffer.put_u64(self.nonce_incremented.fetch_add(1, Ordering::AcqRel));
    }

    fn encrypt_buffer(&self, plaintext: &[u8], full_buf: &mut FixedBuf) {
        let mut buf = FixedBuffer::from_buf(ArrayFixedBuf::<12>::new());
        buf.put_u32(self.nonce_fixed);
        buf.put_u64(self.nonce_incremented.fetch_add(1, Ordering::AcqRel));
        let nonce = Nonce::<Aes256Gcm>::from_slice(buf.as_ref());

        let mut buf = full_buf.slice(Self::INIT_PREFIX_LEN);
        match self.cipher.encrypt_in_place(nonce, b"", &mut buf) {
            Ok(()) => {}
            Err(e) => {
                error!("encryption error: {}", e);
                panic!("encryption error");
            }
        }
    }
}

//TODO unit tests