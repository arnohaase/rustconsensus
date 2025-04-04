use crate::buffers::fixed_buffer::FixedBuf;
use crate::packet_header::PacketHeader;
use aead::{AeadCore, AeadInPlace, Key, KeyInit, Nonce, OsRng};
use aes_gcm::Aes256Gcm;
use bytes::{Buf, BufMut};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::error;


pub trait RudpEncryption: Send + Sync {
    /// This is the number of bytes added at the start of a packet, before the actually encrypted
    ///  payload
    fn prefix_len(&self) -> usize;

    /// This can be greater than the prefix length if the ciphertext is longer then the plaintext,
    ///  e.g. because it includes a tag / hash for verification
    fn encryption_overhead(&self) -> usize;


    fn init_buffer(&self, buffer: &mut FixedBuf);

    fn encrypt_buffer(&self, buf: &mut FixedBuf);

    fn decrypt_buffer(&self, buf: &mut FixedBuf) -> aead::Result<()>;
}

pub struct NoEncryption;
impl RudpEncryption for NoEncryption {
    fn prefix_len(&self) -> usize {
        1 // protocol version
    }

    fn encryption_overhead(&self) -> usize {
        1 // protocol version
    }

    fn init_buffer(&self, buffer: &mut FixedBuf) {
        buffer.put_u8(PacketHeader::PROTOCOL_VERSION_1);
    }

    fn encrypt_buffer(&self, _buf: &mut FixedBuf) {
        // nothing to be done
    }

    fn decrypt_buffer(&self, _buf: &mut FixedBuf) -> aead::Result<()> {
        // nothing to be done
        Ok(())
    }
}


pub struct Aes256GcmEncryption {
    cipher: Aes256Gcm,
    nonce_fixed: u32,
    nonce_incremented: AtomicU64,
}

impl Aes256GcmEncryption {
    /// key must be exactly 32 bytes
    pub fn new(key: &[u8]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(&key);

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let mut nonce_slice = nonce.as_slice();
        let nonce_buf = &mut nonce_slice;
        let nonce_fixed = nonce_buf.get_u32();
        let nonce_incremented = AtomicU64::new(nonce_buf.get_u64());

        Aes256GcmEncryption {
            cipher,
            nonce_fixed,
            nonce_incremented,
        }
    }

    fn nonce_from_buf(full_buf: &[u8]) -> Nonce<Aes256Gcm> {
        let nonce_buf: [u8;12] = full_buf.as_ref()[1..13].try_into().unwrap();
        Nonce::<Aes256Gcm>::from(nonce_buf)
    }
}

impl RudpEncryption for Aes256GcmEncryption {
    fn prefix_len(&self) -> usize {
        1          // protocol version
            + 12   // nonce
    }

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

    fn encrypt_buffer(&self, full_buf: &mut FixedBuf) {
        let nonce = Self::nonce_from_buf(full_buf.as_ref());
        let mut buf = full_buf.slice(self.prefix_len());

        match self.cipher.encrypt_in_place(&nonce, b"", &mut buf) {
            Ok(()) => {}
            Err(e) => {
                error!("encryption error: {}", e);
                panic!("encryption error");
            }
        }
    }

    fn decrypt_buffer(&self, full_buf: &mut FixedBuf) -> aead::Result<()> {
        let nonce = Self::nonce_from_buf(full_buf.as_ref());
        let mut buf = full_buf.slice(self.prefix_len());
        self.cipher.decrypt_in_place(&nonce, b"", &mut buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::buffers::fixed_buffer::FixedBuf;
    use aead::{AeadCore, AeadInPlace, Buffer, Key, KeyInit, Nonce};
    use bytes::{Buf, BufMut};

    #[rstest]
    #[case(NoEncryption)]
    #[case(Aes256GcmEncryption::new(&[9u8;32]))]
    fn test_encrypt_decrypt(#[case] encryption: impl RudpEncryption + 'static) {
        let plaintext = b"hello world";

        let mut buf = FixedBuf::new(100);
        encryption.init_buffer(&mut buf);
        buf.put_slice(plaintext);

        encryption.encrypt_buffer(&mut buf);
        encryption.decrypt_buffer(&mut buf).unwrap();

        assert_eq!(&buf.as_ref()[encryption.prefix_len()..], plaintext);
    }
}