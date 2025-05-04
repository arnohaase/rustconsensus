use aead::{AeadCore, AeadInPlace, Key, KeyInit, Nonce, OsRng};
use aes_gcm::Aes256Gcm;
use anyhow::bail;
use bytes::{Buf, BytesMut};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, trace};

pub trait UdpEncryption: Send + Sync {
    fn encrypt_buffer(&self, buf: &mut BytesMut);

    fn decrypt_buffer(&self, buf: &mut BytesMut) -> anyhow::Result<()>;
}

pub struct NoEncryption;
impl UdpEncryption for NoEncryption {
    fn encrypt_buffer(&self, _buf: &mut BytesMut) {
        // nothing to be done
    }

    fn decrypt_buffer(&self, _buf: &mut BytesMut) -> anyhow::Result<()> {
        // nothing to be done
        Ok(())
    }
}

pub struct Aes256GcmEncryption {
    cipher: Aes256Gcm, //TODO bigger Nonce to reduce risk of collision? -> security warning on 'generate_nonce'
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
   
    fn unique_nonce(&self) -> Nonce<Aes256Gcm> {
        let mut buf: Vec<u8> = Vec::with_capacity(12);
        buf.extend_from_slice(self.nonce_fixed.to_le_bytes().as_ref());
        buf.extend_from_slice(self.nonce_incremented.fetch_add(37, Ordering::AcqRel).to_le_bytes().as_ref());
        Nonce::<Aes256Gcm>::clone_from_slice(&buf)
    }
}

impl UdpEncryption for Aes256GcmEncryption {
    fn encrypt_buffer(&self, buf: &mut BytesMut) {
        let nonce = self.unique_nonce();
        trace!("encrypting {:?} with nonce {:?}", buf, nonce);
        
        if let Err(_) = self.cipher.encrypt_in_place(&nonce, b"", buf) {
            error!("error encrypting buffer");
            panic!("error encrypting buffer"); //TODO better way of handling this?
        }
        
        buf.extend_from_slice(nonce.as_slice());
        trace!("encrypted: {:?}", buf);
    }

    fn decrypt_buffer(&self, full_buf: &mut BytesMut) -> anyhow::Result<()> {
        if full_buf.len() < 12 {
            bail!("received buffer too short");
        }
        
        let nonce = Nonce::<Aes256Gcm>::clone_from_slice(&full_buf.as_ref()[full_buf.len() - 12..]);
        full_buf.truncate(full_buf.len() - 12);
        trace!("decrypting {:?}, nonce: {:?}", full_buf, nonce);
        if let Err(_) = self.cipher.decrypt_in_place(&nonce, b"", full_buf) {
            bail!("decryption error");
        }
        trace!("decrypted {:?}", full_buf);
        Ok(())
    }
}


