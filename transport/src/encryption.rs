use std::sync::atomic::{AtomicU64, Ordering};
use aead::{AeadInPlace, Nonce};
use aes_gcm::Aes256Gcm;
use bytes::BufMut;
use crate::buffers::fixed_buffer::FixedBuf;
use crate::packet_header::PacketHeader;


pub trait RudpEncryption: Send + Sync {
    fn encryption_overhead(&self) -> usize;

    fn init_buffer(&self, buffer: &mut FixedBuf);

    fn encrypt_buffer(&self, plaintext: &[u8], ciphertext: &mut FixedBuf);
}

pub struct NoEncryption;
impl RudpEncryption for NoEncryption {
    fn encryption_overhead(&self) -> usize {
        0
    }

    fn init_buffer(&self, _buffer: &mut FixedBuf) {
        // nothing to be done
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

    fn encrypt_buffer(&self, plaintext: &[u8], ciphertext: &mut FixedBuf) {
        let nonce = {
            let mut nonce_array = [0u8; 12];
            let mut nonce_buf_mut: &mut [u8] = &mut nonce_array;
            let nonce_buf_mut_ref = &mut nonce_buf_mut;
            nonce_buf_mut_ref.put_u32(self.nonce_fixed);
            nonce_buf_mut_ref.put_u64(self.nonce_incremented.fetch_add(1, Ordering::AcqRel));
            Nonce::<Aes256Gcm>::from_slice(todo!()) //&nonce_array)
        };




        let mut a = [0u8; 1024];
        let mut b: &mut [u8] = &mut a;
        let mut c = &mut b;


        // match self.cipher.encrypt_in_place(nonce, b"", c) { //TODO
        //     Ok(()) => {}
        //     Err(e) => {
        //         todo!()
        //     }
        // }


        todo!()
    }
}