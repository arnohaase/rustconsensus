use bytes::{Buf, BufMut, BytesMut};


pub trait Encryption: Send + Sync {
    fn encrypt_in_place(&self, nonce: &Nonce, buf: &mut BytesMut);
    
    fn decrypt_in_place(&self, nonce: &Nonce, buf: &mut [u8]) -> anyhow::Result<()>;
}


#[derive(Clone)]
pub struct Nonce {

}

impl Nonce {
    pub fn new_random() -> Self {
        todo!()
    }

    pub fn increment(&mut self) {
        todo!()
    }
    
    pub fn ser(&self, buf: &mut impl BufMut) {
        todo!()
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<Self> {
        todo!()
    }
}