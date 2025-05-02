use bytes::{Buf, BufMut};
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::tcp::encryption::Nonce;

pub struct InitMsg {
    pub self_addr: NodeAddr,
    pub nonce: Nonce,
}
impl InitMsg { //TODO unit test
    pub fn ser(&self, buf: &mut impl BufMut) {
        self.self_addr.ser(buf);
        self.nonce.ser(buf);
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<Self> {
        let self_addr = NodeAddr::try_deser(buf)?;
        let nonce = Nonce::deser(buf)?;
        Ok(InitMsg {
            self_addr,
            nonce,
        })
    }
}

pub struct InitResponseMsg {
    pub peer_unique: u64, //TODO unique part: u32 instead of u64
}

//TODO rename try_deser to deser()
impl InitResponseMsg { //TODO unit test
    pub const SERIALIZED_LEN: usize = 8;

    pub fn ser(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.peer_unique);
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<Self> {
        let peer_unique = buf.try_get_u64()?;
        Ok(InitResponseMsg { peer_unique })
    }
}
