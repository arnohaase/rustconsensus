use std::net::SocketAddr;
use bytes::Buf;


pub struct MessageDispatcher {

}

impl MessageDispatcher {
    pub async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, msg_buf: &mut impl Buf) {
        todo!()
    }
}