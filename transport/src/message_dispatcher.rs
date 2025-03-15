use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use std::net::SocketAddr;


#[cfg_attr(test, automock)]
#[async_trait]
pub trait MessageDispatcher: Send + Sync + 'static {
    async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, msg_buf: &[u8]);
}
