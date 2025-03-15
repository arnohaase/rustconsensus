pub mod udp;

use std::net::SocketAddr;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait Transport : Sync + Send {
    async fn send(&self, to: SocketAddr, buf: &[u8]) -> anyhow::Result<()>;

    async fn recv_loop(&self, handler: Arc<dyn MessageHandler>) -> anyhow::Result<()>;

    fn shut_down_recv_loop(&self);
}

//TODO UDP based transport that collects messages before sending, negative ACK, resend etc.
//TODO optional encryption wrapper for transport

/// This trait decouples the implementation of message transport (different strategies) from the
///  handling of a message once it is received (always the same, part of the transport library core).
///
/// It is passed around as an `Arc<dyn ...>` to minimize dependencies of [Transport] implementations.
#[async_trait::async_trait]
pub trait MessageHandler : Sync + Send {
    async fn handle_message(&self, buf: &[u8], sender: SocketAddr);
}
