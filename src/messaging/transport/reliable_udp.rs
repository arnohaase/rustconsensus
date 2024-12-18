mod buffers;
mod config;

use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use crate::messaging::transport::{MessageHandler, Transport};


/// This transport implements some additional guarantees on top of UDP:
///
/// * Message ordering: Messages sent from the same sender to the same receiver will arrive
///      in that order (if they arrive at all)
/// * Some retrying: If network packets get lost (or mangled on the network), the transport
///      implementation will retry to some degree (this comes at a cost of resource usage and other
///      messages being delayed, and it is configurable).
/// * Big messages are split into several UDP datagrams for robustness and efficiency
///
/// It is inspired by the Aeron library, and it maintains a set of buffers
pub struct ReliableUdpTransport {}

#[async_trait]
impl Transport for ReliableUdpTransport {
    async fn send(&self, to: SocketAddr, buf: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn recv_loop(&self, handler: Arc<dyn MessageHandler>) -> anyhow::Result<()> {
        todo!()
    }

    fn shut_down_recv_loop(&self) {
        todo!()
    }
}