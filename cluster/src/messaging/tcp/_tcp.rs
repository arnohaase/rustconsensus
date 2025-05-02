
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::net::TcpListener;
use tracing::warn;
use transport::buffers::atomic_map::AtomicMap;
use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;

pub struct TcpMessaging {
    // end_point: EndPoint,
    message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>>,
}
impl Debug for TcpMessaging {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TcpMessagingImpl")
    }
}

impl TcpMessaging {
    pub async fn new() -> anyhow::Result<TcpMessaging> {
        todo!()
    }
}

#[async_trait]
impl MessageSender for TcpMessaging {
    fn get_self_addr(&self) -> NodeAddr {
        todo!()
    }

    async fn send_to_node<T: Message>(&self, to: NodeAddr, stream_id: u16, msg: &T) -> anyhow::Result<()>{
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        todo!()
    }

    async fn send_to_addr<T: Message>(&self, to: SocketAddr, stream_id: u16, msg: &T) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        todo!()
    }

    async fn send_raw_fire_and_forget<T: Message>(&self, to_addr: SocketAddr, required_to_generation: Option<u64>, msg: &T) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        todo!()
    }
}

#[async_trait]
impl Messaging for TcpMessaging {
    fn register_module(&self, message_module: Arc<dyn MessageModule>) {
        self.message_modules
            .overwrite_entry(message_module.id(), message_module);
    }

    fn deregister_module(&self, id: MessageModuleId) {
        self.message_modules
            .remove(&id);
    }

    async fn recv(&self) {
        todo!()
        // self.end_point.recv_loop().await;
    }
}
