use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

pub const MAX_MSG_SIZE: usize = 256*1024; //TODO make this configurable

#[cfg_attr(test, automock)]
#[async_trait]
pub trait MessageSender: Debug + Send + Sync + 'static {
    fn get_self_addr(&self) -> NodeAddr;

    //TODO documentation

    async fn send_to_node<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()>;

    async fn send_to_addr<T: Message>(&self, to: SocketAddr, msg: &T) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Messaging: MessageSender {
    fn register_module(&self, message_module: Arc<dyn MessageModule>);
    fn deregister_module(&self, id: MessageModuleId);
    async fn recv(&self);
}
