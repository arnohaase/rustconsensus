use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{warn, Instrument, Span};
use transport::buffers::atomic_map::AtomicMap;
use transport::message_dispatcher::MessageDispatcher;

pub const MAX_MSG_SIZE: usize = 256*1024; //TODO make this configurable

pub const STREAM_ID_INTERNAL : u16 = 0xFFEE;
#[cfg_attr(test, automock)]
#[async_trait]
pub trait MessageSender: Debug + Send + Sync + 'static {
    fn get_self_addr(&self) -> NodeAddr;

    //TODO documentation

    async fn send_to_node<T: Message>(&self, to: NodeAddr, stream_id: u16, msg: &T) -> anyhow::Result<()>;

    async fn send_to_addr<T: Message>(&self, to: SocketAddr, stream_id: u16, msg: &T) -> anyhow::Result<()>;

    async fn send_raw_fire_and_forget<T: Message>(&self, to_addr: SocketAddr, required_to_generation: Option<u64>, msg: &T) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Messaging: MessageSender {
    fn register_module(&self, message_module: Arc<dyn MessageModule>);
    fn deregister_module(&self, id: MessageModuleId);
    async fn recv(&self);
}


pub struct MessageDispatcherImpl {
    pub message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>>,
}

#[async_trait]
impl MessageDispatcher for MessageDispatcherImpl {
    async fn on_message(&self, sender_addr: SocketAddr, sender_generation: u64, _stream_id: Option<u16>, full_msg_buf: Vec<u8>) {
        let mut msg_buf = full_msg_buf.as_ref();
        match MessageModuleId::deser(&mut msg_buf) {
            Ok(id) => {
                match self.message_modules.get(&id) {
                    Some(message_module) => {
                        tokio::spawn(async move {
                            message_module.on_message(NodeAddr {
                                unique: sender_generation,
                                socket_addr: sender_addr,
                            }, &full_msg_buf[size_of::<MessageModuleId>()..])
                                .instrument(Span::current())
                                .await;
                        });
                    }
                    None => {
                        warn!("received a message for message id {:?} which is not registered - skipping message", id);
                    }
                }
            }
            Err(_) => {
                warn!("received a message without a valid id - skipping");
            }
        }
    }
}
