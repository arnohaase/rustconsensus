use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
use bytes::BytesMut;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::warn;
use transport::buffers::atomic_map::AtomicMap;
use transport::config::RudpConfig;
use transport::end_point::EndPoint;

pub struct RudpMessagingImpl {
    end_point: EndPoint,
    message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>>,
}
impl Debug for RudpMessagingImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RudpMessagingImpl")
    }
}

impl RudpMessagingImpl {
    pub async fn new(config: RudpConfig) -> anyhow::Result<RudpMessagingImpl> {
        let message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>> = Default::default();

        let message_dispatcher = crate::messaging::messaging::MessageDispatcherImpl {
            message_modules: message_modules.clone(),
        };

        let end_point = EndPoint::new(Arc::new(message_dispatcher), Arc::new(config)).await?;

        Ok(RudpMessagingImpl {
            end_point,
            message_modules,
        })
    }
}

#[async_trait]
impl MessageSender for RudpMessagingImpl {
    fn get_self_addr(&self) -> NodeAddr {
        NodeAddr {
            unique: self.end_point.self_generation(),
            socket_addr: self.end_point.self_addr(),
        }
    }

    //TODO unit test
    async fn send_to_node<T: Message>(&self, to: NodeAddr, stream_id: u16, msg: &T) -> anyhow::Result<()>{
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        self.end_point.send_in_stream(to.socket_addr, Some(to.unique), stream_id, &buf).await
    }

    //TODO unit test
    async fn send_to_addr<T: Message>(&self, to: SocketAddr, stream_id: u16, msg: &T) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        self.end_point.send_in_stream(to, None, stream_id, &buf).await
    }

    //TODO unit test
    async fn send_raw_fire_and_forget<T: Message>(&self, to_addr: SocketAddr, required_to_generation: Option<u64>, msg: &T) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        msg.module_id().ser(&mut buf);
        msg.ser(&mut buf);

        self.end_point.send_outside_stream(to_addr, required_to_generation, &buf).await
    }
}

#[async_trait]
impl Messaging for RudpMessagingImpl {
    fn register_module(&self, message_module: Arc<dyn MessageModule>) {
        self.message_modules
            .update(|m| {
                if m.insert(message_module.id(), message_module.clone()).is_some() {
                    warn!("Registering message module {:?} which was already registered", message_module.id());
                };
            });
    }

    fn deregister_module(&self, id: MessageModuleId) {
        self.message_modules
            .update(|m| {
                if m.remove(&id).is_none() {
                    warn!("Deregistering message module {:?} which was not registered", id);
                };
            });
    }

    async fn recv(&self) {
        self.end_point.recv_loop().await;
    }
}
