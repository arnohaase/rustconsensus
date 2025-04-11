use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
#[cfg(test)] use mockall::automock;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, warn};
use transport::buffers::atomic_map::AtomicMap;
use transport::config::RudpConfig;
use transport::end_point::EndPoint;
use transport::message_dispatcher::MessageDispatcher;

pub const MAX_MSG_SIZE: usize = 256*1024; //TODO make this configurable

#[cfg_attr(test, automock)]
#[async_trait]
pub trait MessageSender: Debug + Send + Sync + 'static {
    fn get_self_addr(&self) -> NodeAddr;

    async fn send<T: Message>(&self, to: NodeAddr, msg: &T) {
        if let Err(e) = self.try_send(to, msg).await {
            error!("Error sending message: {}", e);
        }
    }
    async fn try_send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Messaging: MessageSender {
    fn register_module(&self, message_module: Arc<dyn MessageModule>);
    fn deregister_module(&self, id: MessageModuleId);
    async fn recv(&self) -> anyhow::Result<()>;
}

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

        let message_dispatcher = MessageDispatcherImpl {
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
            unique: self.end_point.generation(),
            socket_addr: self.end_point.self_addr(),
        }
    }

    async fn try_send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {


        todo!()
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

    async fn recv(&self) -> anyhow::Result<()> {
        self.end_point.recv_loop().await;
        Ok(()) //todo!()
    }
}

struct MessageDispatcherImpl {
    message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>>,
}

#[async_trait]
impl MessageDispatcher for MessageDispatcherImpl {
    async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, mut msg_buf: &[u8]) {
        match MessageModuleId::deser(&mut msg_buf) {
            Ok(id) => {
                match self.message_modules.load().get(&id) {
                    Some(message_module) => {
                        message_module.on_message(todo!(), msg_buf).await;
                    }
                    None => {
                        warn!("received a message for message id {:?} which is not registered - skipping message", id);
                    }
                }
            }
            Err(e) => {
                warn!("received a message without a valid id - skipping");
            }
        }
    }
}


// impl Debug for MessagingImpl {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
//
// #[async_trait]
// impl MessageSender for MessagingImpl {
//     fn get_self_addr(&self) -> NodeAddr {
//         NodeAddr {
//             unique: self.end_point.generation(),
//             socket_addr: self.end_point.self_addr(),
//         }
//     }
//
//     async fn try_send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
//         let msg_module_id = msg.module_id();
//         self._send(to, msg_module_id, msg).await
//     }
// }
// #[async_trait]
// impl Messaging for MessagingImpl {
//     async fn register_module(&self, message_module: Arc<dyn MessageModule>) -> anyhow::Result<()> {
//         match self.message_modules.write().await
//             .entry(message_module.id())
//         {
//             Entry::Occupied(_) => {
//                 Err(anyhow!("registering a second message module for module id {:?}, replacing the first", message_module.id()))
//             }
//             Entry::Vacant(e) => {
//                 e.insert(message_module);
//                 Ok(())
//             },
//         }
//     }
//
//     async fn deregister_module(&self, id: MessageModuleId) -> anyhow::Result<()> {
//         let prev = self.message_modules.write().await
//             .remove(&id);
//         if prev.is_none() {
//             return Err(anyhow!("deregistering a module that was not previously registered: {:?}", id));
//         }
//         Ok(())
//     }
//
//     #[tracing::instrument] //TODO instrument with some unique message id instead of this generic sig
//     async fn recv(&self) -> anyhow::Result<()> {
//         self.end_point.recv_loop().await?;
//
//
//
//         let handler = ReceivedMessageHandler {
//             myself: self.myself,
//             shared_secret: self.shared_secret.clone(),
//             message_modules: self.message_modules.clone(),
//         };
//
//         match self.transport.recv_loop(Arc::new(handler)).await {
//             Ok(()) => {
//                 info!("shutting down receiver");
//                 Ok(())
//             }
//             Err(e) => {
//                 error!("error: {}", e);
//                 Err(e)
//             }
//         }
//     }
// }
//
// impl MessagingImpl {
//     pub async fn new(self_addr: SocketAddr, config: RudpConfig) -> anyhow::Result<MessagingImpl> {
//         Ok(MessagingImpl {
//             end_point: EndPoint::new(self_addr, message_dispatcher, config),
//             message_modules: Default::default(),
//         })
//     }
//
//     async fn _send(&self, to: NodeAddr, msg_module_id: MessageModuleId, msg: &dyn Message) -> anyhow::Result<()> {
//         trace!(from=?self.myself, ?to, "sending message");
//
//         let mut msg_buf = BytesMut::new();
//         msg.ser(&mut msg_buf);
//
//         //TODO serialize to a single buffer, patch actual checksum afterwards
//
//         let checksum = Checksum::new(&self.shared_secret, self.myself, to, msg_module_id, &msg_buf);
//
//         let mut buf = BytesMut::new();
//         Envelope::write(self.myself, to, checksum, msg_module_id, &mut buf);
//
//         buf.extend_from_slice(&msg_buf);
//
//         self.transport.send(to.socket_addr, &buf).await?;
//         Ok(())
//     }
// }
//
// /// This is a well-known ID: Messaging checks the unique part that a message is addressed to,
// ///  except for JOIN messages
// pub const JOIN_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"CtrJoin\0");
//
//
// struct ReceivedMessageHandler {
//     myself: NodeAddr,
//     shared_secret: Vec<u8>,
//     message_modules: Arc<RwLock<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>>,
// }
//
// #[async_trait::async_trait]
// impl MessageHandler for ReceivedMessageHandler {
//     async fn handle_message(&self, msg_buf: &[u8], _sender: SocketAddr) {
//         //TODO safeguard against panics
//
//         trace!("received message {:?}", msg_buf);
//
//         if msg_buf.len() == MAX_MSG_SIZE {
//             warn!("received a message exceeding max message size of {} bytes - skipping", MAX_MSG_SIZE);
//             return;
//         }
//
//         let mut msg_buf = msg_buf;
//         match Envelope::try_read(&mut msg_buf, self.myself.socket_addr) {
//             Ok(envelope) => {
//                 trace!("message is from {:?}", envelope.from);
//
//                 // NB: JOIN messages are the only messages that are accepted regardless of target node address' unique part
//                 if envelope.to.unique != self.myself.unique && envelope.message_module_id != JOIN_MESSAGE_MODULE_ID {
//                     warn!("received a message for {:?}: wrong unique part - was a node restarted without rejoining? Ignoring the message", envelope.to);
//                     return;
//                 }
//
//                 let actual_checksum = Checksum::new(&self.shared_secret, envelope.from, envelope.to, envelope.message_module_id, msg_buf);
//                 if envelope.checksum != actual_checksum {
//                     warn!("checksum error in message - skipping");
//                     return;
//                 }
//
//                 if let Some(message_module) = self.message_modules.read().await.get(&envelope.message_module_id) {
//                     let message_module = message_module.clone();
//                     //TODO better spawn this? What about message ordering?
//                     //TODO decouple through a spsc channel with backpressure?
//                     //TODO or keep it simple like this, relying on handlers not doing expensive work on this thread?
//                     message_module.on_message(&envelope, msg_buf).await;
//                 }
//                 else {
//                     debug!("received message for module {:?} for which there is no handler (yet?) - ignoring.", envelope.message_module_id);
//                 }
//             }
//             Err(e) => {
//                 warn!("received a message without a valid envelope - discarding: {}", e);
//             }
//         }
//     }
// }
