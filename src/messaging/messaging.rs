use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(test)] use mockall::automock;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use crate::messaging::envelope::{Checksum, Envelope};
use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::transport::{MessageHandler, Transport, UdpTransport};


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
    async fn register_module(&self, message_module: Arc<dyn MessageModule>) -> anyhow::Result<()>;
    async fn deregister_module(&self, id: MessageModuleId) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<()>;
}

pub struct MessagingImpl {
    myself: NodeAddr,
    shared_secret: Vec<u8>,
    message_modules: Arc<RwLock<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>>,
    transport: Arc<dyn Transport>,
}

impl Debug for MessagingImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpTransport{{myself:{:?}}}", &self.myself)
    }
}

#[async_trait]
impl MessageSender for MessagingImpl {
    fn get_self_addr(&self) -> NodeAddr {
        self.myself
    }

    async fn try_send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        let msg_module_id = msg.module_id();
        self._send(to, msg_module_id, msg).await
    }
}
#[async_trait]
impl Messaging for MessagingImpl {
    async fn register_module(&self, message_module: Arc<dyn MessageModule>) -> anyhow::Result<()> {
        match self.message_modules.write().await
            .entry(message_module.id())
        {
            Entry::Occupied(_) => {
                Err(anyhow!("registering a second message module for module id {:?}, replacing the first", message_module.id()))
            }
            Entry::Vacant(e) => {
                e.insert(message_module);
                Ok(())
            },
        }
    }

    async fn deregister_module(&self, id: MessageModuleId) -> anyhow::Result<()> {
        let prev = self.message_modules.write().await
            .remove(&id);
        if prev.is_none() {
            return Err(anyhow!("deregistering a module that was not previously registered: {:?}", id));
        }
        Ok(())
    }

    #[tracing::instrument] //TODO instrument with some unique message id instead of this generic sig
    async fn recv(&self) -> anyhow::Result<()> {
        let handler = ReceivedMessageHandler {
            myself: self.myself,
            shared_secret: self.shared_secret.clone(),
            message_modules: self.message_modules.clone(),
        };

        match self.transport.recv_loop(Arc::new(handler)).await {
            Ok(()) => {
                info!("shutting down receiver");
                Ok(())
            }
            Err(e) => {
                error!("error: {}", e);
                Err(e)
            }
        }
    }
}

impl MessagingImpl {
    pub async fn new(myself: NodeAddr, shared_secret: &[u8]) -> anyhow::Result<MessagingImpl> {
        Ok(MessagingImpl {
            myself,
            shared_secret: shared_secret.to_vec(),
            message_modules: Default::default(),
            transport: Arc::new(UdpTransport::new(myself.socket_addr).await?), //TODO configurable transport
        })
    }

    async fn _send(&self, to: NodeAddr, msg_module_id: MessageModuleId, msg: &dyn Message) -> anyhow::Result<()> {
        trace!(from=?self.myself, ?to, "sending message");

        let mut msg_buf = BytesMut::new();
        msg.ser(&mut msg_buf);

        //TODO serialize to a single buffer, patch actual checksum afterwards

        let checksum = Checksum::new(&self.shared_secret, self.myself, to, msg_module_id, &msg_buf);

        let mut buf = BytesMut::new();
        Envelope::write(self.myself, to, checksum, msg_module_id, &mut buf);

        buf.extend_from_slice(&msg_buf);

        self.transport.send(to.socket_addr, &buf).await?;
        Ok(())
    }
}

/// This is a well-known ID: Messaging checks the unique part that a message is addressed to,
///  except for JOIN messages
pub const JOIN_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"CtrJoin\0");


struct ReceivedMessageHandler {
    myself: NodeAddr,
    shared_secret: Vec<u8>,
    message_modules: Arc<RwLock<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>>,
}

#[async_trait::async_trait]
impl MessageHandler for ReceivedMessageHandler {
    async fn handle_message(&self, msg_buf: &[u8], _sender: SocketAddr) {
        //TODO safeguard against panics

        trace!("received message {:?}", msg_buf);

        if msg_buf.len() == MAX_MSG_SIZE {
            warn!("received a message exceeding max message size of {} bytes - skipping", MAX_MSG_SIZE);
            return;
        }

        let mut msg_buf = msg_buf;
        match Envelope::try_read(&mut msg_buf, self.myself.socket_addr) {
            Ok(envelope) => {
                trace!("message is from {:?}", envelope.from);

                // NB: JOIN messages are the only messages that are accepted regardless of target node address' unique part
                if envelope.to.unique != self.myself.unique && envelope.message_module_id != JOIN_MESSAGE_MODULE_ID {
                    warn!("received a message for {:?}: wrong unique part - was a node restarted without rejoining? Ignoring the message", envelope.to);
                    return;
                }

                let actual_checksum = Checksum::new(&self.shared_secret, envelope.from, envelope.to, envelope.message_module_id, msg_buf);
                if envelope.checksum != actual_checksum {
                    warn!("checksum error in message - skipping");
                    return;
                }

                if let Some(message_module) = self.message_modules.read().await.get(&envelope.message_module_id) {
                    let message_module = message_module.clone();
                    message_module.on_message(&envelope, msg_buf).await;
                }
                else {
                    debug!("received message for module {:?} for which there is no handler (yet?) - ignoring.", envelope.message_module_id);
                }
            }
            Err(e) => {
                warn!("received a message without a valid envelope - discarding: {}", e);
            }
        }
    }
}
