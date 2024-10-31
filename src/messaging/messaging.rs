use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::anyhow;

use bytes::BytesMut;
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use crate::messaging::envelope::{Checksum, Envelope};
use crate::messaging::message_module::{MessageModule, MessageModuleId};
use crate::messaging::transport::{MessageHandler, Transport, UdpTransport};
use crate::messaging::node_addr::NodeAddr;


pub const MAX_MSG_SIZE: usize = 256*1024; //TODO make this configurable

pub struct Messaging {
    myself: NodeAddr,
    message_modules: Arc<RwLock<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>>,
    transport: Arc<dyn Transport>,
}

impl Debug for Messaging {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpTransport{{myself:{:?}}}", &self.myself)
    }
}

impl Messaging {
    pub async fn new(myself: NodeAddr) -> anyhow::Result<Messaging> {
        Ok(Messaging {
            myself,
            message_modules: Default::default(),
            transport: Arc::new(UdpTransport::new(myself.addr).await?), //TODO configurable transport
        })
    }

    pub async fn register_module(&self, message_module: Arc<dyn MessageModule>) -> anyhow::Result<()> {
        match self.message_modules.write().await
            .entry(message_module.id())
        {
            Entry::Occupied(_) => {
                Err(anyhow!("registering a second message module for module id {:?}, replacing the first", message_module.id()))
            }
            Entry::Vacant(e) => {
                let _ = e.insert(message_module);
                Ok(())
            },
        }
    }

    pub async fn deregister_module(&self, id: MessageModuleId) -> anyhow::Result<()> {
        let prev = self.message_modules.write().await
            .remove(&id);
        if prev.is_none() {
            return Err(anyhow!("deregistering a module that was not previously registered: {:?}", id));
        }
        Ok(())
    }

    pub fn get_self_addr(&self) -> NodeAddr {
        self.myself
    }

    /// Passing in the message as a byte slice instead of serializing it into the send buffer may introduce
    ///  some overhead, but it simplifies the design. If profiling shows significant potential for
    ///  speedup at some point, this may be worth revisiting, but for now it looks like a good trade-off.
    ///
    /// When Tokio's UdpSocket adds support for multi-buffer send, the point may be moot anyway.
    pub async fn send(&self, to: NodeAddr, msg_module_id: MessageModuleId, msg: &[u8]) -> anyhow::Result<()> {
        match self._send(to, msg_module_id, msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("error sending message: {}", e);
                Err(e)
            }
        }
    }

    async fn _send(&self, to: NodeAddr, msg_module_id: MessageModuleId, msg: &[u8]) -> anyhow::Result<()> {
        debug!(from=?self.myself, ?to, "sending message");

        let checksum = Checksum::new(self.myself, to, msg_module_id, msg);

        let mut buf = BytesMut::new();
        Envelope::write(self.myself, to, checksum, msg_module_id, &mut buf);

        buf.extend_from_slice(msg);

        self.transport.send(to.addr, &buf).await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn recv(&self) -> anyhow::Result<()> {
        let handler = ReceivedMessageHandler {
            myself: self.myself,
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

/// This is a well-known ID: Messaging checks the unique part that a message is addressed to,
///  except for JOIN messages
pub const JOIN_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"ClstJoin");


struct ReceivedMessageHandler {
    myself: NodeAddr,
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
        match Envelope::try_read(&mut msg_buf, self.myself.addr) {
            Ok(envelope) => {
                trace!("message is from {:?}", envelope.from);

                // NB: JOIN messages are the only messages that are accepted regardless of target node address' unique part
                if envelope.to.unique != self.myself.unique && envelope.message_module_id != JOIN_MESSAGE_MODULE_ID {
                    warn!("received a message for {:?}: wrong unique part - was a node restarted without rejoining? Ignoring the message", envelope.to);
                    return;
                }

                let actual_checksum = Checksum::new(envelope.from, envelope.to, envelope.message_module_id, msg_buf);
                if envelope.checksum != actual_checksum {
                    warn!("checksum error in message - skipping");
                    return;
                }

                if let Some(message_module) = self.message_modules.read().await.get(&envelope.message_module_id) {
                    let message_module = message_module.clone();
                    message_module.on_message(&envelope, msg_buf).await;
                }
                else {
                    warn!("received message for module {:?} for which there is no handler - ignoring. Different nodes may be running different software versions", envelope.message_module_id);
                }
            }
            Err(e) => {
                warn!("received a message without a valid envelope - discarding: {}", e);
            }
        }
    }
}
