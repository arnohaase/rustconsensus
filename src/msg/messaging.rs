use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use rustc_hash::FxHashMap;
use tracing::{debug, error, info, trace, warn};

use crate::msg::envelope::Envelope;
use crate::msg::message_module::{MessageModule, MessageModuleId};
use crate::msg::transport::{MessageHandler, Transport, UdpTransport};
use crate::msg::node_addr::NodeAddr;


pub const MAX_MSG_SIZE: usize = 16384; //TODO make this configurable

pub struct Messaging {
    myself: NodeAddr,
    message_modules: Arc<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>,
    transport: Arc<dyn Transport>,
}

impl Debug for Messaging {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpTransport{{myself:{:?}}}", &self.myself)
    }
}

impl Messaging {
    pub async fn new(myself: NodeAddr, message_modules: Vec<Arc<dyn MessageModule>>) -> anyhow::Result<Messaging> {
        let transport = Arc::new(UdpTransport::new(myself.addr).await?); //TODO configurable transport

        let mut message_module_map = FxHashMap::default();
        for m in message_modules {
            if let Some(prev) = message_module_map.insert(m.id(), m) {
                warn!("registering a second message module for module id {:?}, replacing the first", prev.id());
            }
        }

        Ok(Messaging {
            myself,
            message_modules: Arc::new(message_module_map),
            transport,
        })
    }

    pub fn get_addr(&self) -> NodeAddr {
        self.myself
    }

    /// Passing in the message as a byte slice instead of serializing it into the send buffer may introduce
    ///  some overhead, but it simplifies the design. If profiling shows significant potential for
    ///  speedup at some point, this may be worth revisiting, but for now it looks like a good trade-off.
    ///
    /// When Tokio's UdpSocket adds support for multi-buffer send, the point may be moot anyway.
    pub async fn send(&self, to: NodeAddr, msg_module_id: MessageModuleId, msg: &[u8]) -> anyhow::Result<()> {
        debug!(from=?self.myself, ?to, "sending message");

        let mut buf = BytesMut::new();
        Envelope::write(self.myself, to, msg_module_id, &mut buf);
        buf.put_slice(msg);

        self.transport.send(to.addr, &buf).await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn recv(&self) -> anyhow::Result<()> {
        info!("starting receive loop");

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

struct ReceivedMessageHandler {
    myself: NodeAddr,
    message_modules: Arc<FxHashMap<MessageModuleId, Arc<dyn MessageModule>>>,
}

impl MessageHandler for ReceivedMessageHandler {
    fn handle_message(&self, msg_buf: &[u8], sender: SocketAddr) {
        //TODO safeguard against panics

        debug!("received message");
        trace!(?msg_buf);

        if msg_buf.len() == MAX_MSG_SIZE {
            warn!("received a message exceeding max message size of {} bytes - skipping", MAX_MSG_SIZE);
            return;
        }

        let mut msg_buf = msg_buf;
        match Envelope::try_read(&mut msg_buf, sender, self.myself.addr) {
            Ok(env) => {
                //TODO check myself unique part

                if let Some(message_module) = self.message_modules.get(&env.message_module_id) {
                    message_module.on_message(msg_buf);
                }
                else {
                    warn!("received message for module {:?} for which there is no handler - ignoring. Different nodes may be running different software versions", env.message_module_id);
                }
            }
            Err(e) => {
                warn!("received a message without a valid envelope - discarding: {}", e);
            }
        }
    }
}
