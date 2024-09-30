use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

use anyhow::anyhow;
use bytes::{Buf, BufMut, BytesMut};
use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

use crate::comm::message_module::{MessageModule, MessageModuleId, MessageModuleReceiver};
use crate::node_addr::NodeAddr;

const MAX_MSG_SIZE: usize = 16384; //TODO make this configurable


pub struct Communicator {
    myself: NodeAddr,
    message_modules: FxHashMap<MessageModuleId, Box<dyn MessageModuleReceiver>>,
    cancel_sender: broadcast::Sender<()>,
}
impl Debug for Communicator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Communicator{{myself:{:?}}}", &self.myself)
    }
}

impl Communicator {
    pub fn new(myself: NodeAddr) -> Communicator {
        let (cancel_sender, _) = broadcast::channel(1);

        Communicator {
            myself,
            message_modules: Default::default(),
            cancel_sender,
        }
    }

    pub fn register_message_module<M: MessageModule>(&mut self, message_module: M) {
        let module_id = M::id();

        if let Some(_) = self.message_modules.insert(module_id.clone(), message_module.receiver()) {
            warn!("replacing a second message module for module id {:?}", module_id)
        }
    }

    pub fn cancel_recv(&self) {
        match self.cancel_sender.send(()) {
            Ok(_) => info!("canceling receive loop"),
            Err(_) => debug!("call to cancel_recv while no receive loop is running"),
        }
    }

    #[tracing::instrument]
    pub async fn recv(&self) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(self.myself.addr).await?;
        let mut buf: [u8; MAX_MSG_SIZE] = [0; MAX_MSG_SIZE];

        let mut cancel_receiver = self.cancel_sender.subscribe();

        trace!(addr = ?self.myself, "starting receive loop");

        loop {
            tokio::select! {
                r = socket.recv_from(&mut buf) => {
                    match r {
                        Ok((len, from)) => {
                            self.handle_received(&buf[..len], from);
                        }
                        Err(e) => {
                            error!(error = ?e, "error receiving from datagram socket");
                            return Err(e.into()); //TODO error handling
                        }
                    }
                }
                _ = cancel_receiver.recv() => break,
            }
        }

        Ok(())
    }

    fn handle_received(&self, msg_buf: &[u8], from: SocketAddr) {
        debug!("received message");
        //TODO trace raw message

        if msg_buf.len() == MAX_MSG_SIZE {
            warn!("received a message exceeding max message size of {} bytes - skipping", MAX_MSG_SIZE);
            return;
        }

        let mut msg_buf = msg_buf;
        match Envelope::try_read(&mut msg_buf, from, self.myself.addr) {
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


    pub async fn send<M: MessageModule>(&self, to: &NodeAddr, msg_module: &M, msg: &M::Message) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        Envelope::write(&self.myself, to, M::id(), &mut buf);
        msg_module.ser(msg, &mut buf);

        //TODO reuse sending sockets
        //TODO batch messages (?)

        let socket = UdpSocket::bind(to.addr).await?;
        socket.send_to(&buf, to.addr).await?;

        Ok(())
    }
}


struct Envelope {
    from: NodeAddr,
    to: NodeAddr,
    message_module_id: MessageModuleId,
}
impl Envelope {
    const ADDR_SIZE: usize = size_of::<u32>();
    const ENVELOPE_SIZE: usize = 2*Self::ADDR_SIZE + size_of::<MessageModuleId>();

    fn try_read(buf: &mut impl Buf, from: SocketAddr, to: SocketAddr) -> anyhow::Result<Envelope> {
        if buf.remaining() < Self::ENVELOPE_SIZE {
            return Err(anyhow!("message is shorter than envelope size: {} < {} bytes", buf.remaining(), Self::ENVELOPE_SIZE));
        }

        let from = Self::read_addr(buf, from)?;
        let to = Self::read_addr(buf, to)?;

        let message_module_id = buf.get_u64_le();

        Ok(Envelope {
            from,
            to,
            message_module_id: MessageModuleId(message_module_id),
        })
    }

    fn read_addr(buf: &mut impl Buf, socket_addr: SocketAddr) -> anyhow::Result<NodeAddr> {
        let unique = buf.get_u32_le();
        Ok(NodeAddr {
            unique,
            addr: socket_addr,
        })
    }

    fn write(from: &NodeAddr, to: &NodeAddr, message_module_id: MessageModuleId, buf: &mut BytesMut) {
        buf.put_u32_le(from.unique);
        buf.put_u32_le(to.unique);
        buf.put_u64_le(message_module_id.0);
    }
}



