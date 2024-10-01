use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;

use bytes::BytesMut;
use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

use crate::comm::envelope::Envelope;
use crate::comm::message_module::{MessageModule, MessageModuleId, MessageModuleReceiver};
use crate::node_addr::NodeAddr;


const MAX_MSG_SIZE: usize = 16384; //TODO make this configurable

pub struct UdpTransport {
    myself: NodeAddr,
    message_modules: FxHashMap<MessageModuleId, Box<dyn MessageModuleReceiver>>,
    cancel_sender: broadcast::Sender<()>,
    ipv4_send_socket: UdpSocket,
    ipv6_send_socket: UdpSocket,
}

impl Debug for UdpTransport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpTransport{{myself:{:?}}}", &self.myself)
    }
}

impl UdpTransport {
    pub async fn new(myself: NodeAddr) -> anyhow::Result<UdpTransport> {
        let (cancel_sender, _) = broadcast::channel(1);

        let ipv4_send_socket = UdpSocket::bind(SocketAddr::from_str("0.0.0.0:0")?).await?;
        let ipv6_send_socket = UdpSocket::bind(SocketAddr::from_str("[::]:0")?).await?;

        Ok(UdpTransport {
            myself,
            message_modules: Default::default(),
            cancel_sender,
            ipv4_send_socket,
            ipv6_send_socket,
        })
    }

    pub fn get_addr(&self) -> NodeAddr {
        self.myself
    }

    pub fn register_message_module<M: MessageModule>(&mut self, message_module: M) {
        if let Some(_) = self.message_modules.insert(M::id(), message_module.receiver()) {
            warn!("registering a second message module for module id {:?}, replacing the first", M::id())
        }
    }

    pub async fn send<M: MessageModule>(&self, to: NodeAddr, msg_module: &M, msg: &M::Message) -> anyhow::Result<()> {
        debug!(from=?self.myself, ?to, "sending message");

        let mut buf = BytesMut::new();
        Envelope::write(self.myself, to, M::id(), &mut buf);
        msg_module.ser(msg, &mut buf);

        //TODO message batching (?)

        let socket = if to.addr.is_ipv4() { &self.ipv4_send_socket } else { &self.ipv6_send_socket };
        socket.send_to(&buf, to.addr).await?;

        Ok(())
    }

    #[tracing::instrument]
    pub async fn recv(&self) -> anyhow::Result<()> {
        match self._recv().await {
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

    async fn _recv(&self) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(self.myself.addr).await?;
        let mut buf: [u8; MAX_MSG_SIZE] = [0; MAX_MSG_SIZE];

        let mut cancel_receiver = self.cancel_sender.subscribe();

        trace!("starting receive loop");

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
        //TODO safeguard against panics

        debug!("received message");
        trace!(?msg_buf);
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
}
