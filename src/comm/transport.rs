use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{error, trace, warn};

#[async_trait::async_trait]
pub trait Transport : Sync + Send {
    async fn send(&self, to: SocketAddr, buf: &[u8]) -> anyhow::Result<()>;

    async fn recv_loop(&self, handler: Arc<dyn MessageHandler>) -> anyhow::Result<()>;

    fn cancel_recv_loop(&self);
}

pub struct UdpTransport {
    self_addr: SocketAddr,
    cancel_sender: broadcast::Sender<()>,
    ipv4_send_socket: UdpSocket,
    ipv6_send_socket: UdpSocket,
}
impl UdpTransport {
    pub async fn new(self_addr: SocketAddr) -> anyhow::Result<UdpTransport> {
        let (cancel_sender, _) = broadcast::channel(1);

        let ipv4_send_socket = UdpSocket::bind(SocketAddr::from_str("0.0.0.0:0")?).await?;
        let ipv6_send_socket = UdpSocket::bind(SocketAddr::from_str("[::]:0")?).await?;

        Ok(UdpTransport {
            self_addr,
            cancel_sender,
            ipv4_send_socket,
            ipv6_send_socket,
        })
    }
}

pub trait MessageHandler : Sync + Send {
    fn handle_message(&self, buf: &[u8], sender: SocketAddr);
}

#[async_trait::async_trait]
impl Transport for UdpTransport {
    async fn send(&self, to: SocketAddr, buf: &[u8]) -> anyhow::Result<()> {
        let socket = if to.is_ipv4() { &self.ipv4_send_socket } else { &self.ipv6_send_socket };
        socket.send_to(&buf, to).await?;
        Ok(())
    }

    async fn recv_loop(&self, handler: Arc<dyn MessageHandler>) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(self.self_addr).await?;
        let mut buf: [u8; crate::comm::messaging::MAX_MSG_SIZE] = [0; crate::comm::messaging::MAX_MSG_SIZE]; //TODO

        let mut cancel_receiver = self.cancel_sender.subscribe();

        trace!("starting UDP receive loop");

        loop {
            tokio::select! {
                r = socket.recv_from(&mut buf) => {
                    match r {
                        Ok((len, from)) => {
                            handler.handle_message(&buf[..len], from);
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

    fn cancel_recv_loop(&self) {
        if let Err(err) = self.cancel_sender.send(()) {
            warn!(?err, "error canceling receive loop");
        }
    }
}
