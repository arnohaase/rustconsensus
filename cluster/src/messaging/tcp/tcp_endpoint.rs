use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, instrument, warn, Instrument, Span};
use transport::buffers::atomic_map::AtomicMap;
use transport::message_dispatcher::MessageDispatcher;
use crate::messaging::message_module::Message;
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::tcp::encryption::Encryption;
use crate::messaging::tcp::tcp_receiver::TcpReceiver;
use crate::messaging::tcp::tcp_sender::TcpSender;

struct TcpEndpoint<M: MessageDispatcher> {
    server_socket: TcpListener,
    encryption: Arc<dyn Encryption>,
    self_addr: NodeAddr,
    message_dispatcher: Arc<M>,
    senders: Arc<AtomicMap<NodeAddr, Arc<TcpSender>>>,
}

impl <M: MessageDispatcher> TcpEndpoint<M> {
    pub async fn new(self_addr: NodeAddr, encryption: Arc<dyn Encryption>, message_dispatcher: Arc<M>) -> anyhow::Result<TcpEndpoint<M>> {
        Ok(TcpEndpoint {
            server_socket: TcpListener::bind(self_addr.socket_addr).await?,
            encryption,
            self_addr,
            message_dispatcher,
            senders: Arc::new(AtomicMap::new()),
        })
    }

    pub async fn send_message<MSG: Message>(&self, to: NodeAddr, message: &MSG) -> anyhow::Result<()> {

        // message.module_id()






        todo!()
    }

    pub async fn accept(&self) -> anyhow::Result<()>{
        loop {
            let (stream, addr) = self.server_socket.accept().await?;

            let encryption = self.encryption.clone();
            let self_addr = self.self_addr.clone();
            let message_dispatcher = self.message_dispatcher.clone();
            tokio::spawn(async move { //TODO termination: we shut down, clean up when they shut down, ...
                match handle_connection_request(addr, stream, encryption, self_addr, message_dispatcher).await {
                    Ok(_) => {}
                    Err(e) => warn!("connection from {} broke with an error: {}", addr, e),
                }
            });
        }
    }
}

#[instrument(name="accepted_connection", skip_all, fields(addr = format!("{:?}", _addr)))]
async fn handle_connection_request <M: MessageDispatcher> (
    _addr: SocketAddr,
    stream: TcpStream,
    encryption: Arc<dyn Encryption>,
    self_addr: NodeAddr,
    message_dispatcher: Arc<M>,
) -> anyhow::Result<()> {
    debug!("received connection request");
    let tcp_receiver = TcpReceiver::new(stream, encryption, self_addr)
        .instrument(Span::current())
        .await?;
    debug!("initial handshake complete");
    tcp_receiver.receive_loop(message_dispatcher)
        .instrument(Span::current())
        .await?;
    debug!("connection closed");
    Ok(())
}