use crate::messaging::node_addr::NodeAddr;
use crate::messaging::tcp::encryption::{Encryption, Nonce};
use crate::messaging::tcp::tcp_control_messages::{InitMsg, InitResponseMsg};
use anyhow::bail;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use transport::safe_converter::PrecheckedCast;


/// A TCP connection to a peer, used in simplex mode
pub struct TcpSender {
    stream: TcpStream,
    peer_addr: NodeAddr,
    encryption: Arc<dyn Encryption>,
    nonce: Nonce,
}

impl TcpSender {
    pub async fn new(peer_addr: SocketAddr, self_addr: NodeAddr, encryption: Arc<dyn Encryption>) -> anyhow::Result<TcpSender> {
        let mut stream = TcpStream::connect(peer_addr).await?;

        let nonce = Nonce::new_random();
        
        send_init(&mut stream, self_addr, &nonce).await?;
        let init_response = read_init_response(&mut stream).await?;

        Ok(TcpSender {
            stream,
            peer_addr: NodeAddr {
                unique: init_response.peer_unique,
                socket_addr: peer_addr,
            },
            encryption,
            nonce,
        })
    }

    pub async fn send_message(&mut self, buf: &mut BytesMut) -> anyhow::Result<()> {
        if buf.len() > 16*1024*1024 { //TODO make this configurable
            bail!("message longer than configured max length");
        }

        self.nonce.increment();
        self.encryption.encrypt_in_place(&self.nonce, buf);

        let buf_len: u32 = buf.len().prechecked_cast();
        self.stream.write_all(buf_len.to_be_bytes().as_ref()).await?;
        self.stream.write_all(buf).await?;

        Ok(())
    }
}

async fn send_init(stream: &mut TcpStream, self_addr: NodeAddr, nonce: &Nonce) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();

    InitMsg {
        self_addr,
        nonce: nonce.clone(),
    }
        .ser(&mut buf);

    let buf_len: u32 = buf.len().prechecked_cast();
    stream.write_all(buf_len.to_be_bytes().as_ref()).await?;
    stream.write_all(&buf).await?;
    Ok(())
}

async fn read_init_response(stream: &mut TcpStream) -> anyhow::Result<InitResponseMsg> {
    let mut buf = [0u8; InitResponseMsg::SERIALIZED_LEN];
    stream.read_exact(&mut buf).await?;
    InitResponseMsg::deser(&mut buf.as_ref())
}
