use crate::messaging::node_addr::NodeAddr;
use crate::messaging::tcp::encryption::{Encryption, Nonce};
use crate::messaging::tcp::tcp_control_messages::{InitMsg, InitResponseMsg};
use anyhow::bail;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use transport::message_dispatcher::MessageDispatcher;
use transport::safe_converter::SafeCast;

pub struct TcpReceiver {
    stream: TcpStream,

    encryption: Arc<dyn Encryption>,
    nonce: Nonce,
    peer_addr: NodeAddr,
}
impl TcpReceiver {
    pub async fn new(mut stream: TcpStream, encryption: Arc<dyn Encryption>, self_addr: NodeAddr) -> anyhow::Result<TcpReceiver> {
        let init_msg = read_init(&mut stream).await?;
        send_init_response(&mut stream, self_addr).await?;

        Ok(TcpReceiver {
            stream,
            encryption,
            nonce: init_msg.nonce,
            peer_addr: init_msg.self_addr,
        })
    }

    pub async fn receive_loop<M: MessageDispatcher>(mut self, message_dispatcher: Arc<M>) -> anyhow::Result<()> {
        let mut len_buf = [0u8; 4];

        loop {
            self.stream.read_exact(len_buf.as_mut()).await?; //TODO handle EOF as regular termination instead of propagating as an error
            let msg_len: usize = u32::from_be_bytes(len_buf).safe_cast();

            if msg_len > 16*1024*1024 { //TODO make this configurable
                bail!("received a message that was longer than the configured maximum ({}) - closing connection because it is apparently compromised", msg_len);
            }

            let mut msg_buf = vec![0u8; msg_len];
            self.stream.read_exact(&mut msg_buf[..msg_len]).await?;
            self.encryption.decrypt_in_place(&self.nonce, &mut msg_buf)?;

            //TODO clean up signature
            message_dispatcher.on_message(self.peer_addr.socket_addr, self.peer_addr.unique, None, msg_buf).await;
        }
    }
}


//TODO encryption for init / init response?
async fn read_init(stream: &mut TcpStream) -> anyhow::Result<InitMsg> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let msg_len: usize = u32::from_be_bytes(len_buf).safe_cast();

    let mut msg_buf = vec![0u8; msg_len];
    stream.read_exact(&mut msg_buf).await?;
    InitMsg::deser(&mut msg_buf.as_ref())
}

async fn send_init_response(stream: &mut TcpStream, self_addr: NodeAddr) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    InitResponseMsg {
        peer_unique: self_addr.unique,
    }
        .ser(&mut buf);

    stream.write_all(&buf).await?;
    Ok(())
}