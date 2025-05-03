use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use crate::messaging::udp::udp_config::UdpConfig;
use crate::messaging::udp::udp_encryption::{Aes256GcmEncryption, NoEncryption, UdpEncryption};
use crate::messaging::udp::udp_message_header::UdpMessageHeader;
use async_trait::async_trait;
use bytes::BytesMut;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use crate::util::atomic_map::AtomicMap;
use crate::util::safe_converter::PrecheckedCast;

pub struct UdpMessaging {
    message_modules: Arc<AtomicMap<MessageModuleId, Arc<dyn MessageModule>>>,
    self_addr: NodeAddr,
    receive_socket: UdpSocket,
    ipv4_send_socket: Arc<UdpSocket>,
    ipv6_send_socket: Arc<UdpSocket>,
    encryption: Arc<dyn UdpEncryption>,
    sequence_numbers: RwLock<FxHashMap<SocketAddr, AtomicU64>>,
}
impl Debug for UdpMessaging {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpMessaging")
    }
}

impl UdpMessaging {
    pub async fn new(config: &UdpConfig) -> anyhow::Result<UdpMessaging> {
        let receive_socket = UdpSocket::bind(config.self_addr).await?;

        let ipv4_send_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        let ipv6_send_socket = Arc::new(UdpSocket::bind("[::]:0").await?);

        let encryption: Arc<dyn UdpEncryption> = match &config.encryption_key {
            None => Arc::new(NoEncryption {}),
            Some(key) => Arc::new(Aes256GcmEncryption::new(key.as_slice())),
        };

        Ok(UdpMessaging {
            message_modules: Arc::new(Default::default()),
            self_addr: NodeAddr {
                unique: Self::generation_from_timestamp()?,
                socket_addr: config.self_addr,
            },
            receive_socket,
            ipv4_send_socket,
            ipv6_send_socket,
            encryption,
            sequence_numbers: Default::default(),
        })
    }

    fn generation_from_timestamp() -> anyhow::Result<u64> {
        let raw = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();

        if raw > 0xffff_ffff_ffff {
            anyhow::bail!("system clock is way in the past");
        }
        Ok(raw.prechecked_cast())
    }

    async fn next_sequence_number(&self, to: SocketAddr) -> u64 {
        {
            // trying with a read lock first is an optimization for the common case
            if let Some(atomic_counter) = self.sequence_numbers.read().await
                .get(&to)
            {
                return atomic_counter.fetch_add(1, Ordering::AcqRel);
            }
        }
        
        let mut sequence_numbers = self.sequence_numbers.write().await;
        // we need to check again now that we have the exclusive lock to avoid racy initialization
        if let Some(atomic_counter) = sequence_numbers.get(&to) {
            return atomic_counter.fetch_add(1, Ordering::AcqRel);
        }
        
        sequence_numbers.insert(to, AtomicU64::new(1));
        0
    }

    async fn do_send<T: Message>(&self, to_addr: SocketAddr, to_unique: Option<u64>, msg: &T) -> anyhow::Result<()> {
        let socket = if to_addr.is_ipv4() {
            self.ipv4_send_socket.as_ref()
        }
        else {
            self.ipv6_send_socket.as_ref()
        };

        //TODO capacity?
        let mut buf = BytesMut::new(); //TODO reuse, pooling?

        UdpMessageHeader {
            sender_addr: self.self_addr,
            recipient_unique_part: to_unique,
            sequence_number: self.next_sequence_number(to_addr).await,
            message_module_id: msg.module_id(),
        }.ser(&mut buf);

        msg.ser(&mut buf);

        self.encryption.encrypt_buffer(&mut buf);

        socket.send_to(buf.as_ref(), to_addr).await?;
        Ok(())
    }
    
    async fn do_on_raw_message(&self, message: BytesMut) {
        if let Err(e) = self._do_on_raw_message(message).await {
            error!("error unwrapping message: {}", e);
        }
    }
    
    async fn _do_on_raw_message(&self, mut buf: BytesMut) -> anyhow::Result<()> {
        self.encryption.decrypt_buffer(&mut buf)?; //TODO error message
        
        let mut decrypted = buf.as_ref();
        let parse_buf = &mut decrypted;
        let header = UdpMessageHeader::deser(parse_buf)?;
        
        //TODO deduplication
        
        if let Some(message_module) = self.get_message_module(header.message_module_id) {
            message_module.on_message(header.sender_addr, parse_buf).await;
        }
        else {
            warn!("received message for message module {:?} which is not registered - skipping message", header.message_module_id);
        }
        
        Ok(())
    }
    
    fn get_message_module(&self, id: MessageModuleId) -> Option<Arc<dyn MessageModule>> {
        self.message_modules.get(&id)
    }
}

#[async_trait]
impl MessageSender for UdpMessaging {
    fn get_self_addr(&self) -> NodeAddr {
        self.self_addr
    }

    async fn send_to_node<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.do_send(to.socket_addr, Some(to.unique), msg).await
    }

    async fn send_to_addr<T: Message>(&self, to: SocketAddr, msg: &T) -> anyhow::Result<()> {
        self.do_send(to, None, msg).await
    }
}

#[async_trait]
impl Messaging for UdpMessaging {
        fn register_module(&self, message_module: Arc<dyn MessageModule>) {
            self.message_modules
                .ensure_init(message_module.id(), || message_module.clone());
        }
    
        fn deregister_module(&self, id: MessageModuleId) {
            self.message_modules
                .remove_all(|k| k == &id);
        }

    async fn recv(&self) {
        loop {
            let mut buf = BytesMut::new();
            match self.receive_socket.recv_buf(&mut buf).await {
                Ok(n) => {
                    debug!("received raw message, len {}", n);
                    self.do_on_raw_message(buf).await;
                }
                Err(e) => {
                    error!("error receiving raw message: {}", e);
                }
            }
        }
    }
}