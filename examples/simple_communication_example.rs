use std::any::Any;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use bytes::{BufMut, BytesMut};
use tracing::Level;
use rustconsensus::messaging::envelope::Envelope;

use rustconsensus::messaging::message_module::{Message, MessageModule, MessageModuleId};
use rustconsensus::messaging::messaging::{Messaging, MessagingImpl, MessageSender};
use rustconsensus::messaging::node_addr::NodeAddr;


struct TestMessageModule {
}
impl TestMessageModule {
    const ID: MessageModuleId = MessageModuleId::new(b"test\0\0\0\0");
}

#[async_trait::async_trait]
impl MessageModule for TestMessageModule {
    fn id(&self) -> MessageModuleId where Self: Sized {
        Self::ID
    }

    async fn on_message(&self, _envelope: &Envelope, _buf: &[u8]) {}
}

#[derive(Debug, Clone)]
pub struct TestMessage(pub u32);
impl Message for TestMessage {
    fn module_id(&self) -> MessageModuleId {
        TestMessageModule::ID
    }

    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u32(self.0);
    }

    fn box_clone(&self) -> Arc<dyn Any + Send + Sync + 'static> {
        Arc::new(self.clone())
    }
}


fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

async fn create_messaging(addr: &str) -> anyhow::Result<Arc<MessagingImpl>> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let messaging = MessagingImpl::new(addr, b"abc").await?;
    messaging.register_module(Arc::new(TestMessageModule{})).await?;

    Ok(Arc::new(messaging))
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    init_logging();

    let t1 = create_messaging("127.0.0.1:9810").await?;
    let t2 = create_messaging("127.0.0.1:9811").await?;

    let m = TestMessageModule{};

    let start = SystemTime::now();

    let t1_recv = t1.clone();
    let t2_recv = t2.clone();

    tokio::spawn(async move {tokio::select!(
        _ = t1_recv.recv() => {}
        _ = t2_recv.recv() => {}
    )});

    tokio::time::sleep(Duration::from_millis(500)).await;
    for i in 0u32..10 {
        t1.try_send(t2.get_self_addr(), &TestMessage(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let duration = SystemTime::elapsed(&start).unwrap();
    println!("duration: {:?}", duration);

    Ok(())
}
