use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::Level;
use rustconsensus::messaging::envelope::Envelope;

use rustconsensus::messaging::message_module::{MessageModule, MessageModuleId};
use rustconsensus::messaging::messaging::Messaging;
use rustconsensus::messaging::node_addr::NodeAddr;


struct TestMessageModule {
}
impl TestMessageModule {
    const ID: MessageModuleId = MessageModuleId::new(b"test\0\0\0\0");

    pub fn ser(&self, msg: u32) -> Vec<u8> {
        msg.to_le_bytes().to_vec()
    }
}

#[async_trait::async_trait]
impl MessageModule for TestMessageModule {
    fn id(&self) -> MessageModuleId where Self: Sized {
        Self::ID
    }

    async fn on_message(&self, _envelope: &Envelope, _buf: &[u8]) {}
}


fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

async fn create_messaging(addr: &str) -> anyhow::Result<Arc<Messaging>> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let messaging = Messaging::new(addr, b"abc").await?;
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
        t1.send(t2.get_self_addr(), m.id(), &m.ser(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let duration = SystemTime::elapsed(&start).unwrap();
    println!("duration: {:?}", duration);

    Ok(())
}
