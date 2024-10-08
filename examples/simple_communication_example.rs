use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::Level;
use rustconsensus::msg::envelope::Envelope;

use rustconsensus::msg::message_module::{MessageModule, MessageModuleId};
use rustconsensus::msg::messaging::Messaging;
use rustconsensus::msg::node_addr::NodeAddr;


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

async fn create_transport(addr: &str) -> Arc<Messaging> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let transport = Messaging::new(addr, vec![
        Arc::new(TestMessageModule{}),
    ]).await.unwrap();
    Arc::new(transport)
}

#[tokio::main]
pub async fn main() {
    init_logging();

    let t1 = create_transport("127.0.0.1:9810").await;
    let t2 = create_transport("127.0.0.1:9811").await;

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
        t1.send(t2.get_addr(), m.id(), &m.ser(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let duration = SystemTime::elapsed(&start).unwrap();
    println!("duration: {:?}", duration)
}
