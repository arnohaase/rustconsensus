use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use lazy_static::lazy_static;

use tracing::Level;

use rustconsensus::comm::message_module::{MessageModule, MessageModuleId};
use rustconsensus::comm::messaging::Messaging;
use rustconsensus::node_addr::NodeAddr;

lazy_static! {
    static ref ID: MessageModuleId = MessageModuleId::from("test");
}

struct TestMessageModule {
}
impl TestMessageModule {
    pub fn ser(&self, msg: u32) -> Vec<u8> {
        msg.to_le_bytes().to_vec()
    }
}

impl MessageModule for TestMessageModule {
    fn id(&self) -> MessageModuleId where Self: Sized {
        *ID
    }

    fn on_message(&self, _buf: &[u8]) {}
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

    tokio::select!(
        _ = t1.recv() => {}
        _ = t2.recv() => {}
        _ = async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for i in 0u32..10 {
                t1.send(t2.get_addr(), m.id(), &m.ser(i)).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        } => {}
    );

    let duration = SystemTime::elapsed(&start).unwrap();
    println!("duration: {:?}", duration)
}
