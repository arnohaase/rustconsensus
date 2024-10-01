use std::net::SocketAddr;
use bytes::{Buf, BufMut};

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use bytes::BytesMut;

use rstest::rstest;
use log::info;
use tracing::Level;
use rustconsensus::comm::message_module::{MessageModule, MessageModuleId, MessageModuleReceiver};
use rustconsensus::comm::transport::UdpTransport;
use rustconsensus::node_addr::NodeAddr;


struct TestMessageModule {}
impl MessageModule for TestMessageModule {
    type Message = u32;

    fn id() -> MessageModuleId where Self: Sized {
        MessageModuleId::from("test")
    }

    fn receiver(&self) -> Box<dyn MessageModuleReceiver> {
        Box::new(TestMessageReceiver{})
    }

    fn ser(&self, msg: &Self::Message, buf: &mut impl BufMut) {
        buf.put_u32_le(*msg);
    }
}

struct TestMessageReceiver {}
impl MessageModuleReceiver for TestMessageReceiver {
    fn on_message(&self, buf: &[u8]) {}
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

async fn create_transport(addr: &str) -> Arc<UdpTransport> {
    let addr = NodeAddr::from(SocketAddr::from_str(addr).unwrap());
    let mut transport = UdpTransport::new(addr).await.unwrap();
    transport.register_message_module(TestMessageModule{});
    Arc::new(transport)
}

#[tokio::main]
pub async fn main() {
    init_logging();

    let t1 = create_transport("127.0.0.1:9810").await;
    let t2 = create_transport("127.0.0.1:9811").await;

    let start = SystemTime::now();

    tokio::select!(
        a = t1.recv() => {}
        b = t2.recv() => {}
        _ = async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for i in 0u32..10 {
                t1.send(t2.get_addr(), &TestMessageModule{}, &i).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        } => {}
    );

    let duration = SystemTime::elapsed(&start).unwrap();
    println!("duration: {:?}", duration)
}
