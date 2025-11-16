use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, span, Instrument, Level};
use networkprotocol::config::RudpConfig;
use networkprotocol::end_point::EndPoint;
use networkprotocol::message_dispatcher::MessageDispatcher;

fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        // .with_max_level(Level::DEBUG)
        // .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let msg_dispatcher = Arc::new(SimpleMessageDispatcher::new());

    let addr_a: SocketAddr = SocketAddr::from_str("127.0.0.1:9100")?;
    let addr_b: SocketAddr = SocketAddr::from_str("127.0.0.1:9101")?;

    let a = Arc::new(EndPoint::new(
        msg_dispatcher.clone(),
        Arc::new(RudpConfig::default(addr_a, Some(vec![5u8;32].to_vec()))),
    ).await?);

    let b = Arc::new(EndPoint::new(
        msg_dispatcher.clone(),
        Arc::new(RudpConfig::default(addr_b, Some(vec![5u8;32].to_vec()))),
    ).await?);

    let cloned_a = a.clone();
    let cloned_b = b.clone();
    tokio::spawn(async move {
        let span = span!(Level::INFO, "node a");
        cloned_a.recv_loop().instrument(span).await
    });
    tokio::spawn(async move {
        let span = span!(Level::INFO, "node b");
        cloned_b.recv_loop().instrument(span).await
    });

    let mut msg = vec![];
    for _ in 0..1000 {
        for i in 0u8..255 {
            msg.push(i);
        }
    }
    let msg = Arc::new(msg);
    
    for _ in 0..10_000 {
        let a = a.clone();
        let msg = msg.clone();
        tokio::spawn(async move { 
            let span = span!(Level::INFO, "sending");
            a.send_in_stream(addr_b, None, 99, msg.as_ref()).instrument(span).await 
        });
    }
    
    sleep(Duration::from_secs(20)).await;

    Ok(())
}

struct SimpleMessageDispatcher {
    counter: AtomicUsize,
}
impl SimpleMessageDispatcher { 
    pub fn new() -> Self {
        SimpleMessageDispatcher {
            counter: AtomicUsize::default(),
        }
    }
}

#[async_trait::async_trait]
impl MessageDispatcher for SimpleMessageDispatcher {
    async fn on_message(&self, sender_addr: SocketAddr, sender_generation: u64,  stream_id: Option<u16>, msg_buf: Vec<u8>) {
        assert_eq!(1000 * 255, msg_buf.len());
        let c = self.counter.fetch_add(1, Ordering::AcqRel);
        
        info!("received message #{}: {:?} from {:?}@{} on stream {:?}", c, msg_buf.len(), sender_addr, sender_generation, stream_id);
    }
}
