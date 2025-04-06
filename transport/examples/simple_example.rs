use transport::end_point::EndPoint;
use transport::message_dispatcher::MessageDispatcher;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, span, Instrument, Level};
use transport::config::RudpConfig;

fn init_logging() {
    tracing_subscriber::fmt()
        // .with_max_level(Level::INFO)
        // .with_max_level(Level::DEBUG)
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let msg_dispatcher = Arc::new(SimpleMessageDispatcher {});

    let addr_a: SocketAddr = SocketAddr::from_str("127.0.0.1:9100")?;
    let addr_b: SocketAddr = SocketAddr::from_str("127.0.0.1:9101")?;

    let span = span!(Level::INFO, "node a");
    let a = Arc::new(EndPoint::new(
        addr_a,
        msg_dispatcher.clone(),
        RudpConfig::default_ipv4(Some(vec![5u8;32])),
    ).instrument(span).await?);

    let span = span!(Level::INFO, "node b");
    let b = Arc::new(EndPoint::new(
        addr_b,
        msg_dispatcher.clone(),
        RudpConfig::default_ipv4(Some(vec![5u8;32])),
    ).instrument(span).await?);

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

    a.send_message(addr_b, 4, &[1, 2, 3]).await;
    a.send_message(addr_b, 4, &[2, 3, 4, 5]).await;
    a.send_message(addr_b, 4, &[7]).await;
    a.send_message(addr_b, 4, &[4, 5, 6]).await;

    sleep(Duration::from_millis(20)).await;

    Ok(())
}

struct SimpleMessageDispatcher {}
#[async_trait::async_trait]
impl MessageDispatcher for SimpleMessageDispatcher {
    async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, msg_buf: &[u8]) {
        info!("received message {:?} from {:?} on stream {:?}", msg_buf, sender, stream_id);
    }
}
