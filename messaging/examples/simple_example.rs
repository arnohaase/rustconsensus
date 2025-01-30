use messaging::end_point::EndPoint;
use messaging::message_dispatcher::MessageDispatcher;
use messaging::receive_stream::ReceiveStreamConfig;
use messaging::send_stream::SendStreamConfig;
use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let msg_dispatcher = Arc::new(SimpleMessageDispatcher {});

    let receive_config = Arc::new(ReceiveStreamConfig {
        nak_interval: Duration::from_millis(2),
        sync_interval: Duration::from_secs(1),
        receive_window_size: 1024,
        max_num_naks_per_packet: 128,
    });

    let send_config = Arc::new(SendStreamConfig {
        max_packet_len: 1400,
        late_send_delay: Some(Duration::from_micros(10)),
        send_window_size: 1024,
    });

    let addr_a: SocketAddr = SocketAddr::from_str("127.0.0.1:9100")?;
    let addr_b: SocketAddr = SocketAddr::from_str("127.0.0.1:9101")?;

    let a = Arc::new(EndPoint::new(
        addr_a,
        msg_dispatcher.clone(),
        receive_config.clone(),
        FxHashMap::default(),
        send_config.clone(),
        FxHashMap::default(),
    ).await?);

    let b = Arc::new(EndPoint::new(
        addr_b,
        msg_dispatcher.clone(),
        receive_config.clone(),
        FxHashMap::default(),
        send_config,
        FxHashMap::default(),
    ).await?);

    let cloned_a = a.clone();
    let cloned_b = b.clone();
    tokio::spawn(async move { cloned_a.recv_loop().await });
    tokio::spawn(async move { cloned_b.recv_loop().await });




    Ok(())
}

struct SimpleMessageDispatcher {}
#[async_trait::async_trait]
impl MessageDispatcher for SimpleMessageDispatcher {
    async fn on_message(&self, sender: SocketAddr, stream_id: Option<u16>, msg_buf: &[u8]) {
        info!("received message from {:?} on stream {:?}", sender, stream_id);
    }
}
