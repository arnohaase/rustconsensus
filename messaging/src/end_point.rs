use std::collections::hash_map::Entry;
use std::sync::Arc;
use rustc_hash::FxHashMap;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tracing::{error, warn};
use crate::packet_header::{PacketHeader, PacketKind};
use crate::receive_stream::ReceiveStream;

struct EndPoint {
    receive_socket: Arc<UdpSocket>,
    send_socket_v4: Arc<UdpSocket>,
    send_socket_v6: Arc<UdpSocket>,
    receive_streams: FxHashMap<u16, Arc<ReceiveStream>>,
}
impl EndPoint {
    async fn new(addrs: impl ToSocketAddrs) -> anyhow::Result<EndPoint> {
        //TODO "don't fragment" flag
        let receive_socket = Arc::new(UdpSocket::bind(addrs).await?);
        let (send_socket_v4, send_socket_v6) = if receive_socket.local_addr()?.is_ipv6() {
            (Arc::new(UdpSocket::bind("0.0.0.0:0")), receive_socket.clone())
        }
        else {
            (receive_socket.clone(), Arc::new(UdpSocket::bind("[::]:0")))
        };

        Ok(EndPoint {
            receive_socket,
            send_socket_v4,
            send_socket_v6,
            receive_streams: Default::default(),
        })
    }

    async fn recv_loop(&self)  {
        let mut buf = [0u8; 1500]; //TODO configurable size
        loop {
            let (num_read, from) = match self.receive_socket.recv_from(&mut buf).await {
                Ok(x) => {
                    x
                }
                Err(e) => {
                    error!("socket error: {}", e);
                    continue;
                }
            };

            let parse_buf = &mut &buf[..num_read];
            let packet_header = match PacketHeader::deser(parse_buf) {
                Ok(header) => {
                    header
                },
                Err(e) => {
                    warn!("received packet with unparsable header from {:?}, dropping", from);
                    continue;
                },
            };

            match packet_header.packet_kind {
                PacketKind::RegularSequenced { stream_id, .. } => { todo!() }
                PacketKind::OutOfSequence => { todo!() }
                PacketKind::ControlInit { stream_id } => { todo!() }
                PacketKind::ControlSync { stream_id } => { todo!() }
                PacketKind::ControlNak { stream_id } => { todo!() }
            }
        }
    }

    fn get_receive_stream(&mut self, stream_id: u16) -> Arc<ReceiveStream> {
        match self.receive_streams.entry(stream_id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Arc::new(ReceiveStream::new())).clone()
        }
    }

    async fn handle_init_message(&self, header: PacketHeader, stream_id: u16) {

    }
}