// use bytes::Bytes;
// use std::net::Shutdown;
// use std::sync::Arc;
// use std::time::Duration;
// use tokio::net::UdpSocket;
// use tokio::signal;
// use tokio::sync::mpsc;
// use webrtc::sctp::association::{Association, Config};
// use webrtc::sctp::chunk::chunk_payload_data::PayloadProtocolIdentifier;
// use webrtc::sctp::stream::ReliabilityType;
// use webrtc::sctp::Error;
// use webrtc::util::conn::conn_disconnected_packet::DisconnectedPacketConn;
// use webrtc::util::Conn;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // tokio::spawn(server());
    //
    // client().await?;
    Ok(())
}

// async fn server() -> anyhow::Result<()> {
//     let conn = DisconnectedPacketConn::new(Arc::new(UdpSocket::bind("localhost:5678").await.unwrap()));
//
//     let config = Config {
//         net_conn: Arc::new(conn),
//         max_receive_buffer_size: 0,
//         max_message_size: 0,
//         name: "server".to_owned(),
//     };
//     let a = Association::server(config).await?;
//     println!("SSS      created a server");
//
//     let stream = a.accept_stream().await.unwrap();
//     println!("SSS      accepted a stream");
//
//     // set unordered = true and 10ms threshold for dropping packets
//     stream.set_reliability_params(true, ReliabilityType::Timed, 10);
//
//     let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
//     let stream2 = Arc::clone(&stream);
//     tokio::spawn(async move {
//         let mut buff = vec![0u8; 1024];
//         while let Ok(n) = stream2.read(&mut buff).await {
//             let ping_msg = String::from_utf8(buff[..n].to_vec()).unwrap();
//             println!("SSS      received: {ping_msg}");
//
//             let pong_msg = format!("pong [{ping_msg}]");
//             println!("SSS      sent: {pong_msg}");
//             stream2.write(&Bytes::from(pong_msg)).await?;
//
//             tokio::time::sleep(Duration::from_secs(1)).await;
//         }
//         println!("SSS      finished ping-pong");
//         drop(done_tx);
//
//         Result::<(), Error>::Ok(())
//     });
//
//     println!("SSS      Waiting for Ctrl-C...");
//     signal::ctrl_c().await.expect("failed to listen for event");
//     println!("SSS      Ctrl-C: Closing stream and association...");
//
//     stream.shutdown(Shutdown::Both).await?;
//     a.close().await?;
//
//     let _ = done_rx.recv().await;
//
//     Ok(())
// }
//
// async fn client() -> anyhow::Result<()> {
//     let conn = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
//     conn.connect("localhost:5678").await.unwrap();
//
//     let config = Config {
//         net_conn: conn,
//         max_receive_buffer_size: 0,
//         max_message_size: 0,
//         name: "client".to_owned(),
//     };
//     let a = Association::client(config).await?;
//     println!("CCC created a client");
//
//     let stream = a.open_stream(0, PayloadProtocolIdentifier::String).await?;
//     println!("CCC opened a stream");
//
//     // set unordered = true and 10ms threshold for dropping packets
//     stream.set_reliability_params(true, ReliabilityType::Timed, 10);
//
//     let stream_tx = Arc::clone(&stream);
//     tokio::spawn(async move {
//         let mut ping_seq_num = 0;
//         while ping_seq_num < 3 {
//             let ping_msg = format!("ping {ping_seq_num}");
//             println!("CCC sent: {ping_msg}");
//             stream_tx.write(&Bytes::from(ping_msg)).await.unwrap();
//
//             ping_seq_num += 1;
//         }
//
//         println!("CCC finished send ping");
//         // Result::<(), Error>::Ok(())
//     });
//
//     let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
//     let stream_rx = Arc::clone(&stream);
//     tokio::spawn(async move {
//         let mut buff = vec![0u8; 1024];
//         loop {
//             match stream_rx.read(&mut buff).await {
//                 Err(e) => {
//                     println!("error: {}", e);
//                     break;
//                 }
//                 Ok(0) => {
//                     println!("CCC finished recv pong");
//                     drop(done_tx);
//                     break;
//                 }
//                 Ok(n) => {
//                     let pong_msg = String::from_utf8(buff[..n].to_vec()).unwrap();
//                     println!("CCC received: {pong_msg}");
//                 }
//             }
//         }
//     });
//
//     println!("CCC Waiting for Ctrl-C...");
//     signal::ctrl_c().await.expect("failed to listen for event");
//     println!("CCC Ctrl-C: Closing stream and association...");
//
//     stream.shutdown(Shutdown::Both).await?;
//     println!("CCC closed stream");
//     a.close().await?;
//     println!("CCC closed association");
//
//     let _ = done_rx.recv().await;
//
//     Ok(())
// }
//
