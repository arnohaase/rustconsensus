#![deny(warnings)]

use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{server::conn::http1, service::service_fn};
use hyper::{Error, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // pretty_env_logger::init();
    let addr: SocketAddr = ([127, 0, 0, 1], 3000).into();

    let counter = Arc::new(AtomicUsize::new(0));

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let counter = counter.clone();

        let service = service_fn (move |_req| {
            let count = counter.fetch_add(1, Ordering::AcqRel);
            async move {
                Ok::<_, Error>(Response::new(Full::new(Bytes::from(format!(
                    "Request #{}",
                    count
                )))))
            }
        });

        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            println!("Error serving connection: {:?}", err);
        }
    }
}