//! Unbounded streaming send/recv types for the Large delivery path.
//!
//! These wrap `quinn::SendStream` / `quinn::RecvStream` and exist for two
//! reasons:
//!
//! 1. **Keep the transport pluggable.** `MessageModule::on_stream` and
//!    `MessageSender::open_large_stream` are part of the cross-transport
//!    `Messaging` API. Wrapping the quinn types means modules don't import
//!    `quinn::*` directly.
//! 2. **Make abort semantics safe by default.** `quinn::SendStream::Drop`
//!    calls `finish()` which would deliver a *truncated but clean* stream to
//!    the peer — silent corruption. `LargeSendStream::Drop` calls `reset(0)`
//!    instead unless `finish().await` or `cancel()` was invoked explicitly,
//!    so an accidental drop mid-transfer surfaces as a hard error on the
//!    receiver's next read.
//!
//! `LargeRecvStream` is a thin wrapper around `quinn::RecvStream`; quinn's
//! own `Drop` already calls `stop(0)` on an undrained stream, which is the
//! behavior we want, so no custom `Drop` is needed here.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use quinn::{RecvStream, SendStream, VarInt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::messaging::message_module::MessageModuleId;
use crate::messaging::node_addr::NodeAddr;

/// Application error code for "stream aborted by either side". `Drop`-based
/// cancellation and explicit `cancel()` / receiver `stop` all use this; we
/// don't carry richer information across the wire.
const ABORT_CODE: u32 = 0;

/// Outbound streaming half. Implements `tokio::io::AsyncWrite` so callers
/// can use `tokio::io::copy`, `write_all`, etc. To complete a transfer cleanly
/// the caller MUST `await` `finish()` — dropping without calling either
/// `finish()` or `cancel()` resets the stream and the receiver sees an error.
pub struct LargeSendStream {
    /// `None` once the stream has been consumed by `finish()` / `cancel()`.
    /// `Drop` checks this to decide whether it must reset the stream.
    stream: Option<SendStream>,
}

impl LargeSendStream {
    pub(crate) fn new(stream: SendStream) -> Self {
        Self {
            stream: Some(stream)
        }
    }

    /// Write a chunk to the stream. Returns the number of bytes written
    /// (may be a short write under back-pressure; use `tokio::io::AsyncWriteExt::write_all`
    /// for the all-or-error semantics).
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let stream = self.stream.as_mut().ok_or_else(stream_consumed)?;
        stream
            .write(buf)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    /// Finish the stream cleanly. The peer's `AsyncRead` will see EOF after
    /// it has drained all buffered bytes. Consumes `self` so a successful
    /// finish cannot be silently followed by a `Drop`-time reset.
    pub async fn finish(mut self) -> io::Result<()> {
        //NB: The implementation does not require the async keyword, but for API
        // symmetry reasons the function is async

        let mut stream = self
            .stream
            .take()
            .ok_or_else(stream_consumed)?;
        // quinn's `finish()` is sync — it just marks the stream as finished
        // locally. Waiting for the peer to ACK is `stopped()`; we don't
        // expose it because the caller's contract is "bytes are submitted",
        // not "bytes are acknowledged".
        stream
            .finish()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    /// Abort the stream. The peer's next read returns an error. Consumes
    /// `self` so `Drop` won't double-reset.
    pub fn cancel(mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.reset(VarInt::from_u32(ABORT_CODE));
        }
    }
}

impl AsyncWrite for LargeSendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(Err(stream_consumed()));
        };
        <SendStream as AsyncWrite>::poll_write(Pin::new(stream), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(Err(stream_consumed()));
        };
        <SendStream as AsyncWrite>::poll_flush(Pin::new(stream), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(Err(stream_consumed()));
        };
        <SendStream as AsyncWrite>::poll_shutdown(Pin::new(stream), cx)
    }
}

impl Drop for LargeSendStream {
    fn drop(&mut self) {
        // If the caller didn't reach `finish()` or `cancel()`, the stream is
        // half-open — quinn's default `Drop` would `finish()` it, delivering
        // a truncated-but-clean stream to the peer. Reset instead so the
        // peer sees an error on its next read.
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.reset(VarInt::from_u32(ABORT_CODE));
        }
    }
}

/// Inbound streaming half passed to `MessageModule::on_stream`. The 16-byte
/// transport header (sender unique + module id) has already been consumed by
/// the QUIC accept loop and is exposed via `sender()` / `module_id()`.
///
/// Implements `tokio::io::AsyncRead`; the underlying `quinn::RecvStream::Drop`
/// calls `stop(ABORT_CODE)` on any stream that wasn't fully drained, which
/// surfaces on the sender as a write/finish error.
pub struct LargeRecvStream {
    stream: RecvStream,
    sender: NodeAddr,
    module_id: MessageModuleId,
}

impl LargeRecvStream {
    pub(crate) fn new(stream: RecvStream, sender: NodeAddr, module_id: MessageModuleId) -> Self {
        Self { stream, sender, module_id }
    }

    pub fn sender(&self) -> NodeAddr {
        self.sender
    }
    pub fn module_id(&self) -> MessageModuleId {
        self.module_id
    }
}

impl AsyncRead for LargeRecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

fn stream_consumed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "large stream already consumed")
}
