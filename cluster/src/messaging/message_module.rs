use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};
use tracing::warn;
use crate::messaging::large_stream::LargeRecvStream;
use crate::messaging::node_addr::NodeAddr;

pub trait Message: Send + Sync + Debug + Any {
    fn module_id(&self) -> MessageModuleId;
    fn ser(&self, buf: &mut BytesMut);

    fn box_clone(&self) -> Arc<dyn Any + Send + Sync + 'static>;
}


/// A [MessageModuleId] is sent as part of a message's envelope to identify the module for
///  deserialization and dispatch on the receiving side.
///
/// An id is technically a u64, but it is intended to be used as a sequence of up to eight ASCII
///  characters to give it a human-readable name, both for uniqueness and for debugging at the
///  wire level.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MessageModuleId(pub u64);

impl MessageModuleId {
    pub const fn new(value: &[u8; 8]) -> MessageModuleId {
        Self(u64::from_be_bytes(*value))
    }

    pub fn ser(&self, buf: &mut impl BufMut) { //TODO unit test
        buf.put_u64(self.0);
    }

    pub fn deser(buf: &mut impl Buf) -> anyhow::Result<MessageModuleId> {
        let id = buf.try_get_u64()?;
        Ok(MessageModuleId(id))
    }
}

impl Debug for MessageModuleId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0.to_be_bytes();
        let used = bytes.iter()
            .position(|&b| b == 0)
            .map(|len| &bytes[..len])
            .unwrap_or(&bytes);

        let string_repr = std::str::from_utf8(used).unwrap_or("???");

        write!(f, "0x{:016X}({:?})", self.0, string_repr)
    }
}

/// Messages are pluggable, and they are organized in [MessageModule]s. Each module has its own
///  (single) type of messages, it takes care of serializing / deserializing them, and for handling
///  received messages.
///
/// Messages for a given module are identified in the envelope by a specific and (hopefully) unique
///  [MessageModuleId].
#[async_trait::async_trait]
pub trait MessageModule: 'static + Sync + Send {
    fn id(&self) -> MessageModuleId;

    /// called to handle a message that was received for this message module. It contains the
    ///  module specific message buffer, i.e. starting immediately *after* the module ID.
    ///
    /// This is a blocking call, holding up the central receive loop. Non-trivial work should
    ///  probably be offloaded to some asynchronous processing, but it is up to the module
    ///  implementation to decide and do this.
    async fn on_message(&self, sender: NodeAddr, buf: &[u8]);

    /// Called when a peer opens a large streaming transfer addressed to this
    /// module (see `MessageSender::open_large_stream`). The default
    /// implementation logs a warning and drops the stream, which causes the
    /// underlying `quinn::RecvStream::Drop` to send `stop(0)` so the sender's
    /// next write/finish errors out — modules that don't override this are
    /// therefore safe by default.
    ///
    /// Implementations MUST drive the stream to completion (e.g. via
    /// `tokio::io::AsyncReadExt::read_to_end`) or drop it; holding it idle
    /// will pin a uni-stream slot on the connection.
    async fn on_stream(&self, _sender: NodeAddr, stream: LargeRecvStream) {
        warn!(
            module_id = ?self.id(),
            sender = ?stream.sender(),
            "MessageModule received a streamed message but did not override on_stream; dropping it."
        );
        drop(stream);
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case::abc(MessageModuleId::new(b"abc\0\0\0\0\0"), "0x6162630000000000(\"abc\")")]
    #[case::empty(MessageModuleId::new(b"\0\0\0\0\0\0\0\0"), "0x0000000000000000(\"\")")]
    fn test_id_debug(#[case] id: MessageModuleId, #[case] expected: &str) {
        let formatted = format!("{:?}", id);
        assert_eq!(&formatted, expected);
    }
}
