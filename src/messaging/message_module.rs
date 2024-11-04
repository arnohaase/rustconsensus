use std::fmt::{Debug, Formatter};
use crate::messaging::envelope::Envelope;


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

// impl From<&str> for MessageModuleId {
//     fn from(value: &str) -> Self {
//         let bytes = value.as_bytes();
//         assert!(bytes.len() <= 8, "a message module id must have at most 8 bytes");
//         let mut buf = [0u8;8];
//         buf[..bytes.len()].copy_from_slice(bytes);
//         buf.into()
//     }
// }


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
    async fn on_message(&self, envelope: &Envelope, buf: &[u8]);
}

#[cfg(test)]
mod test {
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
