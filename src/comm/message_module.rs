use std::fmt::{Debug, Formatter};


/// A [MessageModuleId] is sent as part of a message's envelope to identify the module for
///  deserialization and dispatch on the receiving side.
///
/// An id is technically a u64, but it is intended to be used as a sequence of up to eight ASCII
///  characters to give it a human-readable name, both for uniqueness and for debugging at the
///  wire level.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MessageModuleId(pub u64);

impl Debug for MessageModuleId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0.to_le_bytes();
        let used = bytes.iter()
            .position(|&b| b == 0)
            .map(|len| &bytes[..len])
            .unwrap_or(&bytes);

        let string_repr = std::str::from_utf8(used).unwrap_or("???");

        write!(f, "0x{:016X}({:?})", self.0, string_repr)
    }
}

impl From<&str> for MessageModuleId {
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        assert!(bytes.len() <= 8, "a message module id must have at most 8 bytes");
        let mut buf = [0u8;8];
        buf[..bytes.len()].copy_from_slice(bytes);
        buf.into()
    }
}

impl From<[u8;8]> for MessageModuleId {
    fn from(value: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(value)) //TODO check that network byte order keeps string representation
    }
}


/// Messages are pluggable, and they are organized in [MessageModule]s. Each module has its own
///  (single) type of messages, it takes care of serializing / deserializing them, and for handling
///  received messages.
///
/// Messages for a given module are identified in the envelope by a specific and (hopefully) unique
///  [MessageModuleId].
pub trait MessageModule: 'static {
    fn id(&self) -> MessageModuleId where Self: Sized;

    /// called to handle a message that was received for this message module. It contains the
    ///  module specific message buffer, i.e. starting immediately *after* the module ID.
    ///
    /// This is a blocking call, holding up the central receive loop. Non-trivial work should
    ///  probably be offloaded to some asynchronous processing, but it is up to the module
    ///  implementation to decide and do this.
    fn on_message(&self, buf: &[u8]);
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case::abc(MessageModuleId::from("abc"), "0x0000000000636261(\"abc\")")]
    #[case::empty(MessageModuleId::from(""), "0x0000000000000000(\"\")")]
    #[case::hex(MessageModuleId::from([1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8]), "0x0807060504030201(\"\\u{1}\\u{2}\\u{3}\\u{4}\\u{5}\\u{6}\\u{7}\\u{8}\")")]
    #[case::no_utf(MessageModuleId::from([0xff, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]), "0x00000000000000FF(\"???\")")]
    fn test_id_debug(#[case] id: MessageModuleId, #[case] expected: &str) {
        let formatted = format!("{:?}", id);
        assert_eq!(&formatted, expected);
    }
}
