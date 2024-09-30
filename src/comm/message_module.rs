use std::fmt::{Debug, Formatter};
use bytes::BufMut;

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

        write!(f, "{:X}({:?}", self.0, string_repr)
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

pub trait MessageModule: 'static {
    type Message;

    fn id() -> MessageModuleId where Self: Sized;

    fn receiver(&self) -> Box<dyn MessageModuleReceiver>;

    fn ser(&self, msg: &Self::Message, buf: &mut impl BufMut);
}

pub trait MessageModuleReceiver: 'static {
    fn id() -> MessageModuleId where Self: Sized;

    /// called to handle a message that was received for this message module. It contains the
    ///  module specific message buffer, i.e. starting immediately *after* the module ID.
    ///
    /// This is a blocking call, holding up the central receive loop. Non-trivial work should
    ///  probably be offloaded to some asynchronous processing, but it is up to the module
    ///  implementation to decide and do this.
    fn on_message(&self, buf: &[u8]);
}
