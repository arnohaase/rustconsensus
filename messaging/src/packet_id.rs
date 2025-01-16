use std::fmt::{Display, Formatter};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PacketId(u32);

impl Display for PacketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PacketId {
    pub fn from_raw(value: u32) -> Self {
        Self(value)
    }

    pub fn to_raw(&self) -> u32 {
        self.0
    }

    pub fn next(&self) -> PacketId {
        PacketId(self.0.wrapping_add(1))
    }

    pub fn minus(&self, other: u32) -> PacketId {
        PacketId(self.0.wrapping_sub(other))
    }
}

