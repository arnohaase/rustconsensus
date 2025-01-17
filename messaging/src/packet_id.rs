use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PacketId(u64);

impl Display for PacketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PacketId {
    pub const ZERO: PacketId = PacketId(0);

    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    pub fn to_raw(&self) -> u64 {
        self.0
    }

    pub fn next(&self) -> PacketId {
        PacketId(
            self.0.checked_add(1)
                .expect("TODO") //TODO
        )
    }

    pub fn checked_minus(&self, other: u64) -> Option<PacketId> {
        self.0.checked_sub(other).map(PacketId)
    }
}
