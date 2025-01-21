use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Sub};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PacketId(u64);

impl Display for PacketId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PacketId {
    pub const ZERO: PacketId = PacketId(0);
    pub const MAX: PacketId = PacketId(u64::MAX);

    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    pub fn to_raw(&self) -> u64 {
        self.0
    }

    pub fn to(self, end: PacketId) -> PacketIdIterator {
        PacketIdIterator {
            current: self,
            end,
        }
    }
}

impl Add<u64> for PacketId {
    type Output = PacketId;

    fn add(self, rhs: u64) -> Self::Output {
        PacketId(self.0 + rhs)
    }
}

impl AddAssign<u64> for PacketId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for PacketId {
    type Output = Option<PacketId>;

    fn sub(self, rhs: u64) -> Option<PacketId> {
        self.0.checked_sub(rhs)
            .map(PacketId)
    }
}

pub struct PacketIdIterator {
    current: PacketId,
    end: PacketId,
}

impl Iterator for PacketIdIterator {
    type Item = PacketId;

    fn next(&mut self) -> Option<PacketId> {
        self.current += 1;
        if self.current < self.end {
            Some(self.current)
        }
        else {
            None
        }
    }
}