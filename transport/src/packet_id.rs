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

    fn add(self, rhs: u64) -> PacketId {
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
        let result = if self.current < self.end {
            Some(self.current)
        }
        else {
            None
        };
        self.current += 1;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case(0, 0, vec![])]
    #[case(0, 1, vec![0])]
    #[case(0, 2, vec![0, 1])]
    #[case(9, 5, vec![])]
    #[case(10, 20, vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19])]
    fn test_to(#[case] from: u64, #[case] to: u64, #[case] expected: Vec<u64>) {
        let expected = expected.iter().map(|&x| PacketId(x)).collect::<Vec<_>>();

        let actual = PacketId(from).to(PacketId(to))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(0, 0, 0)]
    #[case(0, 1, 1)]
    #[case(1, 0, 1)]
    #[case(4, 5, 9)]
    #[case(u64::MAX, 0, u64::MAX)]
    #[case(0, u64::MAX, u64::MAX)]
    fn test_add(#[case] a: u64, #[case] b: u64, #[case] expected: u64) {
        let mut p = PacketId(a);
        let actual = p + b;
        assert_eq!(actual, PacketId(expected));
        assert_eq!(p, PacketId(a));

        p += b;
        assert_eq!(p, PacketId(expected));
    }

    #[rstest]
    #[case(0, 0, Some(0))]
    #[case(0, 1, None)]
    #[case(1, 0, Some(1))]
    #[case(1, 1, Some(0))]
    #[case(u64::MAX, 0, Some(u64::MAX))]
    #[case(u64::MAX, u64::MAX, Some(0))]
    #[case(0, u64::MAX, None)]
    #[case(10, 9, Some(1))]
    #[case(99, 100, None)]
    fn test_sub(#[case] a: u64, #[case] b: u64, #[case] expected: Option<u64>) {
        let expected = expected.map(PacketId);
        let actual = PacketId(a) - b;
        assert_eq!(actual, expected);
    }
}
