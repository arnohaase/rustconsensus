//! We use value-based CRDTs (see https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type),
//!  and we reason about them in terms of the partial order they define on their state. In these
//!  terms, a merge operation returns the smallest value that is bigger than both initial values.

use std::hash::Hash;
use rustc_hash::FxHashSet;

pub trait Crdt {
    /// Merges 'other' into 'self', modifying 'self' in place. The function returns the
    ///  ordering of 'this' and 'other' before the merge.
    fn merge_from(&mut self, other: &Self) -> CrdtOrdering;
}


/// A [CrdtOrdering] represents the partial ordering between two CRDT values (called 'self' and
/// 'other')
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CrdtOrdering {
    /// Both values were equal
    Equal,
    /// The 'this' value was strictly bigger, so the merge result is 'this'
    SelfWasBigger,
    /// The 'other' value was strictly bigger, so the merge result is 'other'
    OtherWasBigger,
    /// Neither value is strictly bigger than the other, so the merge result is some third
    ///  value that is bigger than both.
    NeitherWasBigger,
}
impl CrdtOrdering {
    pub fn merge(&self, other: CrdtOrdering) -> CrdtOrdering {
        use CrdtOrdering::*;

        if *self == other {
            return *self;
        }

        match (*self, other) {
            (Equal, _) => other,
            (_, Equal) => *self,
            _ => NeitherWasBigger,
        }
    }

    pub fn merge_all(it: impl Iterator<Item = CrdtOrdering>) -> Option<CrdtOrdering> {
        it.reduce(|a, b| a.merge(b))
    }
}


/// Convenience implementation of Crdt for a set.
///
/// NB: merging is O(N)
impl <T: Clone + Hash + Eq + PartialEq> Crdt for FxHashSet<T> {
    fn merge_from(&mut self, other: &Self) -> CrdtOrdering {
        let initial_self_len = self.len();

        self.extend(other.iter().cloned());

        if self.len() == initial_self_len {
            if self.len() == other.len() {
                CrdtOrdering::Equal
            }
            else {
                CrdtOrdering::SelfWasBigger
            }
        }
        else {
            if self.len() == other.len() {
                CrdtOrdering::OtherWasBigger
            }
            else {
                CrdtOrdering::NeitherWasBigger
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use super::*;
    use CrdtOrdering::*;

    #[rstest]
    #[case(Equal,            Equal,            Equal)]
    #[case(Equal,            SelfWasBigger,    SelfWasBigger)]
    #[case(Equal,            OtherWasBigger,   OtherWasBigger)]
    #[case(Equal,            NeitherWasBigger, NeitherWasBigger)]
    #[case(SelfWasBigger,    Equal,            SelfWasBigger)]
    #[case(SelfWasBigger,    SelfWasBigger,    SelfWasBigger)]
    #[case(SelfWasBigger,    OtherWasBigger,   NeitherWasBigger)]
    #[case(SelfWasBigger,    NeitherWasBigger, NeitherWasBigger)]
    #[case(OtherWasBigger,   Equal,            OtherWasBigger)]
    #[case(OtherWasBigger,   SelfWasBigger,    NeitherWasBigger)]
    #[case(OtherWasBigger,   OtherWasBigger,   OtherWasBigger)]
    #[case(OtherWasBigger,   NeitherWasBigger, NeitherWasBigger)]
    #[case(NeitherWasBigger, Equal,            NeitherWasBigger)]
    #[case(NeitherWasBigger, SelfWasBigger,    NeitherWasBigger)]
    #[case(NeitherWasBigger, OtherWasBigger,   NeitherWasBigger)]
    #[case(NeitherWasBigger, NeitherWasBigger, NeitherWasBigger)]
    fn test_crdt_ordering_merge(#[case] a: CrdtOrdering, #[case] b: CrdtOrdering, #[case] expected: CrdtOrdering) {
        assert_eq!(a.merge(b), expected);
    }

    #[rstest]
    #[case(vec![], vec![], vec![], Equal)]
    #[case(vec![1], vec![1], vec![1], Equal)]
    #[case(vec![1], vec![], vec![1], SelfWasBigger)]
    #[case(vec![1, 2], vec![1], vec![1, 2], SelfWasBigger)]
    #[case(vec![], vec![1], vec![1], OtherWasBigger)]
    #[case(vec![1],vec![1, 2], vec![1, 2], OtherWasBigger)]
    #[case(vec![1], vec![2], vec![1, 2], NeitherWasBigger)]
    #[case(vec![1], vec![1, 2], vec![1, 2], OtherWasBigger)]
    #[case(vec![1, 2], vec![1], vec![1, 2], SelfWasBigger)]
    fn test_crdt_hashset(#[case] a: Vec<u32>, #[case] b: Vec<u32>, #[case] merged: Vec<u32>, #[case] ordering: CrdtOrdering) {
        let mut a = a.iter().collect::<FxHashSet<_>>();
        let b = b.iter().collect::<FxHashSet<_>>();

        let actual_ordering = a.merge_from(&b);

        assert_eq!(a, merged.iter().collect());
        assert_eq!(actual_ordering, ordering);
    }
}