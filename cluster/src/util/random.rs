use rand::{Rng, RngCore};
use std::ops::Range;
#[cfg(test)] use std::sync::Mutex;



#[cfg(test)]
/// automock expectations for static methods are global - hold this lock to avoid races
pub static MOCK_RANDOM_MUTEX: Mutex<()> = Mutex::new(());

#[cfg_attr(test, mockall::automock)]
pub trait Random: Send + Sync {
    fn next_u32() -> u32;
    fn gen_f64_range(range: Range<f64>) -> f64;
    fn gen_usize_range(range: Range<usize>) -> usize;
}
pub struct RngRandom {}
impl Random for RngRandom {
    fn next_u32() -> u32 {
        rand::rng().next_u32()
    }

    fn gen_f64_range(range: Range<f64>) -> f64 {
        rand::rng().random_range(range)
    }

    fn gen_usize_range(range: Range<usize>) -> usize {
        rand::rng().random_range(range)
    }
}
