use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use rustc_hash::FxHashMap;

#[tokio::main]
async fn main() {
    let mut map: Arc<FxHashMap<u32, u32>> = Default::default();

    let atom = AtomicPtr::new(&mut map);

    let a1 = unsafe { (*atom.load(Ordering::Acquire)).clone() };
    let a2 = unsafe { (*atom.load(Ordering::Acquire)).clone() };











}