use std::hash::Hash;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use rustc_hash::FxHashMap;

pub(crate) struct AtomicMap<K,V> {
    map: AtomicPtr<Arc<FxHashMap<K,V>>>,
}
impl <K: Hash+Eq+Clone+Sync+Send,V:Clone+Sync+Send> Default for AtomicMap<K,V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Hash+Eq+Clone+Sync+Send, V:Clone+Sync+Send> AtomicMap<K,V> {
    pub fn new() -> AtomicMap<K,V> {
        let map = Arc::new(FxHashMap::<K,V>::default());
        let raw = Box::into_raw(Box::new(map));

        AtomicMap {
            map: AtomicPtr::new(raw),
        }
    }

    pub fn load(&self) -> Arc<FxHashMap<K,V>> {
        unsafe { (*self.map.load(Ordering::Acquire)).clone() }
    }

    pub fn update(&self, f: impl Fn(&mut FxHashMap<K,V>)) {
        loop {
            let old = self.map.load(Ordering::Acquire);

            let mut map: FxHashMap<K,V> = unsafe { (**old).clone() };
            f(&mut map);
            let new = Box::into_raw(Box::new(Arc::new(map)));

            match self.map.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(prev) => {
                    unsafe { drop(Box::from_raw(prev)); }
                    return;
                }
                Err(new) => {
                    unsafe { drop(Box::from_raw(new)); }
                }
            }
        }
    }
}

impl <K,V> Drop for AtomicMap<K,V> {
    fn drop(&mut self) {
        unsafe {
            let raw = self.map.load(Ordering::Acquire);
            drop (Box::from_raw(raw));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_drop() {
        let _ = AtomicMap::<u32, u32>::new();
    }

    #[test]
    fn test_update() {
        let map = AtomicMap::<u32, u32>::new();
        assert!(map.load().is_empty());

        map.update(|m| {
            m.insert(1, 2);
        });
        assert_eq!(1, map.load().len());
        assert_eq!(Some(&2), map.load().get(&1));
    }

    // #[test]
    // fn test_update_no_leak() {
    //     let map = AtomicMap::<u64, u64>::new();
    //
    //     for i in 0..100_000_000 {
    //         map.update(|m| {
    //             m.insert(1, i);
    //         });
    //     }
    // }

    // #[test]
    // fn test_drop_no_leak() {
    //     for _ in 0..100_000_000 {
    //         AtomicMap::<u64, u64>::new();
    //     }
    // }
}