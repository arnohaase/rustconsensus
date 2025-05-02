use std::hash::Hash;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use rustc_hash::FxHashMap;

pub struct AtomicMap<K,V> {
    map: AtomicPtr<Arc<FxHashMap<K,V>>>, //TODO no Arc
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

    pub fn get(&self, key: &K) -> Option<V> {
        unsafe {
            (*self.map.load(Ordering::Acquire))
                .get(key)
                .cloned()
        }
    }

    pub fn ensure_init(&self, key: K, f: impl Fn() -> V) -> V {
        // short-circuit evaluation: we expect frequent reads and extreme rare updates, so this
        //  is a valid optimization
        if let Some(v) = self.get(&key) {
            return v;
        }
        
        loop {
            let old_ptr = self.map.load(Ordering::Acquire);
            let old_map: Arc<FxHashMap<K,V>> = unsafe { (*old_ptr).clone() };
            
            // this function does not ever overwrite what's stored for a key
            if let Some(v) = old_map.get(&key) {
                return v.clone();
            }
            
            let mut new_map = (*old_map).clone();
            
            // collisions are very rare, so the added complexity for avoiding duplicate calls of f()
            //  is not worth it
            let new_value = f();
            new_map.insert(key.clone(), new_value.clone()); 
            
            let new_ptr = Box::into_raw(Box::new(Arc::new(new_map)));

            match self.map.compare_exchange(old_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    unsafe { drop(Box::from_raw(old_ptr)); }
                    return new_value;
                }
                Err(_) => {
                    unsafe { drop(Box::from_raw(new_ptr)); }
                }
            }
        }
    }
    
    pub fn overwrite_entry(&self, key: K, new_value: V) {
        loop {
            let old_ptr = self.map.load(Ordering::Acquire);
            let old_map: Arc<FxHashMap<K,V>> = unsafe { (*old_ptr).clone() };

            let mut new_map = (*old_map).clone();
            new_map.insert(key.clone(), new_value.clone());

            let new_ptr = Box::into_raw(Box::new(Arc::new(new_map)));

            match self.map.compare_exchange(old_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    unsafe { drop(Box::from_raw(old_ptr)); }
                    return;
                }
                Err(_) => {
                    unsafe { drop(Box::from_raw(new_ptr)); }
                }
            }
        }
    }

    pub fn remove_all(&self, key_predicate: impl Fn(&K) -> bool) {
        loop {
            let old_ptr = self.map.load(Ordering::Acquire);
            let old_map: Arc<FxHashMap<K,V>> = unsafe { (*old_ptr).clone() };

            let mut new_map = (*old_map).clone();
            new_map.retain(|k, _| !key_predicate(k));

            let new_ptr = Box::into_raw(Box::new(Arc::new(new_map)));

            match self.map.compare_exchange(old_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    unsafe { drop(Box::from_raw(old_ptr)); }
                    return;
                }
                Err(_) => {
                    unsafe { drop(Box::from_raw(new_ptr)); }
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

        map.ensure_init(1, || 2);
        assert_eq!(Some(2), map.get(&1));
    }

    //TODO concurrent updates

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