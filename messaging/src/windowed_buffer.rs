use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use crate::packet_id::PacketId;

/// valid key range is determined by high water mark:
///  (high_water_mark - window_size) ..< high_water_mark  (with wrap-around semantics)
pub struct WindowedBuffer<V> {
    pub high_water_mark: Option<PacketId>,
    window_size: u32,
    buffer: BTreeMap<PacketId, V>,
}

impl<V> WindowedBuffer<V> {
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    pub fn insert(&mut self, id: PacketId, value: V) -> Option<V> {
        //TODO verify that 'id' is in the window

        //TODO update high_water_mark

        self.buffer.insert(id, value)
    }

    pub fn get(&self, id: &PacketId) -> Option<&V> {
        self.buffer.get(id)
    }

    pub fn remove(&mut self, id: &PacketId) -> Option<V> {
        //TODO update high_water_mark
        self.buffer.remove(id)
    }

    /// empty iterator to satisfy the type system - this is optimized away by the compiler
    fn empty_range(&self) -> Range<PacketId, V> {
        self.buffer.range(PacketId::ZERO..PacketId::ZERO)
    }

    pub fn smallest_in_window(&self) -> Option<PacketId> {
        todo!()

        // let high_water_mark = match self.high_water_mark {
        //     Some(hwm) => hwm,
        //     None => return self.empty_range().chain(self.empty_range())
        //         .map(|(k,_)| *k),
        // };
        //
        // let upper_bound = high_water_mark.next(); //TODO does this lead to the send window being one entry smaller than the receive window?
        // let lower_bound = upper_bound.minus(self.window_size);
        //
        // if lower_bound < upper_bound {
        //     self.buffer.range(lower_bound..upper_bound)
        //         .chain(self.empty_range())
        // }
        // else {
        //     // the window is wrap-around
        //     self.buffer.range(upper_bound..)
        //         .chain(self.buffer.range(..lower_bound))
        // }
        //     .map(|(k, _)| *k)
    }

    pub fn outside_window<'a>(&'a self) -> Vec<PacketId> {
        let high_water_mark = match self.high_water_mark {
            Some(hwm) => hwm,
            None => return vec![],
        };

        let upper_bound = high_water_mark.next(); //TODO does this lead to the send window being one entry smaller than the receive window?
        self.less_than(upper_bound.minus(self.window_size))
    }

    /// returns all packet ids that are less than some lower bound
    pub fn less_than(&self, lower_bound: PacketId) -> Vec<PacketId> {
        let high_water_mark = match self.high_water_mark {
            Some(hwm) => hwm,
            None => return vec![],
        };

        let upper_bound = high_water_mark.next();

        if lower_bound < upper_bound {
            // the window is not wrap-around, so the range outside of it is
            self.buffer.range(upper_bound..)
                .chain(self.buffer.range(..lower_bound))
                .map(|(k, _)| *k)
                .collect()
        }
        else {
            // the window is wrap-around, so the range outside of it isn't
            self.buffer.range(upper_bound..lower_bound)
                .map(|(k, _)| *k)
                .collect()
        }
    }

    /// returns all packet ids that are less than or equal to some lower bound
    pub fn less_equal(&self, lower_bound: PacketId) -> Vec<PacketId> {
        let high_water_mark = match self.high_water_mark {
            Some(hwm) => hwm,
            None => return vec![],
        };
        let upper_bound = high_water_mark.next();

        if lower_bound < upper_bound {
            // the window is not wrap-around, so the range outside of it is
            self.buffer.range(upper_bound..)
                .chain(self.buffer.range(..= lower_bound))
                .map(|(k, _)| *k)
                .collect()
        }
        else {
            // the window is wrap-around, so the range outside of it isn't
            self.buffer.range(upper_bound ..= lower_bound)
                .map(|(k, _)| *k)
                .collect()
        }
    }
}