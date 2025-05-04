
pub trait SafeCast<T> {
    fn safe_cast(self) -> T;
}

impl SafeCast<u64> for u32 {
    fn safe_cast(self) -> u64 {
        self as u64
    }
}

#[cfg(not(target_pointer_width = "16"))]
impl SafeCast<usize> for u32 {
    fn safe_cast(self) -> usize {
        self as usize
    }
}

impl SafeCast<usize> for u16 {
    fn safe_cast(self) -> usize {
        self as usize
    }
}

impl SafeCast<u64> for u8 {
    fn safe_cast(self) -> u64 {
        self as u64
    }
}


/// For narrowing casts where business logic ensures that the value is in the narrower type's range.
/// NB: The implementations will panic otherwise
pub trait PrecheckedCast<T> {
    fn prechecked_cast(self) -> T;
}
impl PrecheckedCast<u64> for u128 {
    fn prechecked_cast(self) -> u64 {
        self as u64
    }
}
impl PrecheckedCast<u64> for usize {
    fn prechecked_cast(self) -> u64 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}
impl PrecheckedCast<u32> for usize {
    fn prechecked_cast(self) -> u32 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}
impl PrecheckedCast<u16> for usize {
    fn prechecked_cast(self) -> u16 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}

impl PrecheckedCast<u32> for u64 {
    fn prechecked_cast(self) -> u32 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}
impl PrecheckedCast<u16> for u64 {
    fn prechecked_cast(self) -> u16 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}
impl PrecheckedCast<u16> for u32 {
    fn prechecked_cast(self) -> u16 {
        self.try_into().expect("this is a bug: application logic should have ensured the value range")
    }
}

