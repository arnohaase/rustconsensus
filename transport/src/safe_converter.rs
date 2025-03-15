
pub trait SafeCast<T> {
    fn safe_cast(self) -> T;
}

impl SafeCast<u32> for u64 {
    fn safe_cast(self) -> u32 {
        self as u32
    }
}

#[cfg(not(target_pointer_width = "16"))]
impl SafeCast<usize> for u32 {
    fn safe_cast(self) -> usize {
        self as usize
    }
}


/// For narrowing casts where business logic ensures that the value is in the narrower type's range.
/// NB: The implementations will panic otherwise
pub trait PrecheckedCast<T> {
    fn prechecked_cast(self) -> T;
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

