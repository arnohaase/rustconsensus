use anyhow::anyhow;
use bytes_varint::*;


//TODO ideas for extraction to the bytes-varint crate

macro_rules! get_try_impl {
    ($try_getter: ident, $ty:ty, $getter: ident) => {
        fn $try_getter(&mut self) -> anyhow::Result<$ty> {
            if self.remaining() < size_of::<$ty>() {
                return Err(anyhow::anyhow!("buffer underflow"));
            }
            Ok(self.$getter())
        }
    }
}


pub trait BufExt: bytes::Buf + bytes_varint::VarIntSupport {
    fn try_get_usize_varint(&mut self) -> anyhow::Result<usize> {
        let a = self.get_u8();

        Ok(self.get_u64_varint().a()? as usize) //TODO make independent of bit width
    }
    fn try_get_isize_varint(&mut self) -> anyhow::Result<isize> {
        Ok(self.get_i64_varint().a()? as isize) //TODO make independent of bit width
    }

    get_try_impl!(try_get_u8, u8, get_u8);
    get_try_impl!(try_get_u16_le, u16, get_u16_le);
    get_try_impl!(try_get_u32_le, u32, get_u32_le);
    get_try_impl!(try_get_u64_le, u64, get_u64_le);
    get_try_impl!(try_get_u128_le, u128, get_u128_le);

    get_try_impl!(try_get_i8, i8, get_i8);
    get_try_impl!(try_get_i16_le, i16, get_i16_le);
    get_try_impl!(try_get_i32_le, i32, get_i32_le);
    get_try_impl!(try_get_i64_le, i64, get_i64_le);

    fn try_get_string(&mut self) -> anyhow::Result<String> {
        let len = self.try_get_usize_varint()?;
        let mut buf = Vec::new();
        for _ in 0..len {
            buf.push(self.try_get_u8()?);
        }
        Ok(String::from_utf8(buf)?)
    }
}

pub trait BufMutExt: bytes::BufMut + bytes_varint::VarIntSupportMut {
    fn put_usize_varint(&mut self, v: usize) {
        self.put_u64_varint(v as u64); //TODO make independent of bit width
    }
    fn put_isize_varint(&mut self, v: isize) {
        self.put_i64_varint(v as i64); //TODO make independent of bit width
    }

    fn put_string(&mut self, s: &str) {
        self.put_usize_varint(s.len());
        self.put_slice(s.as_bytes());
    }
}

//TODO impl Error for VarIntError


pub trait DummyErrorAdapter<T> {
    fn a(self) -> anyhow::Result<T>;
}
impl <T> DummyErrorAdapter<T> for VarIntResult<T> {
    fn a(self) -> anyhow::Result<T> {
        match self {
            Ok(o) => Ok(o),
            Err(e) => Err(anyhow!("VarInt error: {:?}", e)),
        }
    }
}



impl <T: bytes::Buf> BufExt for T {
}

impl <T: bytes::BufMut> BufMutExt for T {
}
