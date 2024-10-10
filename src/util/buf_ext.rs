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
    get_try_impl!(try_get_u16, u16, get_u16);
    get_try_impl!(try_get_u32, u32, get_u32);
    get_try_impl!(try_get_u64, u64, get_u64);
    get_try_impl!(try_get_u128, u128, get_u128);

    get_try_impl!(try_get_i8, i8, get_i8);
    get_try_impl!(try_get_i16, i16, get_i16);
    get_try_impl!(try_get_i32, i32, get_i32);
    get_try_impl!(try_get_i64, i64, get_i64);
}

pub trait BufMutExt: bytes::BufMut + bytes_varint::VarIntSupportMut {
    fn put_usize_varint(&mut self, v: usize) {
        self.put_u64_varint(v as u64); //TODO make independent of bit width
    }
    fn put_isize_varint(&mut self, v: isize) {
        self.put_i64_varint(v as i64); //TODO make independent of bit width
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
