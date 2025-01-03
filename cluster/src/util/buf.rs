use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;


//TODO unit test
//TODO extract to crate?
//TODO convenience for serializing / deserializing collections

pub fn put_string(buf: &mut BytesMut, s: &str) {
    buf.put_usize_varint(s.len());
    buf.put_slice(s.as_bytes());
}

pub fn try_get_string(buf: &mut impl Buf) -> anyhow::Result<String> {
    let len = buf.try_get_usize_varint()?;
    let mut result = Vec::new();
    for _ in 0..len {
        result.push(buf.try_get_u8()?);
    }

    let s = String::from_utf8(result)?;
    Ok(s)
}
