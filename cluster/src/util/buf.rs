use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;

//TODO extract to crate?

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

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};
    use rstest::rstest;
    use crate::util::buf::{put_string, try_get_string};

    #[rstest]
    #[case::empty("", vec![0])]
    #[case::a("a", vec![1,97])]
    #[case::abc("abc", vec![3,97,98,99])]
    #[case::umlaut("ä", vec![2,0xc3,0xa4])]
    #[case::heart("❤️", vec![6, 226,157,164,239,184,143])]
    #[case::hearts("❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️❤️", vec![
        132,1, //NB: length is variable-length encoded
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
        226,157,164,239,184,143,
    ])]
    fn test_put_string(#[case] s: &str, #[case] expected: Vec<u8>) {
        let mut buf = BytesMut::new();
        put_string(&mut buf, s);
        assert_eq!(&buf, &expected);

        let mut deser_buf = &mut buf;
        let deser = try_get_string(deser_buf).unwrap();
        assert!(deser_buf.is_empty());
        assert_eq!(&deser, s);
    }

    #[test]
    fn test_try_get_string_remaining() {
        let mut buf = BytesMut::from(b"\x01abc".as_slice());
        let mut deser_buf = &mut buf;
        let actual = try_get_string(&mut deser_buf).unwrap();
        assert_eq!(&actual, "a");
        assert_eq!(deser_buf.chunk(), b"bc");
    }

    #[test]
    fn test_try_get_string_too_short() {
        let mut buf = BytesMut::from(b"\x02a".as_slice());
        let mut deser_buf = &mut buf;
        let actual = try_get_string(&mut deser_buf);
        assert!(actual.is_err());
    }

    #[test]
    fn test_try_get_string_not_unicode() {
        let mut buf = BytesMut::from(b"\x02\xc0\xaf".as_slice());
        let mut deser_buf = &mut buf;
        let actual = try_get_string(&mut deser_buf);
        assert!(actual.is_err());
    }
}
