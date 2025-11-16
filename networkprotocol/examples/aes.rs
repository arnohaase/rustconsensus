use bytes::BufMut;
use networkprotocol::buffers::encryption::{Aes256GcmEncryption, RudpEncryption};
use networkprotocol::buffers::fixed_buffer::FixedBuf;

fn main() {
    let mut buf = FixedBuf::new(1000);
    buf.put_slice(&[0, 91, 217, 220, 214, 61, 78, 165, 77, 132, 146, 247, 194, 2, 1, 150, 0, 156, 21, 101, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3, 0, 0, 0, 4, 2, 3, 4, 5, 0, 0, 0, 1, 7, 0, 0, 0, 3, 4, 5, 6]);

    let expected: &[u8] = &[0, 91, 217, 220, 214, 61, 78, 165, 77, 132, 146, 247, 194, 215, 238, 213, 26, 68, 148, 49, 34, 184, 252, 138, 71, 110, 210, 73, 37, 138, 194, 50, 210, 233, 108, 167, 67, 209, 156, 78, 42, 242, 205, 187, 30, 90, 60, 190, 197, 170, 63, 143, 27, 157, 70, 42, 150, 59, 167, 182, 198, 202, 196, 96, 200, 175, 187, 168, 207, 187, 189, 117, 73, 155, 161].to_vec();

    let encryption = Aes256GcmEncryption::new(&vec![5u8; 32]);
    encryption.encrypt_buffer(&mut buf);

    println!("{:?}", buf);
    assert_eq!(buf.as_ref(), expected);

    encryption.decrypt_buffer(&mut buf).unwrap();
    println!("{:?}", buf);
}
