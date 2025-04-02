use aead::{AeadMutInPlace, Buffer};
use aes_gcm::{Aes256Gcm, Key, AeadInPlace};

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Nonce};
use bytes::BytesMut;

fn main() {
    let text = b"plaintext message";

    let key = Key::<Aes256Gcm>::from_slice(b"very secret very secret 12345678");

    let cipher = Aes256Gcm::new(&key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let nonce = Nonce::<Aes256Gcm>::from_slice(b"123456781234");

    let ciphertext = cipher.encrypt(&nonce, text.as_ref()).unwrap();
    println!("ciphertext: {:?}", ciphertext);
    println!("ciphertext: {:?}", ciphertext.len());
    let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
    println!("plaintext: {:?}", plaintext);
    println!("plaintext: {:?}", plaintext.len());

    let mut buf = BytesMut::with_capacity(1024);
    encrypt_in_place_bytesmut(&cipher, &nonce, text.as_ref(), &mut buf).unwrap();
    println!("ciphertext2: {:?}", buf.as_ref());
    println!("ciphertext2: {:?}", buf.as_ref().len());
    let plaintext = cipher.decrypt(&nonce, &*buf).unwrap();
    println!("plaintext: {:?}", plaintext);
}


fn decrypt_in_place(cipher: &Aes256Gcm, nonce: &Nonce<Aes256Gcm>, buffer: &mut impl Buffer) {
    cipher.decrypt_in_place(nonce, b"", buffer)
        .unwrap()

}

fn encrypt_in_place_bytesmut(
    cipher: &Aes256Gcm,
    nonce: &Nonce<Aes256Gcm>,
    plaintext: &[u8],
    out: &mut BytesMut,
) -> Result<(), aes_gcm::aead::Error> {
    // Clear and prepare
    out.clear();
    out.extend_from_slice(plaintext);

    // Encrypt in place
    cipher.encrypt_in_place(nonce, b"", out)?;
    Ok(())
}