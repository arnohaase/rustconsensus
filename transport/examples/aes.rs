use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce, Key // Or `Aes128Gcm`
};

fn main() {
    let key = Key::<Aes256Gcm>::from_slice(b"very secret very secret 12345678");

    let cipher = Aes256Gcm::new(&key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let nonce = Nonce::from_slice(b"123456781234");

    let ciphertext = cipher.encrypt(&nonce, b"plaintext message".as_ref()).unwrap();
    println!("ciphertext: {:?}", ciphertext);
    println!("ciphertext: {:?}", ciphertext.len());
    let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
    println!("plaintext: {:?}", plaintext);
    println!("plaintext: {:?}", plaintext.len());
}