use std::net::SocketAddr;

#[derive(Debug)]
pub struct UdpConfig {
    /// The address that the receiving UDP socket is bound to
    pub self_addr: SocketAddr,

    /// This is the shared secret of all nodes, and it must be set to the same value. If a key is
    ///  present, AES-256-Gcm encryption is applied to all data, and the key must be exactly
    ///  32 bytes long (per AES spec). If no key is present, packets are sent unencrypted.
    ///
    /// NB: There can be no mixed operation, i.e. either *all* nodes share the same key, or no
    ///      nodes have a key //TODO several keys, phase out
    pub encryption_key: Option<Vec<u8>>,

}


impl UdpConfig {
    pub fn default(self_addr: SocketAddr, encryption_key: Option<Vec<u8>>) -> UdpConfig {
        UdpConfig {
            self_addr,
            encryption_key,
        }
    }
}
