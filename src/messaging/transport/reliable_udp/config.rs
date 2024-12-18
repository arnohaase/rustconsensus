

pub struct ReliableUdpTransportConfig {
    /// The MTU is used to compute max buffer size: We implement our own fragmentation protocol
    ///  which is better than the network layer's, so we are better off avoiding fragmentation
    ///  at the network layer
    pub network_mtu: usize,
    /// The number of MTU-sized buffers on sender side for resending (on NAK), or on the receiver
    ///  side for out-of-order buffer caching.
    pub buffer_cache_size: usize,
}
impl Default for ReliableUdpTransportConfig {
    fn default() -> Self {
        ReliableUdpTransportConfig {
            network_mtu: 1454,
            buffer_cache_size: 16,
        }
    }
}
impl ReliableUdpTransportConfig {
    const UDP_HEADER_SIZE: usize = 8;

    pub fn max_datagram_size(&self) -> usize {
        self.network_mtu - Self::UDP_HEADER_SIZE
    }
}