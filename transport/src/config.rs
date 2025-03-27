use anyhow::bail;

pub struct RudpConfig {
    /// This is the payload size inside UDP packets that RUDP assumes. Since RUDP enforces
    ///  non-fragmentation of packets, this payload size (and the implied packet size) must be
    ///  supported by all network connections between nodes.
    ///
    /// In an ideal world, we would configure the MTU (or even discover it) and derive payload
    ///  size from that, but there is some uncertainty involved (e.g. optional IP headers that
    ///  may be introduced by some network hardware). Therefor RUDP leaves the responsibility
    ///  of determining UDP payload with the application rather than making assumptions on its own.
    ///
    /// With full Ethernet frames and no optional IP headers, this payload is `1500 - 20 - 8 = 1472`
    ///  for IPV4 and `1500 - 40 - 8 = 1452` for IPV6. With Jumbo frames, this can be significantly
    ///  bigger. But as explained above, there may be surprising network hardware on (some of)
    ///  the routes, and RUDP does not attempt to guess or compensate for those.
    ///
    /// Choosing this value too big causes packets to be dropped, which may be partial if only some
    ///  of the routes support smaller frames. Choosing it too small wastes bandwidth.
    pub payload_size_inside_udp: usize,
    //TODO integrate with SendStreamConfig::max_packet_len

    /// This is the number of buffers that will be pooled at a given time - buffers in excess of this
    ///  number are discarded when they are returned.
    /// TODO default value
    /// TODO keep track / limit buffers 'in flight'?
    pub buffer_pool_size: usize,

    //TODO integrate with SendStreamConfig and ReceiveStreamConfig
}

impl RudpConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.payload_size_inside_udp < 100 {
            bail!("Payload size is too small");
        }

        Ok(())
    }
}