use std::time::Duration;
use anyhow::bail;
use rustc_hash::FxHashMap;

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

    pub max_message_size: u32,

    pub default_send_stream_config: SendStreamConfig,
    pub specific_send_stream_configs: FxHashMap<u16, SendStreamConfig>,
}

impl RudpConfig {

    ///TODO documentation - ipv4 with end-to-end full Ethernet MTU - without optional headers
    pub fn default_ipv4() -> RudpConfig {
        RudpConfig {
            payload_size_inside_udp: 1472,
            buffer_pool_size: 4096,
            max_message_size: 16*1024*1024,
            default_send_stream_config: SendStreamConfig {
                send_delay: Some(Duration::from_millis(1)),
                send_window_size: 1024,
            },
            specific_send_stream_configs: FxHashMap::default(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.payload_size_inside_udp < 100 {
            bail!("Payload size is too small");
        }

        Ok(())
    }

    pub fn get_effective_send_stream_config(&self, stream_id: u16) -> EffectiveSendStreamConfig {
        let raw = self.specific_send_stream_configs.get(&stream_id)
            .unwrap_or(&self.default_send_stream_config);

        EffectiveSendStreamConfig {
            max_payload_len: self.payload_size_inside_udp, //TODO reduce this by encryption overhead
            late_send_delay: raw.send_delay,
            send_window_size: raw.send_window_size,
        }
    }
}

pub struct SendStreamConfig {
    pub send_delay: Option<Duration>,

    /// This is the maximum number of *packets* (not bytes) stored on the sender side pending an
    ///  ack message
    pub send_window_size: u32,
}


pub struct ReceiveStreamConfig {
    pub nak_interval: Duration, // configure to roughly 2x RTT
    pub sync_interval: Duration, // configure on the order of seconds

    pub receive_window_size: u32,
    pub max_num_naks_per_packet: usize, //TODO limit so it fits into a single packet

    //TODO send this as part of the INIT message to verify agreement (+ max payload size)
    pub max_message_size: u32,
}

pub struct EffectiveSendStreamConfig {
    pub max_payload_len: usize, //TODO calculated from MTU, encryption wrapper, ...
    pub late_send_delay: Option<Duration>,
    pub send_window_size: u32, //TODO ensure that this is <= u32::MAX / 4 (or maybe a far smaller upper bound???)
    // pub max_message_len: usize,
}
