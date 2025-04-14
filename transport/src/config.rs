use crate::buffers::encryption::RudpEncryption;
use anyhow::bail;
use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RudpConfig {
    /// The address that the receiving UDP socket is bound to
    pub self_addr: SocketAddr,

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
    /// This is the shared secret of all nodes, and it must be set to the same value. If a key is
    ///  present, AES-256-Gcm encryption is applied to all data, and the key must be exactly
    ///  32 bytes long (per AES spec). If no key is present, packets are sent unencrypted.
    ///
    /// NB: There can be no mixed operation, i.e. either *all* nodes share the same key, or no
    ///      nodes have a key //TODO several keys, phase out
    pub encryption_key: Option<Vec<u8>>,

    pub max_message_len: usize,

    pub default_send_stream_config: SendStreamConfig,
    pub specific_send_stream_configs: FxHashMap<u16, SendStreamConfig>,

    pub default_receive_stream_config: ReceiveStreamConfig,
    pub specific_receive_stream_configs: FxHashMap<u16, ReceiveStreamConfig>,
}

impl RudpConfig {

    ///TODO documentation - ipv4 with end-to-end full Ethernet MTU - without optional headers
    pub fn default(self_addr: SocketAddr, encryption_key: Option<Vec<u8>>) -> RudpConfig {
        let ip_header_size = if self_addr.is_ipv4() {
            20
        }
        else {
            40
        };

        RudpConfig {
            self_addr,
            payload_size_inside_udp: 1500 - ip_header_size - 8,
            buffer_pool_size: 4096,
            encryption_key,
            max_message_len: 16 * 1024 * 1024,
            default_send_stream_config: SendStreamConfig {
                send_delay: Some(Duration::from_millis(1)),
                send_window_size: 1024,
            },
            specific_send_stream_configs: FxHashMap::default(),
            default_receive_stream_config: ReceiveStreamConfig {
                nak_interval: Duration::from_millis(4),
                sync_interval: Duration::from_millis(500),
                receive_window_size: 16 * 1024,
                max_num_naks_per_packet: 150,
            },
            specific_receive_stream_configs: FxHashMap::default(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.payload_size_inside_udp < 100 {
            bail!("Payload size is too small");
        }

        Ok(())
    }

    fn effective_payload_length(&self, encryption: &dyn RudpEncryption) -> usize {
        self.payload_size_inside_udp - encryption.encryption_overhead()
    }

    pub fn get_effective_send_stream_config(&self, stream_id: u16, encryption: &dyn RudpEncryption) -> EffectiveSendStreamConfig {
        let raw = self.specific_send_stream_configs.get(&stream_id)
            .unwrap_or(&self.default_send_stream_config);

        EffectiveSendStreamConfig {
            max_payload_len: self.effective_payload_length(encryption),
            late_send_delay: raw.send_delay,
            send_window_size: raw.send_window_size,
            max_message_len: self.max_message_len,
        }
    }

    pub fn get_effective_receive_stream_config(&self, stream_id: u16) -> EffectiveReceiveStreamConfig {
        let raw = self.specific_receive_stream_configs.get(&stream_id)
            .unwrap_or(&self.default_receive_stream_config);

        EffectiveReceiveStreamConfig {
            nak_interval: raw.nak_interval,
            sync_interval: raw.sync_interval,
            receive_window_size: raw.receive_window_size,
            max_num_naks_per_packet: raw.max_num_naks_per_packet, //TODO calculate this based on packet size
            max_message_len: self.max_message_len,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SendStreamConfig {
    /// This is the duration that a partially filled packet is held back in case some other message
    ///  is sent and can be put into the same packet ('late send').
    ///
    /// None means that all packets are sent immediately without 'late send', meaning that messages
    ///  are never combined and sent in a single packet.
    pub send_delay: Option<Duration>,

    /// This is the maximum number of *packets* (not bytes) stored on the sender side pending an
    ///  ack message
    pub send_window_size: u32,
}

pub struct EffectiveSendStreamConfig {
    /// This is the number of bytes available for RUDP payload in a network packet. This is smaller
    ///  than the UDP payload due to RUDP encryption overhead
    pub max_payload_len: usize,
    pub late_send_delay: Option<Duration>,
    pub send_window_size: u32, //TODO ensure that this is <= u32::MAX / 4 (or maybe a far smaller upper bound???)
    pub max_message_len: usize,
}

//TODO measure RTT
//TODO traffic shaping - regular ACK, delay when send buffer is full

#[derive(Clone, Debug)]
pub struct ReceiveStreamConfig {
    pub nak_interval: Duration, // configure to roughly 2x RTT
    pub sync_interval: Duration, // configure on the order of seconds

    pub receive_window_size: u32,
    pub max_num_naks_per_packet: usize, //TODO limit so it fits into a single packet
}

pub struct EffectiveReceiveStreamConfig {
    pub nak_interval: Duration, // configure to roughly 2x RTT
    pub sync_interval: Duration, // configure on the order of seconds

    pub receive_window_size: u32,
    pub max_num_naks_per_packet: usize, //TODO limit so it fits into a single packet

    //TODO send this as part of the INIT message to verify agreement (+ max payload size)
    pub max_message_len: usize,
}

