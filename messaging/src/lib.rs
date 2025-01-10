//! This transport protocol is designed as a compromise between TCP and UDP, providing some
//!  reliability and order-of-delivery guarantees while prioritising low latency over "fully"
//!  in-sequence delivery (TCP style).
//!
//! ## Design goals
//!
//! It aims to fulfill the following design goals:
//! * The protocol is peer-to-peer without a dedicated server vs. client
//!   * each node needs a listening UDP socket that handles all 'connections'
//!   * allow 'mixed protocol' operation, i.e. connect a node listening on IP V4 with a node
//!     listening on IP V6
//!   * designed for distributed applications with shared configuration and administration
//!   * several disjoint connections can go through the same pair of UDP ports - for this, an
//!      additional identifier is introduced for multiplexing / demultiplexing. This has
//!      'port numbers' (tunneled through a UDP socket with a single port)
//!   * explicitly *not* a messaging system with pub/sub, broadcast, really reliable delivery etc.
//!      --> different trade-offs
//! * The abstraction is sending / receiving *messages* (i.e. defined-length chunks of data as
//!   opposed to streams of bytes etc.)
//! * Maximise throughput and minimise latency on reliable networks (e.g. inside a data center)
//!   * The default is to assume delivery unless a negative ack is sent
//! * Buffer incoming data if packets are missing, delaying delivery to the application until
//!   gaps are filled
//!   * *packets* have sequence numbers and are acknowledged, re-sent etc. *Messages* can be
//!     split across packets
//! * Guarantee that those messages that are delivered, are delivered in the order they were sent
//! * Big messages are sent without IP-level fragmentation - this protocol takes care of chunking,
//!   buffering and re-assembling
//!   * configured MTU since discovery does not work reliably
//! * Skip missing packets if the receive window becomes too big
//!   * There is no way to guarantee delivery (in-sequence or otherwise) --> TCP timeout etc.
//!   * This protocol is designed for skipping dropped messages and continuing with fresh ones
//!      rather than maximising delivery reliability at the cost of latency and throughput
//! * Combine small messages into a single packet, delaying the send operation for a configurable
//!   interval.
//!   * NB: While there are similarities to Nagle's algorithm, the use of negative ACK mitigates
//!     the problem of stacked delays with slow ACKs that exists for TCP
//! * Support out-of-sequence messages (coordination / system messages) similar to what TCP's
//!    'urgent' flag attempted
//! * The protocol should not require an explicit handshake to set up a connection. The peers
//!   should rather be able to sync on the place in the packet stream 'on the go'
//! * There should be a reliable checksum per packet
//! * There should be (optional) strong encryption at the package level
//! * cleanup of resources is triggered by a configurable timeout or via API
//!   * independently for both sides - "re-connect" should happen transparently anyway
//! * stream multiplexing over a single port / 'connection' - with different QoS configurations,
//!    ack strategies etc.
//!   * e.g. control messages + regular messages and high-frequency, low-latency messages where
//!      packets older than 1 second are obsolete and can be dropped
//!
//! ## Header
//!
//! Packet header (inside a UDP packet) - all numbers in network byte order (BE):
//! ```ascii
//! 0:  CRC checksum for the rest of the packet, starting after the checksum: u32
//! 4:  protocol version (u8)
//! 5:  flags (8 bits):
//!     * bit 0-1: protocol version of the reply-to address:
//!       * 00  V4, explicitly provided in packet
//!       * 01  V6, explicitly provided in packet
//!       * 10  identical to UDP sender
//!     * bit 2-4: kind of frame:
//!       * 000 regular sequenced
//!       * 001 out-of-sequence for single-packet application-level messages
//!       * 010 INIT
//!       * 011 NAK
//!       * 100 RECV_SYNC
//!       * 101 SEND_SYNC
//!     * 5-7: unused, should be 0
//! 6:  reply-to address (4 bytes if IP V4, 7 bytes if IP V6)
//! *:  reply-to port: u16
//! *: stream id (varint up to u16): the id of the multiplexed stream that this frame belongs
//!      or refers to. Not present for frame kind '001'.
//!      NB: Each stream has its own send and receive buffers, incurring per-stream overhead
//! *:  first message offset (u16): offset of the first message header after the header, or
//!      FFFF if the frame continues from a message from the previous frame does not finish it.
//!      Present only for frame kind '0000'.
//!      NB: If this frame completes a multi-frame message without starting a new one, this
//!       offset points to the first offset after the end of the packet
//! *: packet sequence number (varint up to u32): sequence number of this frame in its stream.
//!      Present only for frame kind '0000'.
//!      NB: Sequence numbers are wrap-around, so 0 follows after FFFFFFFF.
//!```
//!
//! The packet header has variable size, ranging from 12 bytes for a control message with IP V4
//!  reply-to address to 25 bytes for a sequenced packet with IP V6 reply-to address and maximum
//!  stream ID and packet sequence numbers.
//!
//! Message header (message may be split across multiple packets)
//!
//! ``` ascii
//! 0: packet length (var-length encoded), starting *after* the encoded length TODO upper limit
//! ```
//!
//! ## Control messages
//!
//! *INIT*
//!
//! This control message, sent by a receiver, requests the peer to send a SEND_SYNC message
//!  and (re)send all messages in its send buffer. It is one way (though not the only
//!  reasonable one) to start a conversation.
//!  NB: This message is sent *for one specific stream*.
//!
//! ```ascii
//! [no payload]
//! ```
//!
//! *RECV_SYNC*
//!
//! This control message is sent periodically by a receiver of a stream to sync with the sender
//!  of that stream in a robust way - most importantly sending a positive ACK that all messages
//!  up to a low water mark are processed and will never be requested again, so the sender can
//!  remove them from its send buffer. This acts as a safety net - acknowledgement of messages
//!  is taken care of in `NAK` messages during regular operation.
//!
//! This message requests the sender to respond with a `SEND_SYNC` message.
//!
//! ```ascii
//! 0: receive buffer high water mark (varint) - a u32 value for the highest packet id that was
//!     received, or `u32::MAX + 1` if no packet was received yet
//! *: receive buffer low water mark (varint) - a u32 value for the lowest packet id that was
//!     received but not yet dispatched fully, or `u32::MAX + 1` if no packet was received yet.
//!     NB: This can be lower than the ACK threshold if some initial part of a multi-packet message
//!          was received successfully
//! *: receive buffer ACK threshold (varint) - a u32 value for the highest packet id up to which
//!     all packets are acknowledged ('late ack'), or `U32::MAX + 1` if no ACK threshold is
//!     established yet.
//! ```
//!
//! *SEND_SYNC*
//!
//! This control message is sent by the sender of a stream in response to `RECV_SYNC`, giving
//!  some statistics about the send buffers.
//!
//! ```ascii
//! 0: send buffer high water mark (u32 BE) - the packet id after the highest sent
//!     packet, i.e. the next packet to be sent
//! *: send buffer low water mark (u32 BE) - the lowest packet id for which a packet
//!     is retained for resending, or the high water mark if none
//! ```
//!
//! TODO timestamp / RTT
//!
//! *NAK*
//!
//! Request that the peer re-send a specific set of packets that got dropped or corrupted, or
//!  were not delivered in a timely fashion for some reason. This is the protocol's primary
//!  means of acknowledging packets and ensuring a gap-free in-sequence dispatch of messages on
//!  the receiver side. It is possible and intended to tune this to the quality criteria of
//!  the underlying network, especially in a data center.
//!
//! NB: The criteria for sending this message are configurable, and the protocol is robust
//!      with regard to differing configurations. It is desirable to let some grace period pass
//!      before NAK'ing a gap to give regular out-of-order arrival a chance before requesting
//!      a re-send. Other possible criteria are the number of missing packets, or a grace period
//!      before re-requesting packets that were NAK'ed before.
//!
//! NB: This is a control message, so it must fit into a single packet. It is the sender's
//!      responsibility to ensure this, and split the NAK'ed packet ids into several NAK messages
//!      if necessary
//!
//! ```ascii
//! 0: number of NAK'ed packet ids (varint u16)
//! *: (repeated) packet id to be re-sent (varint u32)
//! ```
//!
//! ## Related:
//! * UDT
//!   * dedicated UDP socket per peer
//!   * single channel
//!   * backpressure, i.e. slow down sending on congestion
//!   * optimized for sending large data volumes / streams over fast but unreliable networks
//! * QUIC
//!   * connection based - initial handshake
//!   * enforces encryption (TLS 1.3)
//!   * has stream multiplexing
//!   * abstracts over client's IP address to facilitate switch-over from Wifi to GSM
//!   * focus on large 'messages' (files) - stream per 'message'
//!   * asymmetric - client vs server
//!   * dedicated port per peer
//! * Aeron
//!   * message broker - pub/sub etc.
//!   * designed for minimum latency
//!   * dedicated, pre-allocated buffers per peer
//!   * back pressure, never drop messages
//!

mod packet_header;
mod control_messages;
mod receive_stream;
mod end_point;
mod send_stream;
mod message_dispatcher;
mod raw_send_socket;