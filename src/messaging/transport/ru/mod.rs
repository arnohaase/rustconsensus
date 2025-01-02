//! This transport protocol is designed as a compromise between TCP and UDP, providing some
//!  reliability and order-of-delivery guarantees while prioritising low latency over "fully"
//!  in-sequence delivery (TCP style).
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
//!   * This protocol is designed for
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
//!
//! Packet header (inside a UDP packet) - all numbers in network byte order (BE):
//! ```ascii
//!     0                   1                   2                   3
//!     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//!  0 | CRC checksum for the rest of the packet, starting after this  |
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//!  4 |V|# # #|kind of|  sender / reply-to address (IPV4 or IPV6)     |
//!    |6|# # #|frame  |                                               |
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//!  8 | sender / reply-to address (IPV4 or IPV6)                      |
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! 12 | sender / reply-to port        | sender / reply to identifier  |
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! 16 | receiver identifier           | first message offset (encoded)|
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! 20 | packet sequence number (windowed, wrap-around)                |
//!    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! 24...
//! ```
//! Flags:
//! * Bit 0: IPV4 vs. IPV6 for the reply-to address
//! * Bits 1-3: protocol version, 0 for this version
//! * Bits 4-7: kind of frame:
//!   * 0000 regular sequenced
//!   * 0001 out-of-sequence
//!   * 0002 NAK
//!   * 0003 status (both send and receive side)
//! * first message offset: offset of the start of the first message in the frame, starting
//!    after the header - or FFFF if the frame continues a message from a previous frame that
//!    continues in the next frame.
//!    * If the frame completes a multi-frame message, the offset points to the first
//!      byte after the end of the message
//!
//!
//! Message header (message may be split across multiple packets)
//!
//! 0: packet length (var-length encoded), starting *after* the encoded length TODO upper limit



struct Abc {

}
