# Congestion Control

A simple network protocol that "just sends" data when the application asks it to can overwhelm 
a network if an application's send rate exceeds available bandwidth. This leads to packet loss,
triggering resends and further increasing network load and potentially escalating problems.

This can happen even if an application generates load that is well withing the network's capacity:
Failure of some network component can temporarily degrade available bandwidth, or some unrelated
network traffic can eat up available bandwidth. Such events are a fact of life in the real world
in a data center - they are unpredictable, and applications need to handle them gracefully, ideally
at the network protocol level.

## The RUDP approach

RUDP reacts to bandwidth limitation by limiting the number of "in-flight" (i.e. unacknowledged) packets
when packet loss occurs. This is similar to the approach of TCP or SCTP, and it plays fair with these
protocols (which is important when running on the same network).

This approach consists of several aspects that warrant a closer look:
* Congestion Control is done on the sending side. There are no 'dynamic receive window' adjustments
    etc. because RUDP is designed for Peer-to-Peer communication inside a data center where this
    kind of dynamicity has limited value.
* Limiting the number of in-flight packets actually reduces the bandwidth that is used by RUDP: If
    this limit is reached, further packets are held back (with backpressure to the application, see
    below) until acknowledgements arrive. 
* RUDP uses packet loss as a proxy for reaching the network's bandwidth limits. Packet loss is detected
    by NAK responses.
* Congestion control is applied separately per peer. The algorithm balances the throttling statistically
    if some or all peers share the bandwidth-limited part of the network. 
* More precisely, every channel for a peer has its own congestion control. This allows some channels
    to be prioritized over others (automatically for low-traffic channels).
    TODO channel instead of stream
* RUDP uses *cubic* congestion window adjustment: TODO
* RUDP starts *fast*, i.e. it starts assuming high available network bandwidth, which is assumed to be
    a good default for networks inside a data center.

## Congestion Window

Let's look at this in detail. 

A sender has dedicated *send buffers* per channel, storing packets once they are submitted (which may be some time
before they are actually sent on the network) through their entire lifecycle, ending when a client acknowledges
them or they move out of the send buffer.

The following diagram sketches such a send buffer:

```text
+ congestion window size \
|                         |  Congestion Window: The congestion marker (which is relative to the 
...                       |   unacknowledged marker) defines the highest packet ID that will actually
|                         |   be sent. Since it moves with each received acknowledgement, this moving
+ high-water mark         |   window will reach all packets eventually, the speed of its advancement
|                         |   determined by its size.
...                       |
|                         |
+ unsent marker           |
|                         |
...                       |
|                         |
+ unacknowledged marker  /
```

It effectively starts with the lowest unacknowledged packet: There is no reason to keep acknowledged packets in the
buffer.

Next, there is the *unsent marker*: packets below it have already been sent to the network, while packets above have
not. 

The *high-water mark* marks the upper bound of send buffer usage, with packets between the *unsent marker* and the
*high-water mark* queued up for sending. There is a configured hard upper bound for the difference between *high-water
mark* and *unacknowledged marker*: Once that is reached, there is backpressure to application code, preventing more
packets from being added.

### cwnd

The **congestion window size (cwnd)** is an upper bound for the number of sent but unacknowledged packets, i.e.

> `unsent-marker` <= `cwnd`

(NB: This is a 'soft' limit which can temporarily be exceeded if *cwnd* is decreased below the current number of
in-flight packets)

During normal operation without packet loss, the congestion window size is the full configured maximum send buffer
size, i.e. it does not impose an additional constraint, and both *unsent-marker* and *high-water mark* can never exceed
*cwnd*. In this state, all buffered packets can be sent immediately, i.e. there is no reason for *high-water mark* to
be greater than *unsent-marker*.

*cwnd* can decrease or increase (see next section), affecting the way RUDP handles unsent packages.
* unsent packets inside the congestion window are sent (e.g. if the congestion window moves up due to a newly
    received ACK)
* new packets are sent if they fit into the congestion window, and buffered if they don't. Neither causes backpressure
    to the application
* if buffered packets reach the configured maximum send buffer size, that causes back pressure at the application
    level. This can happen (even repeatedly) in the middle of sending a large message, and it allows sending messages
    that exceed send buffer size.

### cwnd adjustment

 








