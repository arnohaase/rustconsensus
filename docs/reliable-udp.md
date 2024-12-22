


## conceptual ideas

* separate kinds of frames for control messages and regular messages:
    * control messages are sent immediately, fire-and-forget, each message in its own frame
        * heartbeat as control messages as well
    * regular messages are sent in regular frames
        * consecutive numbers for frames -> receiver notices when frames are missing
            * NAK after some grace period, and 'status' messages
            * optional back pressure -> ?!?!
        * fragmentation of large messages to MTU-sized buffers
            * serialize small messages directly into the send buffers, zero-copy
    * receive buffers from a global pool
        * assemble parts of a fragmented message into a pre-allocated buffer until all parts are present
* Frame headers, message headers

* join protocol?


## design

### headers, protocol layers 

* Lowest layer: 'transport' (similar to Aeron 'channel')
  * protocol version
  * UDP, stateless
  * sender, receiver (including 'unique' part), encoded whatever way
  * integrity - checksum, shared secret
  * (eventually) optional encryption

* stream: independently sequenced (and eventually subscribable) sequence of packets
  * stream id 
  * sequenced vs fire-and-forget
    * sequenced: packet number
    * fire-and-forget: -

* message - may be spread across several packets, so different headers for first/middle/last
  * length
  * first: message-handler-id (?)
  * middle: -
  * last: -

* *no* receive window
  * back pressure through max number of unacknowledged packets? max difference between
      oldest unacked packet and current counter? 

### sending

* each stream needs its own dedicated set of send buffers to support re-send

* when creating a new stream in the sender:
  * to-one: receiver (socket addr + unique part)
  * (eventually maybe to-many / pub-sub)
  * reliability
    * fire-and-forget
    * best effort buffered / resend, but drop oldest if buffer is full
    * (back pressure - or not)

* buffered sender
  * fragmentation
  * back pressure: message may be only partly sent while waiting for ack -> 'work in progress' parking lot 

### receiving

* receive buffers are pooled globally




### bootstrapping / discovery

TODO




## implementation considerations

* dual-stack sockets do not appear to be universally supported
  * use dedicated v4 and v6 socket for sending, depending on the recipient's stack
  * sender must be part of every message (or there must be some translation table in the receiver)
* message sending: passing in a message as &dyn Message prevents automock
* combine transport and stream flags in a single byte
* socket2 for full control over sockets
  * DF (don't fragment)
