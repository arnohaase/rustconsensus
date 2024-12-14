# rustconsensus

This is a bare-bones library for nodes collaborating across a network. It offers solutions for common
tasks:

* sending and receiving messages
* having a concept of 'membership', i.e. reaching consensus on which nodes are currently part of the
      cluster
  * having a globally agreed-upon leader (at least in the cluster's stable state)
  * allowing nodes to leave and re-join in a robust and race-free manner
* monitoring reachability
  * distributing information on (un)reachability of nodes to the entire cluster
  * remove nodes that remain unreachable in a robust fashion (e.g. because their processes just died)
  * handling more complex unreachability scenarios like network partitioning ('split brain') or flaky network
        components, at least pretty robustly

# TODO

TODO documentation for transport
TODO documentation for gossip
TODO documentation for heartbeat
TODO higher-level abstractions like sharding, ddata, broadcast / pub-sub, req/resp with timeout
