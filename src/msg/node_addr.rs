use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

/// Nodes' lifecycle of membership in a cluster is monotonous to allow tracking with CRDTs, so a
///  node can never rejoin once it left (or was evicted). To allow rejoining from the same network
///  address, a u32 is added to the network address for identification purposes (initialized with
///  the seconds since epoch) for disambiguation.
///
/// NB: It is *not* a security feature that the unique part must be truly kept unique, unguessable
///       etc. It is purely in the interest of a rejoining node to have a different value from
///       previous join attempts from the same network address. Using the seconds since epoch is
///       just a convenient way of ensuring this in typical environments
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct NodeAddr {
    pub unique: u32,
    pub addr: SocketAddr,
}
impl Debug for NodeAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}@{}]", self.addr, self.unique)
    }
}

impl NodeAddr {
    #[cfg(test)]
    pub fn localhost(unique: u32) -> NodeAddr {
        let addr: SocketAddr = std::str::FromStr::from_str("127.0.0.1:8888").unwrap();

        NodeAddr {
            unique,
            addr,
        }
    }
}

impl From<SocketAddr> for NodeAddr { //TODO ToSocketAddrs
    fn from(addr: SocketAddr) -> Self {
        //TODO overarching clock concept
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)
            .expect("system time is before UNIX epoch") //TODO
            .as_secs() as u32;

        NodeAddr {
            unique,
            addr,
        }
    }
}
