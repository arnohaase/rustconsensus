use std::fmt::{Debug, Formatter};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::anyhow;

use bytes::{Buf, BufMut};

use bytes_varint::try_get_fixed::TryGetFixedSupport;

/// Nodes' lifecycle of membership in a _cluster is monotonous to allow tracking with CRDTs, so a
///  node can never rejoin once it left (or was evicted). To allow rejoining from the same network
///  address, a u32 is added to the network address for identification purposes (initialized with
///  the seconds since epoch) for disambiguation.
///
/// NB: It is *not* a security feature that the unique part must be truly kept unique, unguessable
///       etc. It is purely in the interest of a rejoining node to have a different value from
///       previous join attempts from the same network address. Using the seconds since epoch is
///       just a convenient way of ensuring this in typical environments
#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

    //TODO unit test
    pub fn ser(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.unique);
        match &self.addr {
            SocketAddr::V4(data) => {
                buf.put_u8(4);
                buf.put_u32(data.ip().to_bits());
                buf.put_u16(data.port());
            }
            SocketAddr::V6(data) => {
                buf.put_u8(6);
                buf.put_u128(data.ip().to_bits());
                buf.put_u16(data.port());
            }
        }
    }

    //TODO unit test
    pub fn try_deser(buf: &mut impl Buf) -> anyhow::Result<NodeAddr> {
        let unique = buf.try_get_u32()?;

        let addr = match buf.try_get_u8()? {
            4 => {
                let ip = buf.try_get_u32()?;
                let port = buf.try_get_u16()?;
                SocketAddr::V4(SocketAddrV4::new(ip.into(), port))
            }
            6 => {
                let ip = buf.try_get_u128()?;
                let port = buf.try_get_u16()?;
                SocketAddr::V6(SocketAddrV6::new(ip.into(), port, 0, 0))
            }
            n => {
                return Err(anyhow!("invalid socket address discrimiator: {}", n));
            }
        };
        Ok(NodeAddr {
            unique,
            addr,
        })
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
