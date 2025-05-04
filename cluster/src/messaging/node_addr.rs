use anyhow::anyhow;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::{Buf, BufMut};


/// Nodes' lifecycle of membership in a _cluster is monotonous to allow tracking with CRDTs, so a
///  node can never rejoin once it left (or was evicted). To allow rejoining from the same network
///  address, a u32 is added to the network address for identification purposes (initialized with
///  the seconds since epoch) for disambiguation.
///
/// NB: It is *not* a security feature that the unique part must be truly kept unique, unguessable
///       etc. It is purely in the interest of a rejoining node to have a different value from
///       previous join attempts from the same network address. Using the seconds since epoch is
///       just a convenient way of ensuring this in typical environments
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeAddr {
    pub unique: u64,
    pub socket_addr: SocketAddr,
}
impl Hash for NodeAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.unique.hash(state);
        match self.socket_addr {
            SocketAddr::V4(s) => s.ip().to_bits().hash(state),
            SocketAddr::V6(s) => s.ip().to_bits().hash(state),
        };
        self.socket_addr.port().hash(state);
    }
}

impl Debug for NodeAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}@{}]", self.socket_addr, self.unique)
    }
}

impl NodeAddr {
    #[cfg(test)]
    pub fn localhost(unique: u64) -> NodeAddr {
        let addr: SocketAddr = std::str::FromStr::from_str("127.0.0.1:16385").unwrap();

        NodeAddr {
            unique,
            socket_addr: addr,
        }
    }

    pub fn ser(&self, buf: &mut impl BufMut) {
        //TODO unique part is u48, as per RUDP spec
        buf.put_u16((self.unique >> 32) as u16);
        buf.put_u32(self.unique as u32);

        match &self.socket_addr {
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

    pub fn try_deser(buf: &mut impl Buf) -> anyhow::Result<NodeAddr> {
        // unique part is u48 as per RUDP spec
        let unique = ((buf.try_get_u16()? as u64) << 32) + buf.try_get_u32()? as u64;

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
            socket_addr: addr,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::messaging::node_addr::NodeAddr;
    use bytes::BytesMut;
    use rstest::rstest;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[rstest]
    #[case(NodeAddr { unique: 5, socket_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9876).into() })]
    #[case(NodeAddr { unique: 758964, socket_addr: "4.5.6.7:89".parse().unwrap() })]
    #[case(NodeAddr { unique: 3456, socket_addr: "[2001:db8::1]:8080".parse().unwrap() })]
    fn test_ser_deser(#[case] addr: NodeAddr) {
        let mut buf = BytesMut::new();
        addr.ser(&mut buf);
        let deser = NodeAddr::try_deser(&mut buf);
        assert_eq!(deser.unwrap(), addr);
    }
}
