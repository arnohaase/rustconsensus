use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::BytesMut;
use bytes_varint::{VarIntSupport, VarIntSupportMut};
use tokio::sync::RwLock;
use tracing::error;
use crate::cluster::cluster_state::ClusterState;
use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;
use crate::util::buf::{put_string, try_get_string};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JoinMessage {
    Join{ roles: BTreeSet<String>, }
}

impl Message for JoinMessage {
    fn module_id(&self) -> MessageModuleId {
        Self::JOIN_MESSAGE_MODULE_ID
    }

    fn ser(&self, buf: &mut BytesMut) {
        let JoinMessage::Join { roles} = self;

        buf.put_usize_varint(roles.len());
        for role in roles {
            put_string(buf, role);
        }
    }

    fn box_clone(&self) -> Arc<dyn Any + Send + Sync + 'static> {
        Arc::new(self.clone())
    }
}

impl JoinMessage {
    pub const JOIN_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"CtrJoin\0");

    pub fn deser(mut buf: &[u8]) -> anyhow::Result<JoinMessage> {
        let mut roles = BTreeSet::default();

        let num_roles = buf.try_get_usize_varint()?;
        for _ in 0..num_roles {
            roles.insert(try_get_string(&mut buf)?);
        }

        Ok(JoinMessage::Join { roles })
    }
}

pub struct JoinMessageModule {
    cluster_state: Arc<RwLock<ClusterState>>,
}

impl JoinMessageModule {
    pub fn new(cluster_state: Arc<RwLock<ClusterState>>) -> Arc<JoinMessageModule> {
        Arc::new(JoinMessageModule {
            cluster_state,
        })
    }
}

#[async_trait]
impl MessageModule for JoinMessageModule {

    fn id(&self) -> MessageModuleId {
        JoinMessage::JOIN_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, sender: NodeAddr, buf: &[u8]) {
        match JoinMessage::deser(buf) {
            Ok(JoinMessage::Join { roles }) => {
                //TODO check shared secret

                self.cluster_state.write().await
                    .add_joiner(sender, roles);
            }
            Err(e) => {
                error!("error deserializing message: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case::no_roles(vec![])]
    #[case::one_role(vec!["a"])]
    #[case::two_roles(vec!["x", "yz"])]
    fn test_ser(#[case] roles: Vec<&str>) {
        let roles = roles.iter()
            .map(|s| s.to_string())
            .collect::<BTreeSet<String>>();
        let msg = JoinMessage::Join { roles };

        let mut buf = BytesMut::new();
        msg.ser(&mut buf);
        let deserialized = JoinMessage::deser(&buf).unwrap();

        assert_eq!(deserialized, msg);
    }
}
