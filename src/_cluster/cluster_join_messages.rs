use std::sync::Arc;

use bytes::BufMut;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::_cluster::cluster::Cluster;
use crate::messaging::envelope::Envelope;
use crate::messaging::message_module::{MessageModule, MessageModuleId};
use crate::messaging::messaging::JOIN_MESSAGE_MODULE_ID;


pub struct ClusterJoinMessageModule {
    cluster: Arc<RwLock<Cluster>>,
}
impl ClusterJoinMessageModule {
    const MSG_ID: u8 = 0;

    //TODO unit test
    pub fn ser_join_message(buf: &mut impl BufMut) {
        buf.put_u8(Self::MSG_ID);
    }
}
#[async_trait::async_trait]
impl MessageModule for ClusterJoinMessageModule {
    //TODO unit test
    fn id(&self) -> MessageModuleId {
        JOIN_MESSAGE_MODULE_ID
    }

    //TODO unit test
    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        if buf.len() == 1 {
            match buf[0] {
                Self::MSG_ID => {
                    debug!("received JOIN message from {:?}", envelope.from);

                    //TODO shared secret

                    self.cluster.write()
                        .await
                        .add_joining_node(envelope.from);
                },
                _ => warn!("invalid message, skipping"),
            };
        }
        else {
            warn!("invalid message length, skipping");
        }
    }
}

#[cfg(test)]
mod test {


}