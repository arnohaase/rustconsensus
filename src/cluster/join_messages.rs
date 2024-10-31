use std::sync::Arc;
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::RwLock;
use tracing::error;
use crate::cluster::cluster_state::ClusterState;
use crate::messaging::envelope::Envelope;
use crate::messaging::message_module::{MessageModule, MessageModuleId};
use crate::messaging::messaging::JOIN_MESSAGE_MODULE_ID;

pub enum JoinMessage {
    Join
}
impl JoinMessage {
    pub fn ser(&self, _buf: &mut BytesMut) {
        //TODO write shared secret
    }

    pub fn deser(_buf: &[u8]) -> anyhow::Result<JoinMessage> {
        Ok(JoinMessage::Join)
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
        JOIN_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, envelope: &Envelope, buf: &[u8]) {
        match JoinMessage::deser(buf) {
            Ok(msg) => {
                //TODO check shared secret

                self.cluster_state.write().await
                    .add_joiner(envelope.from, Default::default()) //TODO roles in 'join' message
            }
            Err(e) => {
                error!("error deserializing message: {}", e);
            }
        }
    }
}
