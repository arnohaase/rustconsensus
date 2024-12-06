use crate::messaging::message_module::Message;
use crate::messaging::messaging::MessageSender;
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::cluster::join_messages::JoinMessage;

#[derive(Debug)]
pub struct TrackingMockMessageSender {
    myself: NodeAddr,
    tracker: Arc<RwLock<Vec<(NodeAddr, Arc<dyn Any + Send + Sync>)>>>,
}
impl TrackingMockMessageSender {
    pub fn new(myself: NodeAddr) -> Self {
        TrackingMockMessageSender {
            myself,
            tracker: Default::default()
        }
    }

    pub async fn assert_message_sent<M: Message + PartialEq + Eq>(&self, to: NodeAddr, message: M) {
        let mut lock = self.tracker.write().await;
        if lock.is_empty() {
            panic!("no message was sent");
        }

        let (actual_to, actual_msg) = lock.remove(0);
        let is_join_message = (*actual_msg).is::<JoinMessage>();

        if let Ok(actual_msg) = actual_msg.clone().downcast::<M>() {
            assert_eq!(actual_msg.as_ref(), &message);
            if is_join_message {
                // the target address' unique part is ignored for Join messages
                assert_eq!(actual_to.socket_addr, to.socket_addr);
            }
            else {
                assert_eq!(actual_to, to);
            }
        }
        else {
            panic!("expected message {:?}, was {:?}", message, actual_msg);
        }
    }

    pub async fn assert_no_remaining_messages(&self) {
        assert!(
            self.tracker.read().await
                .is_empty()
        );
    }
}

#[async_trait]
impl MessageSender for TrackingMockMessageSender {
    fn get_self_addr(&self) -> NodeAddr {
        self.myself
    }

    async fn send<T: Message>(&self, to: NodeAddr, msg: &T) -> anyhow::Result<()> {
        self.tracker.write().await.push((to, msg.box_clone()));
        Ok(())
    }
}
