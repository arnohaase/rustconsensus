use crate::messaging::message_module::Message;
use crate::messaging::messaging::MessageSender;
use crate::messaging::node_addr::NodeAddr;
use async_trait::async_trait;
use std::any::Any;
use std::net::SocketAddr;
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

    async fn send_to_node<T: Message>(&self, to: NodeAddr, _stream_id: u16, msg: &T) {
        self.tracker.write().await.push((to, msg.box_clone()));
    }

    async fn send_to_addr<T: Message>(&self, to: SocketAddr, _stream_id: u16, msg: &T) {
        self.tracker.write().await.push((NodeAddr { unique: 0, socket_addr: to}, msg.box_clone()));
    }

    async fn send_raw_fire_and_forget<T: Message>(&self, to_addr: SocketAddr, required_to_generation: Option<u64>, msg: &T) {
        let unique = required_to_generation.unwrap_or(0);
        self.tracker.write().await.push((NodeAddr { unique, socket_addr: to_addr}, msg.box_clone()));
    }
}
