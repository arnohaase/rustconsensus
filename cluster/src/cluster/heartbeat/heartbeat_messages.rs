use std::any::Any;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use bytes_varint::try_get_fixed::TryGetFixedSupport;
use tokio::sync::mpsc;
use tracing::error;

use crate::messaging::message_module::{Message, MessageModule, MessageModuleId};
use crate::messaging::node_addr::NodeAddr;

pub const HEARTBEAT_MESSAGE_MODULE_ID: MessageModuleId = MessageModuleId::new(b"CtrHeart");

pub struct HeartbeatMessageModule {
    channel: mpsc::Sender<(NodeAddr, HeartbeatMessage)>,
}
impl HeartbeatMessageModule {
    pub fn new(channel: mpsc::Sender<(NodeAddr, HeartbeatMessage)>) -> Arc<HeartbeatMessageModule> {
        Arc::new({
            HeartbeatMessageModule {
                channel,
            }
        })
    }

    //TODO unit test heartbeat message module
    async fn _on_message(&self, sender: NodeAddr, buf: &[u8]) -> anyhow::Result<()> {
        let msg = HeartbeatMessage::deser(buf)?;
        self.channel.send((sender, msg)).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageModule for HeartbeatMessageModule {
    fn id(&self) -> MessageModuleId {
        HEARTBEAT_MESSAGE_MODULE_ID
    }

    async fn on_message(&self, sender: NodeAddr, buf: &[u8]) {
        if let Err(e) = self._on_message(sender, buf).await {
            error!("error handling message: {}", e);
        }
    }
}


const ID_HEARTBEAT: u8 = 1;
const ID_HEARTBEAT_RESPONSE: u8 = 2;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum HeartbeatMessage {
    Heartbeat(HeartbeatData),
    HeartbeatResponse(HeartbeatResponseData),
}
impl Message for HeartbeatMessage {
    fn module_id(&self) -> MessageModuleId {
        HEARTBEAT_MESSAGE_MODULE_ID
    }

    fn ser(&self, buf: &mut BytesMut) {
        buf.put_u8(self.id());
        match self {
            HeartbeatMessage::Heartbeat(data) => Self::ser_heartbeat(data, buf),
            HeartbeatMessage::HeartbeatResponse(data) => Self::ser_heartbeat_response(data, buf),
        }
    }

    fn box_clone(&self) -> Arc<dyn Any + Send + Sync + 'static> {
        Arc::new(self.clone())
    }
}

impl HeartbeatMessage {
    pub fn id(&self) -> u8 {
        match self {
            HeartbeatMessage::Heartbeat(_) => ID_HEARTBEAT,
            HeartbeatMessage::HeartbeatResponse(_) => ID_HEARTBEAT_RESPONSE,
        }
    }

    fn ser_heartbeat(data: &HeartbeatData, buf: &mut impl BufMut) {
        buf.put_u64(data.timestamp_nanos);
    }

    fn ser_heartbeat_response(data: &HeartbeatResponseData, buf: &mut impl BufMut) {
        buf.put_u64(data.timestamp_nanos);
    }

    //TODO &mut impl Buf
    pub fn deser(buf: &[u8]) -> anyhow::Result<HeartbeatMessage> {
        let mut buf = buf;
        match buf.try_get_u8()? {
            ID_HEARTBEAT => Self::deser_heartbeat(buf),
            ID_HEARTBEAT_RESPONSE => Self::deser_heartbeat_response(buf),
            id => Err(anyhow!("invalid message discriminator {}", id)),
        }
    }


    fn deser_heartbeat(mut buf: &[u8]) -> anyhow::Result<HeartbeatMessage> {
        let timestamp_nanos = buf.try_get_u64()?;

        Ok(HeartbeatMessage::Heartbeat(HeartbeatData {
            timestamp_nanos,
        }))
    }

    fn deser_heartbeat_response(mut buf: &[u8]) -> anyhow::Result<HeartbeatMessage> {
        let timestamp_nanos = buf.try_get_u64()?;

        Ok(HeartbeatMessage::HeartbeatResponse(HeartbeatResponseData {
            timestamp_nanos,
        }))
    }
}


#[derive(Eq, PartialEq, Debug, Clone)]
pub struct HeartbeatData {
    pub timestamp_nanos: u64,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct HeartbeatResponseData {
    pub timestamp_nanos: u64,
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use HeartbeatMessage::*;

    use super::*;

    #[rstest]
    #[case::heartbeat(Heartbeat(HeartbeatData { timestamp_nanos: 5}), ID_HEARTBEAT)]
    #[case::heartbeat_response(HeartbeatResponse(HeartbeatResponseData { timestamp_nanos: 5}), ID_HEARTBEAT_RESPONSE)]
    fn test_ser_cluster_message(#[case] msg: HeartbeatMessage, #[case] msg_id: u8) {
        assert_eq!(msg.id(), msg_id);

        let mut buf = BytesMut::new();
        msg.ser(&mut buf);
        let deser_msg = HeartbeatMessage::deser(&buf).unwrap();
        assert_eq!(msg, deser_msg);
    }
}
