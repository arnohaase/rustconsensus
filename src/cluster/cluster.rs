use std::net::ToSocketAddrs;
use std::sync::Arc;
use bytes::BytesMut;
use tokio::select;

use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_driver::run_cluster;
use crate::cluster::cluster_events::{ClusterEventListener, ClusterEventNotifier};
use crate::cluster::cluster_messages::ClusterMessageModule;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::Gossip;
use crate::cluster::heartbeat::HeartBeat;
use crate::cluster::join_messages::{JoinMessage, JoinMessageModule};
use crate::messaging::message_module::MessageModule;
use crate::messaging::messaging::{JOIN_MESSAGE_MODULE_ID, Messaging};

/// This is the cluster's public API
pub struct Cluster {
    config: Arc<ClusterConfig>,
    messaging: Arc<Messaging>,
    event_notifier: Arc<ClusterEventNotifier>,
}
impl Cluster {
    pub fn new(config: Arc<ClusterConfig>, messaging: Arc<Messaging>) -> Cluster {
        Cluster {
            config,
            messaging,
            event_notifier: Arc::new(ClusterEventNotifier::new()),
        }
    }

    pub async fn run(&self, to_join: Option<impl ToSocketAddrs>) -> anyhow::Result<()> {
        select! {
            r = self.messaging.recv() => r,
            r = self._run(to_join) => r,
        }
    }

    async fn _run(&self, to_join: Option<impl ToSocketAddrs>) -> anyhow::Result<()> {
        let myself = self.messaging.get_self_addr();
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, self.config.clone(), self.event_notifier.clone())));
        let heart_beat = Arc::new(RwLock::new(HeartBeat::new(myself, self.config.clone())));
        let gossip = Arc::new(RwLock::new(Gossip::new(myself, self.config.clone(), cluster_state.clone())));

        if let Some(to_join) = to_join {
            let mut join_msg_buf = BytesMut::new();
            JoinMessage::Join.ser(&mut join_msg_buf);

            for addr in to_join.to_socket_addrs()? {
                info!("trying to join cluster at {}", addr);
                self.messaging.send(addr.into(), JOIN_MESSAGE_MODULE_ID, &join_msg_buf).await?;
            }
        }


        debug!("registering cluster message module");
        let cluster_messaging = ClusterMessageModule::new(gossip.clone(), self.messaging.clone(), heart_beat.clone());
        self.messaging.register_module(cluster_messaging.clone()).await?;
        debug!("registering cluster join module");
        let join_messaging = JoinMessageModule::new(cluster_state.clone());
        self.messaging.register_module(join_messaging.clone()).await?;

        run_cluster(self.config.clone(), cluster_state, heart_beat, gossip, self.messaging.clone()).await;

        debug!("deregistering cluster join module");
        self.messaging.deregister_module(join_messaging.id()).await?;
        debug!("deregistering cluster message module");
        self.messaging.deregister_module(cluster_messaging.id()).await
    }

    pub async fn add_listener(&self, listener: Arc<dyn ClusterEventListener>) -> Uuid {
        self.event_notifier.add_listener(listener).await
    }

    pub async fn remove_listener(&self, id: &Uuid) -> anyhow::Result<()> {
        self.event_notifier.try_remove_listener(id).await
    }

    //TODO external API for accessing state
}