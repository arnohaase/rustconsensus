use std::sync::Arc;

use bytes::BytesMut;
use tokio::{select, time};
use tokio::sync::{mpsc, RwLock};
use tracing::debug;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::gossip_logic::Gossip;
use crate::cluster::gossip::gossip_messages::{GOSSIP_MESSAGE_MODULE_ID, GossipMessage, GossipMessageModule};
use crate::messaging::messaging::Messaging;
use crate::messaging::node_addr::NodeAddr;

pub mod gossip_messages;
mod gossip_logic;


pub async fn run_gossip(config: Arc<ClusterConfig>, messaging: Arc<dyn Messaging>, cluster_state: Arc<RwLock<ClusterState>>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();

    let (send, mut recv) = mpsc::channel(32);

    let message_module = GossipMessageModule::new(send);
    messaging.register_module(message_module.clone()).await?;
    let messaging = messaging.as_ref();

    let mut gossip = Gossip::new(myself, config.clone(), cluster_state.clone());

    let mut converged_send_gossip_ticks = time::interval(config.converged_gossip_interval);
    let mut unconverged_send_gossip_ticks = time::interval(config.unconverged_gossip_interval);

    loop {
        select! {
             opt_msg = recv.recv() => {
                let (sender, msg) = opt_msg.expect("sender and receiver are defined locally - this should never happen");
                on_gossip_message(msg, sender, &mut gossip, messaging).await
            }
            _ = converged_send_gossip_ticks.tick() => {
                if cluster_state.read().await.is_converged() {
                    do_gossip(&gossip, messaging).await
                }
            }
            _ = unconverged_send_gossip_ticks.tick() => {
                if !cluster_state.read().await.is_converged() {
                    do_gossip(&gossip, messaging).await
                }
            }
        }
    }
}

async fn do_gossip(gossip: &Gossip, messaging: &dyn Messaging) { //TODO move somewhere else
    debug!("periodic gossip");
    let gossip_partners = gossip.gossip_partners().await;
    for (addr, msg) in gossip_partners {
        debug!("sending gossip message to {:?}", addr);
        let mut buf = BytesMut::new();
        msg.ser(&mut buf);
        let _ = messaging.send(addr, GOSSIP_MESSAGE_MODULE_ID, &buf).await;
    }

}

//TODO extract sending a message to MessageModule

async fn reply(sender: NodeAddr, message: GossipMessage, messaging: &dyn Messaging) {
    let mut buf = BytesMut::new();
    message.ser(&mut buf);
    let _ = messaging.send(sender, GOSSIP_MESSAGE_MODULE_ID, &buf).await;
}


async fn on_gossip_message(msg: GossipMessage, sender: NodeAddr, gossip: &mut Gossip, messaging: &dyn Messaging) { //TODO move to 'gossip_messages.rs'
    use GossipMessage::*;

    match msg {
        GossipSummaryDigest(digest) => {
            if let Some(response) = gossip.on_summary_digest(&digest).await {
                reply(sender, GossipDetailedDigest(response), messaging).await
            }
        }
        GossipDetailedDigest(digest) => {
            if let Some(response) = gossip.on_detailed_digest(&digest).await {
                reply(sender, GossipDifferingAndMissingNodes(response), messaging).await
            }
        }
        GossipDifferingAndMissingNodes(data) => {
            if let Some(response) = gossip.on_differing_and_missing_nodes(data).await {
                reply(sender, GossipNodes(response), messaging).await
            }
        }
        GossipNodes(data) => {
            gossip.on_nodes(data).await
        }
        DownYourself => {
            gossip.down_yourself().await
        }
    }
}