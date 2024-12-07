use std::sync::Arc;

use tokio::{select, time};
use tokio::sync::{mpsc, RwLock};
use tracing::debug;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::gossip_logic::Gossip;
use crate::cluster::gossip::gossip_messages::{GossipMessage, GossipMessageModule};
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;

pub mod gossip_messages;
mod gossip_logic;


pub async fn run_gossip<M: Messaging>(config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: Arc<RwLock<ClusterState>>) -> anyhow::Result<()> {
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

async fn do_gossip<M: MessageSender>(gossip: &Gossip, messaging: &M) { //TODO move somewhere else
    debug!("periodic gossip");
    let gossip_partners = gossip.gossip_partners().await;
    for (addr, msg) in gossip_partners {
        debug!("sending gossip message to {:?}", addr);
        messaging.send(addr, msg.as_ref()).await;
    }
}

async fn on_gossip_message<M: MessageSender>(msg: GossipMessage, sender: NodeAddr, gossip: &mut Gossip, messaging: &M) {
    use GossipMessage::*;

    match msg {
        GossipSummaryDigest(digest) => {
            if let Some(response) = gossip.on_summary_digest(&digest).await {
                messaging.send(sender, &GossipDetailedDigest(response)).await;
            }
        }
        GossipDetailedDigest(digest) => {
            if let Some(response) = gossip.on_detailed_digest(&digest).await {
                messaging.send(sender, &GossipDifferingAndMissingNodes(response)).await;
            }
        }
        GossipDifferingAndMissingNodes(data) => {
            if let Some(response) = gossip.on_differing_and_missing_nodes(data).await {
                messaging.send(sender, &GossipNodes(response)).await;
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

#[cfg(test)]
mod test {
    #[test]
    fn test_run_gossip() {
        todo!()
    }

    #[test]
    fn test_do_gossip() {
        todo!()
    }

    #[test]
    fn test_on_gossip_message() {
        todo!()
    }
}