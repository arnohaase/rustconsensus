use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::{select, time};
use tracing::debug;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::ClusterState;
use crate::cluster::gossip::gossip_logic::Gossip;
use crate::cluster::gossip::gossip_messages::{GossipDetailedDigestData, GossipDifferingAndMissingNodesData, GossipMessage, GossipMessageModule, GossipNodesData, GossipSummaryDigestData};
use crate::messaging::messaging::{MessageSender, Messaging};
use crate::messaging::node_addr::NodeAddr;
use crate::util::random::Random;

pub mod gossip_messages;
mod gossip_logic;


#[cfg_attr(test, mockall::automock)]
#[async_trait]
trait GossipApi {
    async fn on_summary_digest(&self, data: &GossipSummaryDigestData) -> Option<GossipDetailedDigestData>;
    async fn on_detailed_digest(&self, data: &GossipDetailedDigestData) -> Option<GossipDifferingAndMissingNodesData>;
    async fn on_differing_and_missing_nodes(&self, data: GossipDifferingAndMissingNodesData) -> Option<GossipNodesData>;
    async fn on_nodes(&self, data: GossipNodesData);
    async fn down_myself(&self);

    async fn gossip_partners(&self) -> Vec<(NodeAddr, Arc<GossipMessage>)>;
}

#[async_trait]
impl <R: Random> GossipApi for Gossip<R> {
    async fn on_summary_digest(&self, data: &GossipSummaryDigestData) -> Option<GossipDetailedDigestData> {
        Self::on_summary_digest(self, data).await
    }

    async fn on_detailed_digest(&self, data: &GossipDetailedDigestData) -> Option<GossipDifferingAndMissingNodesData> {
        Self::on_detailed_digest(&self, data).await
    }

    async fn on_differing_and_missing_nodes(&self, data: GossipDifferingAndMissingNodesData) -> Option<GossipNodesData> {
        Self::on_differing_and_missing_nodes(&self, data).await
    }

    async fn on_nodes(&self, data: GossipNodesData) {
        Self::on_nodes(self, data).await
    }

    async fn down_myself(&self) {
        Self::down_myself(self).await
    }

    async fn gossip_partners(&self) -> Vec<(NodeAddr, Arc<GossipMessage>)> {
        Self::gossip_partners(self).await
    }
}

//TODO unit test for gossip loop
pub async fn run_gossip<M: Messaging>(config: Arc<ClusterConfig>, messaging: Arc<M>, cluster_state: Arc<RwLock<ClusterState>>) -> anyhow::Result<()> {
    let myself = messaging.get_self_addr();

    let (send, mut recv) = mpsc::channel(32);

    let message_module = GossipMessageModule::new(send);
    messaging.register_module(message_module.clone());
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

async fn do_gossip<M: MessageSender>(gossip: &impl GossipApi, messaging: &M) { //TODO move somewhere else
    debug!("periodic gossip");
    let gossip_partners = gossip.gossip_partners().await;
    for (addr, msg) in gossip_partners {
        debug!("sending gossip message to {:?}", addr);
        messaging.send_to_node(addr, msg.as_ref()).await
            .expect("message length upper bound should be configured big enough for gossip");
    }
}

async fn on_gossip_message<M: MessageSender>(msg: GossipMessage, sender: NodeAddr, gossip: &mut impl GossipApi, messaging: &M) {
    use GossipMessage::*;

    match msg {
        GossipSummaryDigest(digest) => {
            if let Some(response) = gossip.on_summary_digest(&digest).await {
                messaging.send_to_node(sender, &GossipDetailedDigest(response)).await
                    .expect("message length upper bound should be configured big enough for gossip");
            }
        }
        GossipDetailedDigest(digest) => {
            if let Some(response) = gossip.on_detailed_digest(&digest).await {
                messaging.send_to_node(sender, &GossipDifferingAndMissingNodes(response)).await
                    .expect("message length upper bound should be configured big enough for gossip");
            }
        }
        GossipDifferingAndMissingNodes(data) => {
            if let Some(response) = gossip.on_differing_and_missing_nodes(data).await {
                messaging.send_to_node(sender, &GossipNodes(response)).await
                    .expect("message length upper bound should be configured big enough for gossip");
            }
        }
        GossipNodes(data) => {
            gossip.on_nodes(data).await
        }
        DownYourself => {
            gossip.down_myself().await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::gossip::gossip_messages::{GossipDetailedDigestData, GossipDifferingAndMissingNodesData, GossipMessage, GossipNodesData, GossipSummaryDigestData};
    use crate::cluster::gossip::{do_gossip, on_gossip_message, MockGossipApi};
    use crate::node_state;
    use crate::test_util::message::TrackingMockMessageSender;
    use crate::test_util::node::test_node_addr_from_number;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    #[rstest]
    #[case::empty(vec![])]
    #[case::one(vec![(2, GossipMessage::DownYourself)])]
    #[case::two(vec![(27, GossipMessage::GossipNodes(GossipNodesData{nodes: vec![]})), (4, GossipMessage::DownYourself)])]
    fn test_do_gossip(#[case] gossip_partners: Vec<(u16, GossipMessage)>) {
        let gossip_partners = gossip_partners.into_iter()
            .map(|(n, m)| (test_node_addr_from_number(n), Arc::new(m)))
            .collect::<Vec<_>>();

        let rt = Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);

            let mut gossip = MockGossipApi::new();
            gossip.expect_gossip_partners()
                .once()
                .return_const(gossip_partners.clone());

            let messaging = TrackingMockMessageSender::new(myself);

            do_gossip(&gossip, &messaging).await;

            for (addr, msg) in gossip_partners {
                messaging.assert_message_sent(addr, msg.as_ref().clone()).await;
            }
            messaging.assert_no_remaining_messages().await;
        });
    }

    #[tokio::test]
    async fn test_gossip_message_summary_digest() {
        let myself = test_node_addr_from_number(1);
        let data = GossipSummaryDigestData {
            full_sha256_digest: [7;32],
        };
        let response_data = GossipDetailedDigestData {
            nonce: 123,
            nodes: [(test_node_addr_from_number(5), 9988)].into(),
        };
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_summary_digest()
            .once()
            .with(eq(data.clone()))
            .return_const(Some(response_data.clone()));

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipSummaryDigest(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_message_sent(addr, GossipMessage::GossipDetailedDigest(response_data)).await;
        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_message_summary_digest_none() {
        let myself = test_node_addr_from_number(1);
        let data = GossipSummaryDigestData {
            full_sha256_digest: [7;32],
        };
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_summary_digest()
            .once()
            .with(eq(data.clone()))
            .return_const(None);

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipSummaryDigest(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_message_detailed_digest() {
        let myself = test_node_addr_from_number(1);
        let data = GossipDetailedDigestData {
            nonce: 123,
            nodes: [(test_node_addr_from_number(5), 9988)].into(),
        };
        let addr = test_node_addr_from_number(7);
        let response_data = GossipDifferingAndMissingNodesData {
            differing: vec![],
            missing: vec![test_node_addr_from_number(3)],
        };

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_detailed_digest()
            .once()
            .with(eq(data.clone()))
            .return_const(Some(response_data.clone()));

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipDetailedDigest(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_message_sent(addr, GossipMessage::GossipDifferingAndMissingNodes(response_data)).await;
        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_message_detailed_digest_none() {
        let myself = test_node_addr_from_number(1);
        let data = GossipDetailedDigestData {
            nonce: 123,
            nodes: [(test_node_addr_from_number(5), 9988)].into(),
        };
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_detailed_digest()
            .once()
            .with(eq(data.clone()))
            .return_const(None);

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipDetailedDigest(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_message_differing_and_missing_nodes() {
        use crate::cluster::cluster_state::MembershipState::Up;

        let myself = test_node_addr_from_number(1);
        let data = GossipDifferingAndMissingNodesData {
            differing: vec![],
            missing: vec![test_node_addr_from_number(3)],
        };
        let addr = test_node_addr_from_number(7);
        let response_data = GossipNodesData {
            nodes: vec![node_state!(3[]:Up->[]@[1,3])],
        };

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_differing_and_missing_nodes()
            .once()
            .with(eq(data.clone()))
            .return_const(Some(response_data.clone()));

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipDifferingAndMissingNodes(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_message_sent(addr, GossipMessage::GossipNodes(response_data)).await;
        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_message_differing_and_missing_nodes_none() {
        let myself = test_node_addr_from_number(1);
        let data = GossipDifferingAndMissingNodesData {
            differing: vec![],
            missing: vec![test_node_addr_from_number(3)],
        };
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_differing_and_missing_nodes()
            .once()
            .with(eq(data.clone()))
            .return_const(None);

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipDifferingAndMissingNodes(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_nodes() {
        use crate::cluster::cluster_state::MembershipState::Up;

        let myself = test_node_addr_from_number(1);
        let data = GossipNodesData {
            nodes: vec![node_state!(3[]:Up->[]@[1,3])],
        };
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_on_nodes()
            .once()
            .with(eq(data.clone()))
            .return_const(());

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::GossipNodes(data.clone()), addr, &mut gossip, &messaging).await;

        messaging.assert_no_remaining_messages().await;
    }

    #[tokio::test]
    async fn test_gossip_down_yourself() {
        let myself = test_node_addr_from_number(1);
        let addr = test_node_addr_from_number(7);

        let mut gossip = MockGossipApi::new();
        gossip.expect_down_myself()
            .once()
            .return_const(());

        let messaging = TrackingMockMessageSender::new(myself);

        on_gossip_message(GossipMessage::DownYourself, addr, &mut gossip, &messaging).await;

        messaging.assert_no_remaining_messages().await;
    }
}
