use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use rustc_hash::FxHasher;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use super::gossip_messages::*;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_state::{ClusterState, NodeState};
use crate::messaging::node_addr::NodeAddr;
use crate::util::random::{Random, RngRandom};

pub struct Gossip<R: Random> {
    config: Arc<ClusterConfig>,
    myself: NodeAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    pd: PhantomData<R>,
}
impl Gossip<RngRandom> {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>) -> Gossip<RngRandom> {
        Gossip {
            config,
            myself,
            cluster_state,
            pd: PhantomData::default(),
        }
    }
}
impl <R: Random> Gossip<R> {
    pub fn new_with_random(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>) -> Gossip<R> {
        Gossip {
            config,
            myself,
            cluster_state,
            pd: PhantomData::default(),
        }
    }

    /// separates nodes into two groups based on their "seen by" sets:
    ///
    ///  * nodes with all nodes in their "seen by" sets, i.e. nodes about which there is potential
    ///     consensus
    ///  * nodes with an incomplete "seen by" set, i.e. nodes about there is guaranteed to be no
    ///     consensus
    ///
    /// Nodes without consensus are given a higher probability for being gossip targets.
    ///
    /// NB: Since this function is about picking targets for gossip, only potential gossip partners
    ///      are included - i.e. myself is excluded as well as unreachable nodes or those with
    ///      non-gossip-eligible membership states.
    async fn gossip_candidates_by_differing_state(&self) -> (Vec<NodeAddr>, Vec<NodeAddr>) {
        let cluster_state = self.cluster_state.read().await;

        let mut maybe_same = Vec::new();
        let mut proven_different = Vec::new();

        for candidate in cluster_state.node_states()
            .filter(|n| n.is_reachable())
            .filter(|n| n.addr != self.myself)
            .filter(|n| n.membership_state.is_gossip_partner())
            .map(|n| n.addr)
        {
            if cluster_state.is_node_converged(candidate) {
                debug!("+ {:?}", candidate);
                maybe_same.push(candidate);
            }
            else {
                debug!("- {:?}", candidate);
                proven_different.push(candidate);
            }
        }

        (maybe_same, proven_different)
    }

    async fn gossip_summary_digest(&self) -> GossipSummaryDigestData {
        Self::gossip_summary_digest_for_cluster_state(self.cluster_state.read().await.node_states())
    }

    fn gossip_summary_digest_for_cluster_state<'a>(node_states: impl Iterator<Item=&'a NodeState>) -> GossipSummaryDigestData {
        let mut sha256 = Sha256::default();

        {
            fn hash_node_addr(sha256: &mut Sha256, addr: NodeAddr) {
                sha256.update(addr.unique.to_le_bytes());
                match addr.socket_addr {
                    SocketAddr::V4(data) => {
                        sha256.update(data.ip().to_bits().to_le_bytes());
                        sha256.update(data.port().to_le_bytes());
                    }
                    SocketAddr::V6(data) => {
                        sha256.update(data.ip().to_bits().to_le_bytes());
                        sha256.update(data.port().to_le_bytes());
                    }
                }
            }

            for s in node_states {
                hash_node_addr(&mut sha256, s.addr);
                sha256.update(&[s.membership_state.into()]);

                for role in &s.roles {
                    sha256.update((role.len() as u64).to_le_bytes());
                    sha256.update(role.as_bytes());
                }

                for (&addr, r) in &s.reachability {
                    hash_node_addr(&mut sha256, addr);
                    sha256.update(r.counter_of_reporter.to_le_bytes());
                    sha256.update(if r.is_reachable { &[1u8] } else { &[0u8] });
                }

                for &addr in &s.seen_by {
                    hash_node_addr(&mut sha256, addr);
                }
            }
        }

        GossipSummaryDigestData {
            full_sha256_digest: sha256.finalize().into(),
        }
    }

    async fn gossip_detailed_digest(&self) -> GossipDetailedDigestData {
        let nonce = R::next_u32();
        gossip_detailed_digest_with_given_nonce(&*self.cluster_state.read().await, nonce)
    }

    pub async fn gossip_partners(&self) -> Vec<(NodeAddr, Arc<GossipMessage>)> {
        let mut result = Vec::with_capacity(self.config.num_gossip_partners);

        let (mut maybe_same, mut proven_different) = self.gossip_candidates_by_differing_state().await;

        let mut summary_digest_message = None;
        let mut detailed_digest_message = None;

        for _ in 0..self.config.num_gossip_partners {
            // give more weight to nodes with a state that is proven to be different, but give
            //  nodes without known differences a non-zero probability

            let use_proven_different = if maybe_same.is_empty() {
                if proven_different.is_empty() {
                    break;
                }
                true
            }
            else if proven_different.is_empty() {
                false
            }
            else {
                R::gen_f64_range(0.0 .. 1.0) < self.config.gossip_with_differing_state_probability
            };

            if use_proven_different {
                let idx = R::gen_usize_range(0..proven_different.len());
                let addr = proven_different.remove(idx);
                let msg = if let Some(msg) = &detailed_digest_message {
                    Arc::clone(msg)
                }
                else {
                    let msg = Arc::new(GossipMessage::GossipDetailedDigest(self.gossip_detailed_digest().await));
                    detailed_digest_message = Some(msg.clone());
                    msg
                };

                result.push((addr, msg));
            }
            else {
                let idx = R::gen_usize_range(0..maybe_same.len());
                let addr = maybe_same.remove(idx);
                let msg = if let Some(msg) = &summary_digest_message {
                    Arc::clone(msg)
                }
                else {
                    let msg = Arc::new(GossipMessage::GossipSummaryDigest(self.gossip_summary_digest().await));
                    summary_digest_message = Some(msg.clone());
                    msg
                };
                result.push((addr, msg));
            }
        }

        result
    }

    pub async fn on_summary_digest(&self, other_digest: &GossipSummaryDigestData) -> Option<GossipDetailedDigestData> {
        debug!("received gossip summary digest message");
        let own_digest = self.gossip_summary_digest().await;
        if own_digest.full_sha256_digest == other_digest.full_sha256_digest {
            return None;
        }

        Some(self.gossip_detailed_digest().await)
    }

    //TODO debug logging for gossip

    pub async fn on_detailed_digest(&self, other_digest: &GossipDetailedDigestData) -> Option<GossipDifferingAndMissingNodesData> {
        debug!("received gossip detailed digest message");
        let cluster_state = self.cluster_state.read().await;

        //NB: we don't want anyone else to change state between hashing and comparing the hashes, so we get the lock once at
        //     the start
        let own_digest = gossip_detailed_digest_with_given_nonce(&*cluster_state, other_digest.nonce);

        // my own data for nodes that hash differently from the gossip partner's hash
        let differing: Vec<NodeState> = own_digest.nodes.iter()
            .filter(|(addr, &hash)| Some(&hash) != other_digest.nodes.get(addr))
            .flat_map(|(addr, _)| cluster_state.get_node_state(addr))
            .cloned()
            .collect();

        // nodes that are apparently present on the remote node but not locally
        let missing: Vec<NodeAddr> = other_digest.nodes.keys()
            .filter(|addr| !own_digest.nodes.contains_key(addr))
            .copied()
            .collect();

        if differing.is_empty() && missing.is_empty() {
            None
        }
        else {
            Some(GossipDifferingAndMissingNodesData {
                differing,
                missing,
            })
        }
    }

    pub async fn on_differing_and_missing_nodes(&self, other_data: GossipDifferingAndMissingNodesData) -> Option<GossipNodesData> {
        let differing_keys = other_data.differing.iter()
            .map(|n| n.addr)
            .collect::<Vec<_>>();

        debug!("received gossip with differing / missing nodes: {:?} / {:?}",
            differing_keys,
            other_data.missing);

        let mut response_nodes = Vec::new();

        let mut cluster_state = self.cluster_state.write().await;
        for s in other_data.differing {
            let other_node = s.clone();
            cluster_state.merge_node_state(s).await;

            if let Some(merged) = cluster_state.get_node_state(&other_node.addr) {
                if merged != &other_node {
                    response_nodes.push(merged.clone());
                }
            }
        }

        for missing in &other_data.missing {
            if let Some(state) = cluster_state.get_node_state(missing) {
                response_nodes.push(state.clone());
            }
        }

        if response_nodes.is_empty() {
            None
        }
        else {
            Some(GossipNodesData {
                nodes: response_nodes,
            })
        }
    }

    pub async fn on_nodes(&self, other_data: GossipNodesData) {
        debug!("received gossip nodes message");
        let mut cluster_state = self.cluster_state.write().await;
        for s in other_data.nodes {
            cluster_state.merge_node_state(s).await;
        }
    }

    pub async fn down_myself(&self) {
        debug!("received 'down yourself' message");
        self.cluster_state.write().await
            .promote_myself_to_down().await
    }
}

fn gossip_detailed_digest_with_given_nonce(cluster_state: &ClusterState, nonce: u32) -> GossipDetailedDigestData {
    let mut nodes: BTreeMap<NodeAddr, u64> = Default::default();

    for s in cluster_state.node_states() {
        let mut hasher = FxHasher::with_seed(nonce as usize); //TODO we assume at least 32-bit architecture - how to ensure it once and for all?

        // no need to add the node address to the hash (it's the key in the returned map of
        //  hashes), or the roles (they're supposed to be immutable anyway)

        Into::<u8>::into(s.membership_state).hash(&mut hasher);

        for (a, r) in &s.reachability {
            a.hash(&mut hasher);
            r.hash(&mut hasher);
        }

        for sb in &s.seen_by {
            sb.hash(&mut hasher);
        }

        nodes.insert(s.addr, hasher.finish());
        trace!("hashing {:?} with nonce {}: {}", s, nonce, nodes.get(&s.addr).unwrap());
    }

    GossipDetailedDigestData {
        nonce,
        nodes,
    }
}


#[cfg(test)]
mod tests {
    use crate::cluster::cluster_config::ClusterConfig;
    use crate::cluster::cluster_events::ClusterEventNotifier;
    use crate::cluster::cluster_state::MembershipState::*;
    use crate::cluster::cluster_state::{ClusterState, MembershipState, NodeReachability, NodeState};
    use crate::cluster::gossip::gossip_logic::{gossip_detailed_digest_with_given_nonce, Gossip};
    use crate::cluster::gossip::gossip_messages::{GossipDetailedDigestData, GossipDifferingAndMissingNodesData, GossipNodesData, GossipSummaryDigestData};
    use crate::node_state;
    use crate::test_util::node::test_node_addr_from_number;
    use crate::util::random::{MockRandom, MOCK_RANDOM_MUTEX};
    use rstest::rstest;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::runtime::Builder;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_gossip_candidates_by_differing_state() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));

        {
            let mut cl = cluster_state.write().await;
            cl.merge_node_state(node_state!(2[]:Up->[]@[1,2,3,4,5])).await;
            cl.merge_node_state(node_state!(3[]:Up->[]@[1,  3,4  ])).await;        // non-converged
            cl.merge_node_state(node_state!(4[]:Up->[]@[1,2,3,4  ])).await;        // non-converged (unreachable must have seen)
            cl.merge_node_state(node_state!(5[]:Up->[1:false@6]@[1,2,3,4])).await; // unreachable
            cl.merge_node_state(node_state!(6[]:Down->[]@[1,2,3,4,5])).await;      // non-gossip membership state
        }

        let gossip = Gossip::new(myself, config.clone(), cluster_state.clone());

        let (converged, not_converged) = gossip.gossip_candidates_by_differing_state().await;

        assert_eq!(converged, vec![
            test_node_addr_from_number(2),
        ]);
        assert_eq!(not_converged, vec![
            test_node_addr_from_number(3),
            test_node_addr_from_number(4),
        ]);
    }

    #[tokio::test]
    async fn test_gossip_summary_digest() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        cluster_state.write().await
            .merge_node_state(node_state!(2["a", "b"]:Up->[7:false@88]@[1,2])).await;
        let gossip = Gossip::new(myself, config.clone(), cluster_state.clone());

        let digest = gossip.gossip_summary_digest().await;
        assert_eq!(digest, GossipSummaryDigestData {
            full_sha256_digest: [56, 159, 183, 220, 160, 198, 187, 159, 36, 169, 181, 155, 139, 38, 154, 149, 93, 23, 150, 94, 28, 235, 227, 61, 177, 116, 119, 82, 220, 156, 26, 13],
        });
    }

    #[rstest]
    #[case(7, vec![(1, 12337464493871681589), (2, 6689209898252340538)])]
    #[case(9, vec![(1, 2509790823383955335), (2, 2735948127633228801)])]
    fn test_gossip_detailed_digest(#[case] nonce: u32, #[case] nodes: Vec<(u16, u64)>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let _lock = MOCK_RANDOM_MUTEX.lock();

            let nodes = nodes.into_iter()
                .map(|(n, d)| (test_node_addr_from_number(n), d))
                .collect::<BTreeMap<_, _>>();

            let myself = test_node_addr_from_number(1);
            let config = Arc::new(ClusterConfig::new(myself.socket_addr));
            let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
            cluster_state.write().await
                .merge_node_state(node_state!(2["a", "b"]:Up->[7:false@88]@[1,2])).await;

            let ctx = MockRandom::next_u32_context();
            ctx.expect()
                .returning(move || nonce);

            let gossip = Gossip::<MockRandom>::new_with_random(myself, config.clone(), cluster_state.clone());

            let digest = gossip.gossip_detailed_digest().await;

            assert_eq!(digest, GossipDetailedDigestData {
                nonce,
                nodes,
            });
        });
    }

    #[tokio::test]
    async fn test_gossip_detailed_digest_with_given_nonce() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        cluster_state.write().await
            .merge_node_state(node_state!(2["a", "b"]:Up->[7:false@88]@[1,2])).await;

        let digest = gossip_detailed_digest_with_given_nonce(&*cluster_state.read().await, 7);
        assert_eq!(digest, GossipDetailedDigestData {
            nonce: 7,
            nodes: [
                (test_node_addr_from_number(1), 12337464493871681589),
                (test_node_addr_from_number(2), 6689209898252340538),
            ].into(),
        });
    }

    #[test]
    fn test_gossip_partners() {
        todo!()
    }

    #[rstest]
    #[case([1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,], Some(vec![(1, 12337464493871681589), (2, 6689209898252340538)]))]
    #[case([56, 159, 183, 220, 160, 198, 187, 159, 36, 169, 181, 155, 139, 38, 154, 149, 93, 23, 150, 94, 28, 235, 227, 61, 177, 116, 119, 82, 220, 156, 26, 13], None)]
    fn test_on_summary_digest(#[case] digest: [u8;32], #[case] expected: Option<Vec<(u16,u64)>>) {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let _lock = MOCK_RANDOM_MUTEX.lock();

            let myself = test_node_addr_from_number(1);
            let config = Arc::new(ClusterConfig::new(myself.socket_addr));
            let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
            cluster_state.write().await
                .merge_node_state(node_state!(2["a", "b"]:Up->[7:false@88]@[1,2])).await;
            let gossip = Gossip::<MockRandom>::new_with_random(myself, config, cluster_state.clone());

            let context = MockRandom::next_u32_context();
            context.expect()
                .returning(|| 7);

            let actual = gossip.on_summary_digest(&GossipSummaryDigestData {
                full_sha256_digest: digest,
            }).await;

            let expected = expected.map(|d| GossipDetailedDigestData {
                nonce: 7,
                nodes: d.into_iter()
                    .map(|(n,h)| (test_node_addr_from_number(n), h))
                    .collect()
                ,
            });

            assert_eq!(actual, expected);
        });
    }

    #[rstest]
    #[case::stable(vec![(1,12416313083010759684), (2,12416313083010759684)], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![], vec![])]
    #[case::local_only(vec![(1,12416313083010759684)], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![node_state!(2[]:Up->[]@[1,2])], vec![])]
    #[case::remote_only(vec![(1,12416313083010759684), (2,12416313083010759684)], vec![node_state!(1[]:Up->[]@[1,2])], vec![], vec![2])]
    #[case::different(vec![(1,12416313083010759684), (2,123)], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])], vec![node_state!(2[]:Up->[]@[1,2])], vec![])]
    #[case::mix(vec![(1,12416313083010759684), (2,12416313083010759684), (3,123)], vec![node_state!(1[]:Up->[]@[1,2]), node_state!(3[]:Up->[]@[1,2]), node_state!(4[]:Up->[]@[1,2])], vec![node_state!(3[]:Up->[]@[1,2]), node_state!(4[]:Up->[]@[1,2])], vec![2])]
    fn test_on_detailed_digest(
        #[case] nodes_in_gossip: Vec<(u16, u64)>,
        #[case] local_nodes: Vec<NodeState>,
        #[case] expected_differing: Vec<NodeState>,
        #[case] expected_missing: Vec<u16>,
    ) {
        let expected_missing = expected_missing.into_iter()
            .map(|n| test_node_addr_from_number(n))
            .collect::<Vec<_>>();

        let rt = Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let _lock = MOCK_RANDOM_MUTEX.lock();

            let myself = test_node_addr_from_number(1);
            let config = Arc::new(ClusterConfig::new(myself.socket_addr));
            let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
            for n in local_nodes {
                cluster_state.write().await
                    .merge_node_state(n).await;
            }
            let gossip = Gossip::<MockRandom>::new_with_random(myself, config, cluster_state.clone());

            let context = MockRandom::next_u32_context();
            context.expect()
                .returning(move || 7);

            let actual_response = gossip.on_detailed_digest(&GossipDetailedDigestData {
                nonce: 7,
                nodes: nodes_in_gossip.into_iter()
                    .map(|(n,h)| (test_node_addr_from_number(n), h))
                    .collect(),
            }).await;

            if expected_differing.is_empty() && expected_missing.is_empty() {
                assert!(actual_response.is_none());
            }
            else {
                let actual_response = actual_response.unwrap();

                assert_eq!(actual_response.differing, expected_differing);
                assert_eq!(actual_response.missing, expected_missing);
            }
        });
    }

    #[rstest]
    #[case::newer_locally(
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,3])],
        vec![node_state!(2[]:Joining->[]@[1,2])],
        vec![],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,3])],
        vec![node_state!(2[]:Up->[]@[1,3])])]
    #[case::newer_remote(
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Joining->[]@[1,3])],
        vec![node_state!(2[]:Up->[]@[2,3])],
        vec![],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2,3])],
        vec![node_state!(2[]:Up->[]@[1,2,3])])]
    #[case::newer_remote_seen_by_self( // should not happen regularly - this is a robustness corner case
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Joining->[]@[1,3])],
        vec![node_state!(2[]:Up->[]@[1,2,3])],
        vec![],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2,3])],
        vec![])]
    #[case::not_present_locally( // should not happen regularly - this is a robustness corner case
        vec![node_state!(1[]:Up->[]@[1,2])],
        vec![node_state!(2[]:Up->[]@[2,3])],
        vec![],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2,3])],
        vec![node_state!(2[]:Up->[]@[1,2,3])])]
    #[case::same( // should not happen regularly - this is a robustness corner case
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])],
        vec![node_state!(2[]:Up->[]@[1,2])],
        vec![],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])],
        vec![])]
    #[case::missing(
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(3[]:Up->[]@[1,3])],
        vec![],
        vec![3],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(3[]:Up->[]@[1,3])],
        vec![node_state!(3[]:Up->[]@[1,3])])]
    #[case::non_existing_missing( // should not happen regularly - this is a robustness corner case
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(3[]:Up->[]@[1,3])],
        vec![],
        vec![4],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(3[]:Up->[]@[1,3])],
        vec![])]
    #[case::mixed(
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2])],
        vec![node_state!(3[]:Joining->[]@[2,3])],
        vec![2],
        vec![node_state!(1[]:Up->[]@[1,2]), node_state!(2[]:Up->[]@[1,2]), node_state!(3[]:Joining->[]@[1,2,3])],
        vec![node_state!(3[]:Joining->[]@[1,2,3]), node_state!(2[]:Up->[]@[1,2])])]
    fn test_on_differing_and_missing_nodes(
        #[case] local_nodes: Vec<NodeState>,
        #[case] msg_differing: Vec<NodeState>,
        #[case] msg_missing: Vec<u16>,
        #[case] expected_merged: Vec<NodeState>,
        #[case] expected_response: Vec<NodeState>,
    ) {
        let msg_missing = msg_missing.into_iter()
            .map(|n| test_node_addr_from_number(n))
            .collect::<Vec<_>>();

        let rt = Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let myself = test_node_addr_from_number(1);
            let config = Arc::new(ClusterConfig::new(myself.socket_addr));
            let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
            for n in local_nodes {
                cluster_state.write().await
                    .merge_node_state(n).await;
            }
            let gossip = Gossip::new(myself, config, cluster_state.clone());

            let actual_response = gossip.on_differing_and_missing_nodes(GossipDifferingAndMissingNodesData {
                differing: msg_differing,
                missing: msg_missing,
            }).await;

            let actual_nodes = cluster_state.read().await
                .node_states()
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(actual_nodes, expected_merged);

            if expected_response.is_empty() {
                assert!(actual_response.is_none());
            }
            else {
                assert_eq!(actual_response, Some(GossipNodesData {
                    nodes: expected_response,
                }));
            }
        });

        // merge provided nodes
        // return missing and modified (after merging) nodes, if any
    }

    #[tokio::test]
    async fn test_on_nodes() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        let gossip = Gossip::new(myself, config, cluster_state.clone());

        gossip.on_nodes(GossipNodesData {
            nodes: vec![
                node_state!(1[]:Up->[]@[1,2]),
                node_state!(2[]:Up->[]@[1,2]),
            ],
        }).await;

        assert_eq!(
            cluster_state.read().await
                .get_node_state(&myself).unwrap(),
            &node_state!(1[]:Up->[]@[1,2]),
        );
        assert_eq!(
            cluster_state.read().await
                .get_node_state(&test_node_addr_from_number(2)).unwrap(),
            &node_state!(2[]:Up->[]@[1,2]),
        );
    }

    #[tokio::test]
    async fn test_down_myself() {
        let myself = test_node_addr_from_number(1);
        let config = Arc::new(ClusterConfig::new(myself.socket_addr));
        let cluster_state = Arc::new(RwLock::new(ClusterState::new(myself, config.clone(), Arc::new(ClusterEventNotifier::new()))));
        let gossip = Gossip::new(myself, config, cluster_state.clone());

        gossip.down_myself().await;

        assert_eq!(
            cluster_state.read().await
                .get_node_state(&myself).unwrap()
                .membership_state,
            MembershipState::Down
        );
    }
}
