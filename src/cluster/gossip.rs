use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;

use rand::{Rng, RngCore};
use rustc_hash::{FxHasher, FxHashMap};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_messages::{ClusterMessage, GossipDetailedDigestData, GossipDifferingAndMissingNodesData, GossipNodesData, GossipSummaryDigestData};
use crate::cluster::cluster_state::{ClusterState, NodeState};
use crate::messaging::node_addr::NodeAddr;

pub struct Gossip {
    config: Arc<ClusterConfig>,
    myself: NodeAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
}
impl  Gossip {
    pub fn new(myself: NodeAddr, config: Arc<ClusterConfig>, cluster_state: Arc<RwLock<ClusterState>>) -> Gossip {
        Gossip {
            config,
            myself,
            cluster_state,
        }
    }

    //TODO unit test
    async fn nodes_by_differing_state(&self) -> (Vec<NodeAddr>, Vec<NodeAddr>) {
        let cluster_state = self.cluster_state.read().await;

        let mut maybe_same = Vec::new();
        let mut proven_different = Vec::new();

        for candidate in cluster_state.node_states()
            .filter(|n| n.is_reachable())
            .filter(|n| n.addr != self.myself)
            .filter(|n| n.membership_state.is_gossip_partner())
            .map(|n| n.addr)
        {
            if cluster_state.node_states()
                .all(|s| s.seen_by.contains(&candidate))
            {
                maybe_same.push(candidate);
            }
            else {
                proven_different.push(candidate);
            }
        }

        (maybe_same, proven_different)
    }

    async fn gossip_summary_digest(&self) -> GossipSummaryDigestData {
        let mut sha256 = Sha256::default();

        {
            fn hash_node_addr(sha256: &mut Sha256, addr: NodeAddr) {
                sha256.update(addr.unique.to_le_bytes());
                match addr.addr {
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

            let cluster_state = self.cluster_state.read().await;

            // NB: This relies on the map to have same iteration order on all nodes (which is true
            //      for FxHashMap)
            for s in cluster_state.node_states() {
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

    //TODO unit test
    async fn gossip_detailed_digest(&self) -> GossipDetailedDigestData {
        let nonce = rand::thread_rng().next_u32();
        Self::gossip_detailed_digest_with_given_nonce(&*self.cluster_state.read().await, nonce)
    }

    fn gossip_detailed_digest_with_given_nonce(cluster_state: &ClusterState, nonce: u32) -> GossipDetailedDigestData {
        let mut nodes: FxHashMap<NodeAddr, u64> = Default::default();

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

            let _ = nodes.insert(s.addr, hasher.finish());
        }

        GossipDetailedDigestData {
            nonce,
            nodes,
        }
    }

    //TODO unit test
    pub async fn gossip_partners(&self) -> Vec<(NodeAddr, Arc<ClusterMessage>)> {
        let mut result = Vec::with_capacity(self.config.num_gossip_partners);

        let (mut maybe_same, mut proven_different) = self.nodes_by_differing_state().await;

        let mut rand = rand::thread_rng();
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
                rand.gen_range(0.0 .. 1.0) < self.config.gossip_with_differing_state_probability
            };

            if use_proven_different {
                let idx = rand.gen_range(0..proven_different.len());
                let addr = proven_different.remove(idx);
                let msg = if let Some(msg) = &detailed_digest_message {
                    Arc::clone(msg)
                }
                else {
                    let msg = Arc::new(ClusterMessage::GossipDetailedDigest(self.gossip_detailed_digest().await));
                    detailed_digest_message = Some(msg.clone());
                    msg
                };

                result.push((addr, msg));
            }
            else {
                let idx = rand.gen_range(0..maybe_same.len());
                let addr = maybe_same.remove(idx);
                let msg = if let Some(msg) = &summary_digest_message {
                    Arc::clone(msg)
                }
                else {
                    let msg = Arc::new(ClusterMessage::GossipSummaryDigest(self.gossip_summary_digest().await));
                    summary_digest_message = Some(msg.clone());
                    msg
                };
                result.push((addr, msg));
            }
        }

        result
    }

    //TODO unit test
    pub async fn on_summary_digest(&self, other_digest: &GossipSummaryDigestData) -> Option<GossipDetailedDigestData> {
        let own_digest = self.gossip_summary_digest().await;
        if own_digest.full_sha256_digest == other_digest.full_sha256_digest {
            return None;
        }

        Some(self.gossip_detailed_digest().await)
    }

    //TODO debug logging for gossip

    //TODO unit test
    pub async fn on_detailed_digest(&self, other_digest: &GossipDetailedDigestData) -> Option<GossipDifferingAndMissingNodesData> {
        let cluster_state = self.cluster_state.read().await;

        //NB: we don't want anyone else to change state between hashing and comparing the hashes, so we get the lock once at
        //     the start
        let own_digest = Self::gossip_detailed_digest_with_given_nonce(&*cluster_state, other_digest.nonce);

        // my own data for nodes that hash differently from the gossip partner's hash
        let differing: Vec<NodeState> = own_digest.nodes.iter()
            .filter(|(addr, &hash)| Some(&hash) != other_digest.nodes.get(addr))
            .flat_map(|(addr, _)| cluster_state.get_node_state(addr).cloned())
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

    //TODO unit test
    pub async fn on_differing_and_missing_nodes(&self, other_data: GossipDifferingAndMissingNodesData) -> Option<GossipNodesData> {
        let differing_keys = other_data.differing.iter()
            .map(|n| n.addr)
            .collect::<Vec<_>>();

        let mut cluster_state = self.cluster_state.write().await;
        for s in other_data.differing {
            cluster_state.merge_node_state(s).await;
        }

        let mut nodes = Vec::new();
        for differing_addr in differing_keys {
            if let Some(state) = cluster_state.get_node_state(&differing_addr) {
                nodes.push(state.clone());
            }
        }

        for missing in &other_data.missing {
            if let Some(state) = cluster_state.get_node_state(missing) {
                nodes.push(state.clone());
            }
        }

        if nodes.is_empty() {
            None
        }
        else {
            Some(GossipNodesData {
                nodes,
            })
        }
    }

    //TODO unit test
    pub async fn on_nodes(&self, other_data: GossipNodesData) {
        let mut cluster_state = self.cluster_state.write().await;
        for s in other_data.nodes {
            cluster_state.merge_node_state(s).await;
        }
    }
}
