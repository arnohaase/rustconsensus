//! Single-writer actor that owns the cluster's [`ClusterStateSnapshot`] and
//! publishes immutable snapshots via [`ArcSwap`].
//!
//! Post-collapse: there is only one cluster-state type. The actor task
//! created by [`spawn_actor`] owns a [`ClusterStateSnapshot`] by value and
//! is the sole entity that can mutate it. Mutations are dispatched as
//! [`ClusterCommand`] variants over an `mpsc` channel; commands that need
//! to return a value carry a `oneshot::Sender` reply channel.
//!
//! Each reducer on `ClusterStateSnapshot` returns a `Vec<ClusterEvent>`;
//! the actor publishes the new snapshot (via `ArcSwap::store`) and *then*
//! forwards those events to the [`ClusterEventNotifier`]. Per-node
//! structural sharing (entries are `Arc<NodeState>`) keeps the publish
//! step cheap: only the modified entries are cloned by the preceding
//! reducer via `Arc::make_mut`.
//!
//! Event-ordering invariant: a subscriber observing an event tagged with
//! snapshot version `V` can rely on
//! `handle.snapshot().snapshot_version >= V`.
//!
//! Reply-after-snapshot invariant: for commands with a reply, the reply
//! is signalled after the snapshot is refreshed and events are emitted,
//! so a caller `await`ing the reply is guaranteed to observe its own
//! write via `handle.snapshot()`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use arc_swap::ArcSwap;
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

use crate::cluster::cluster_events::{ClusterEvent, ClusterEventNotifier};
use crate::cluster::state::node_state::{MembershipState, NodeState};
use crate::cluster::gossip::gossip_messages::GossipNodesData;
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision;
use crate::cluster::state::snapshot::ClusterStateSnapshot;
use crate::messaging::node_addr::NodeAddr;

/// Capacity of the command channel between [`ClusterStateHandle`] clones and
/// the actor task. Sized generously: command processing is fast (sync work
/// + one snapshot refresh) so the channel should normally be near-empty.
pub(crate) const COMMAND_CHANNEL_CAPACITY: usize = 1024;

/// All cluster-state mutations are expressed as one of these variants.
pub(crate) enum ClusterCommand {
    MergeNodeState {
        node_state: NodeState,
        reply: Option<oneshot::Sender<Option<NodeState>>>,
    },

    /// Atomic counterpart to `on_differing_and_missing_nodes`: merges each
    /// node from `differing`, collects any that diverged from the peer's
    /// view post-merge, then appends the locally-known state for every
    /// address in `missing`.
    GossipMergeAndCollectResponse {
        differing: Vec<NodeState>,
        missing: Vec<NodeAddr>,
        reply: oneshot::Sender<Option<GossipNodesData>>,
    },

    UpdateReachabilityFromMyself {
        reachability: BTreeMap<NodeAddr, bool>,
    },

    PromoteMyselfToWeaklyUp,
    PromoteMyselfToUp,
    PromoteMyselfToDown,

    DoLeaderActions,

    /// Apply a downing decision (DownUs / DownThem). Returns the list of
    /// addresses that were actually downed so the caller can send a
    /// best-effort "you are down" message to each.
    ApplyDowningDecision {
        decision: DowningStrategyDecision,
        reply: oneshot::Sender<Vec<NodeAddr>>,
    },

    AddJoiner {
        addr: NodeAddr,
        roles: BTreeSet<String>,
    },

    /// Synchronization probe: the actor processes prior commands (in order),
    /// refreshes the snapshot, and then signals `reply`. Used by tests and
    /// by call sites that need to ensure their fire-and-forget commands
    /// have been applied before proceeding.
    #[cfg(test)]
    Flush {
        reply: oneshot::Sender<()>,
    },
}

/// Spawn the single-writer actor task. The actor owns `state` by value and
/// is the only entity that can mutate it. Returns the sender half of the
/// command channel.
pub(crate) fn spawn_actor(
    state: ClusterStateSnapshot,
    snapshot: Arc<ArcSwap<ClusterStateSnapshot>>,
    event_notifier: Arc<ClusterEventNotifier>,
) -> mpsc::Sender<ClusterCommand> {
    let (tx, mut rx) = mpsc::channel::<ClusterCommand>(COMMAND_CHANNEL_CAPACITY);

    // Tests built around `new_synchronous` are invoked outside a Tokio
    // runtime; those tests never submit commands. Detect the no-runtime
    // case and skip the spawn rather than panicking inside `tokio::spawn`.
    if tokio::runtime::Handle::try_current().is_err() {
        trace!("cluster-state actor: no Tokio runtime; skipping spawn (test mode)");
        // Drop state; the no-runtime tests using `new_synchronous` only
        // need the published snapshot, never the actor-owned state.
        drop(state);
        return tx;
    }

    tokio::spawn(async move {
        let mut state = state;
        while let Some(cmd) = rx.recv().await {
            let (post_refresh, derived_events) = handle_command(&mut state, cmd);
            refresh_snapshot(&state, &snapshot);
            // Emit derived events *after* the new snapshot is published so
            // any subscriber observing an event tagged with version V can
            // rely on `handle.snapshot().snapshot_version >= V`.
            for evt in derived_events {
                event_notifier.send_event(evt);
            }
            if let Some(action) = post_refresh {
                action();
            }
        }
        trace!("cluster-state actor: command channel closed, shutting down");
    });

    tx
}

/// Publish the latest state as the new snapshot. Cloning is cheap because
/// per-node entries are `Arc<NodeState>` (structural sharing); only nodes
/// touched by the preceding reducer are deep-cloned (via `Arc::make_mut`).
fn refresh_snapshot(
    state: &ClusterStateSnapshot,
    snapshot: &Arc<ArcSwap<ClusterStateSnapshot>>,
) {
    snapshot.store(Arc::new(state.clone()));
}

/// Reply-after-snapshot-publish action returned by [`handle_command`].
type PostRefresh = Box<dyn FnOnce() + Send>;

fn handle_command(
    state: &mut ClusterStateSnapshot,
    cmd: ClusterCommand,
) -> (Option<PostRefresh>, Vec<ClusterEvent>) {
    use ClusterCommand::*;
    match cmd {
        MergeNodeState { node_state, reply } => {
            let addr = node_state.addr;
            let events = state.merge_node_state(node_state);
            let post = reply.map(|reply| {
                let merged = state.get_node_state(&addr).map(|arc| (**arc).clone());
                let action: PostRefresh = Box::new(move || { let _ = reply.send(merged); });
                action
            });
            (post, events)
        }
        GossipMergeAndCollectResponse { differing, missing, reply } => {
            let mut response_nodes = Vec::new();
            let mut all_events = Vec::new();
            for s in differing {
                let other_node = s.clone();
                all_events.extend(state.merge_node_state(s));
                if let Some(merged) = state.get_node_state(&other_node.addr) {
                    if merged.as_ref() != &other_node {
                        response_nodes.push((**merged).clone());
                    }
                }
            }
            for missing_addr in &missing {
                if let Some(s) = state.get_node_state(missing_addr) {
                    response_nodes.push((**s).clone());
                }
            }
            let resp = if response_nodes.is_empty() {
                None
            } else {
                Some(GossipNodesData { nodes: response_nodes })
            };
            (Some(Box::new(move || { let _ = reply.send(resp); })), all_events)
        }
        UpdateReachabilityFromMyself { reachability } => {
            let events = state.update_reachability_from_myself(&reachability);
            (None, events)
        }
        PromoteMyselfToWeaklyUp => {
            let events = state.promote_myself_to_weakly_up();
            (None, events)
        }
        PromoteMyselfToUp => {
            let events = state.promote_myself(MembershipState::Up);
            (None, events)
        }
        PromoteMyselfToDown => {
            let events = state.promote_myself(MembershipState::Down);
            (None, events)
        }
        DoLeaderActions => {
            let events = state.do_leader_actions();
            (None, events)
        }
        ApplyDowningDecision { decision, reply } => {
            let (downed, events) = state.apply_downing_decision(decision);
            (Some(Box::new(move || { let _ = reply.send(downed); })), events)
        }
        AddJoiner { addr, roles } => {
            state.add_joiner(addr, roles);
            (None, Vec::new())
        }
        #[cfg(test)]
        Flush { reply } => {
            (Some(Box::new(move || { let _ = reply.send(()); })), Vec::new())
        }
    }
}

/// Helper for ergonomic `.await` on the reply side of a command.
pub(crate) async fn recv_reply<T>(rx: oneshot::Receiver<T>, what: &'static str) -> Option<T> {
    match rx.await {
        Ok(v) => Some(v),
        Err(_) => {
            warn!("cluster-state actor reply channel closed before reply to {}", what);
            None
        }
    }
}
