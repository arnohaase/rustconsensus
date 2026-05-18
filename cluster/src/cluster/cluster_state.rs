//! Single owner of the cluster's mutable state and of the
//! [`ClusterEventNotifier`] used to broadcast derived events.
//!
//! Composition:
//!
//! ```text
//!                ┌──────────────────────────────────────────────┐
//!                │  ClusterStateHandle  (Clone, cheap)          │
//!                │  ┌────────────────────────────────────────┐  │
//!                │  │ snapshot: ArcSwap<ClusterStateSnapshot>│  │  ← lock-free reads
//!                │  │ events:   Arc<ClusterEventNotifier>    │  │  ← subscribe here
//!                │  │ commands: mpsc::Sender<ClusterCommand> │  │  ← dispatch writes
//!                │  └────────────────────────────────────────┘  │
//!                └──────────────────────────────────────────────┘
//!                                       │
//!                                       ▼  spawned by `from_snapshot`
//!                ┌──────────────────────────────────────────────┐
//!                │  actor task  (single writer)                 │
//!                │     owns:  ClusterStateSnapshot              │
//!                │            Arc<ClusterEventNotifier>         │  ← emits events
//!                └──────────────────────────────────────────────┘
//! ```
//!
//! Lifecycle: [`ClusterStateHandle::new`] constructs the snapshot, the
//! published-version counter, and the notifier (handing the notifier a
//! read-only `Arc` clone of the counter), then spawns the actor. Production
//! code goes through `new`; tests that need to seed state without a Tokio
//! runtime use [`ClusterStateHandle::new_synchronous`].

use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::cluster_events::ClusterEvent;
use crate::cluster::cluster_events::ClusterEventNotifier;
use crate::cluster::gossip::gossip_messages::GossipNodesData;
use crate::cluster::heartbeat::downing_strategy::DowningStrategyDecision;
use crate::cluster::state::command::{self, ClusterCommand};
use crate::cluster::state::node_state::NodeState;
use crate::cluster::state::snapshot::ClusterStateSnapshot;
use crate::messaging::node_addr::NodeAddr;
use arc_swap::ArcSwap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, spawn, time};
use tracing::{debug, info};

/// Cheap to clone (`Arc` inside). All clones see the same published snapshot,
/// dispatch to the same actor task, and share the same event notifier.
#[derive(Clone)]
pub struct ClusterStateHandle {
    snapshot: Arc<ArcSwap<ClusterStateSnapshot>>,
    /// Broadcaster for derived `ClusterEvent`s. Subscribers go through
    /// [`Self::events`]. The notifier itself is read-only with respect to
    /// the snapshot-version counter; only the actor task (owner of the
    /// writable counter clone) bumps it.
    events: Arc<ClusterEventNotifier>,
    /// Sender half of the channel feeding the single-writer actor task
    /// spawned by [`command::spawn_actor`]. Cloning the handle clones this
    /// sender, so every clone targets the same actor.
    commands: mpsc::Sender<ClusterCommand>,
}

impl ClusterStateHandle {
    /// Build a handle around a freshly created [`ClusterStateSnapshot`].
    /// Constructs the [`ClusterEventNotifier`] internally, spawns the actor,
    /// and publishes an initial snapshot at version `1`.
    pub(crate) fn new(myself: NodeAddr, config: Arc<ClusterConfig>) -> Self {
        let state = ClusterStateSnapshot::new(myself, config);
        let initial = Arc::new(state.clone());
        let snapshot = Arc::new(ArcSwap::from(initial));
        let events = Arc::new(ClusterEventNotifier::new());
        let commands = command::spawn_actor(
            state,
            snapshot.clone(),
            events.clone(),
        );
        ClusterStateHandle {
            snapshot,
            events,
            commands,
        }
    }

    /// Lock-free access to the most recently published snapshot.
    ///
    /// Single atomic load + `Arc` clone; no `.await`.
    pub(crate) fn snapshot(&self) -> Arc<ClusterStateSnapshot> {
        self.snapshot.load_full()
    }

    /// Event notifier shared with the rest of the cluster. Subscribers
    /// outside the cluster crate reach this via `Cluster::subscribe`, which
    /// delegates here.
    pub(crate) fn events(&self) -> &Arc<ClusterEventNotifier> {
        &self.events
    }

    // ------------------------------------------------------------------
    // Command-channel API
    //
    // Each helper builds a [`ClusterCommand`], sends it to the actor task,
    // and (for commands that produce a value) awaits the reply. Awaiting
    // the reply also guarantees the published snapshot reflects the
    // command's effect by the time the caller resumes, since the actor
    // refreshes the snapshot before signaling the reply channel.
    // ------------------------------------------------------------------

    async fn send_cmd(&self, cmd: ClusterCommand) {
        if self.commands.send(cmd).await.is_err() {
            tracing::warn!(
                "cluster-state actor channel closed; command dropped (cluster shutting down?)"
            );
        }
    }

    /// Dispatch [`ClusterCommand::MergeNodeState`] (fire-and-forget).
    pub(crate) async fn cmd_merge_node_state(&self, node_state: NodeState) {
        self.send_cmd(ClusterCommand::MergeNodeState { node_state, reply: None }).await;
    }

    /// Dispatch [`ClusterCommand::GossipMergeAndCollectResponse`] and await
    /// the actor's response. Returns `None` only on actor shutdown.
    pub(crate) async fn cmd_gossip_merge_and_collect(
        &self,
        differing: Vec<NodeState>,
        missing: Vec<NodeAddr>,
    ) -> Option<GossipNodesData> {
        let (tx, rx) = oneshot::channel();
        self.send_cmd(ClusterCommand::GossipMergeAndCollectResponse {
            differing,
            missing,
            reply: tx,
        }).await;
        command::recv_reply(rx, "GossipMergeAndCollectResponse").await.flatten()
    }

    pub(crate) async fn cmd_update_reachability_from_myself(
        &self,
        reachability: std::collections::BTreeMap<NodeAddr, bool>,
    ) {
        self.send_cmd(ClusterCommand::UpdateReachabilityFromMyself { reachability }).await;
    }

    pub(crate) async fn cmd_promote_myself_to_weakly_up(&self) {
        self.send_cmd(ClusterCommand::PromoteMyselfToWeaklyUp).await;
    }

    pub(crate) async fn cmd_promote_myself_to_up(&self) {
        self.send_cmd(ClusterCommand::PromoteMyselfToUp).await;
    }

    pub(crate) async fn cmd_promote_myself_to_down(&self) {
        self.send_cmd(ClusterCommand::PromoteMyselfToDown).await;
    }

    pub(crate) async fn cmd_do_leader_actions(&self) {
        self.send_cmd(ClusterCommand::DoLeaderActions).await;
    }

    /// Dispatch [`ClusterCommand::ApplyDowningDecision`] and await the list
    /// of nodes that were downed (empty on actor shutdown).
    pub(crate) async fn cmd_apply_downing_decision(
        &self,
        decision: DowningStrategyDecision,
    ) -> Vec<NodeAddr> {
        let (tx, rx) = oneshot::channel();
        self.send_cmd(ClusterCommand::ApplyDowningDecision { decision, reply: tx }).await;
        command::recv_reply(rx, "ApplyDowningDecision").await.unwrap_or_default()
    }

    pub(crate) async fn cmd_add_joiner(
        &self,
        addr: NodeAddr,
        roles: std::collections::BTreeSet<String>,
    ) {
        self.send_cmd(ClusterCommand::AddJoiner { addr, roles }).await;
    }

    /// Round-trip a no-op probe through the actor. Returns when all
    /// previously-submitted commands have been processed and the snapshot
    /// has been refreshed. Primarily used by tests that submit a
    /// fire-and-forget command and need to observe its effect
    /// deterministically.
    #[cfg(test)]
    pub(crate) async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.send_cmd(ClusterCommand::Flush { reply: tx }).await;
        let _ = command::recv_reply(rx, "Flush").await;
    }
}

pub(crate) async fn run_administrative_tasks_loop(
    config: Arc<ClusterConfig>,
    state_handle: ClusterStateHandle,
    myself: NodeAddr,
) {
    let mut leader_action_ticks = time::interval(config.leader_action_interval);

    let mut events = state_handle.events().subscribe();

    //TODO documentation
    if let Some(weakly_up_after) = config.weakly_up_after {
        let state_handle = state_handle.clone();
        spawn(async move {
            time::sleep(weakly_up_after).await;
            // Submitted via the single-writer actor so the snapshot
            // refresh is performed by the actor loop.
            state_handle.cmd_promote_myself_to_weakly_up().await;
        });
    }

    loop {
        select! {
            _ = leader_action_ticks.tick() => {
                debug!("running periodic leader actions");
                state_handle.cmd_do_leader_actions().await;
            }
            evt = events.recv() => {
                if let Ok(ClusterEvent::NodeStateChanged(data)) = evt {
                    if data.new_state.is_terminal() && data.addr == myself {
                        info!("Shutting down this cluster node because it reached terminal state {:?}", data.new_state);
                        return;
                    }
                }
            }
        }
    }
}
