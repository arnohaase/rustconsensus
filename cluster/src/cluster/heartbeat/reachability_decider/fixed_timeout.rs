use std::time::Duration;
use tokio::time::Instant;
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;

/// This implementation decides reachability based on elapsed time since the most recent heartbeat:
///  if that time exceeds a given fixed timeout, the node is considered unreachable.
pub struct FixedTimeoutDecider {
    unreachable_threshold: Duration,
    last_heartbeat_timestamp: Instant,
}
impl FixedTimeoutDecider {
    pub fn new_for_threshold(unreachable_threshold: Duration) -> Self {
        FixedTimeoutDecider {
            unreachable_threshold,
            last_heartbeat_timestamp: Instant::now(),
        }
    }
}
impl ReachabilityDecider for FixedTimeoutDecider {
    fn new(config: &ClusterConfig, _initial_rtt: Duration) -> Self {
        Self::new_for_threshold(config.heartbeat_interval * 3) //TODO explicit config parameter
    }

    fn on_heartbeat(&mut self, _rtt: Duration) {
        self.last_heartbeat_timestamp = Instant::now();
    }

    fn is_reachable(&self) -> bool {
        self.last_heartbeat_timestamp.elapsed() < self.unreachable_threshold
    }
}


#[cfg(test)]
mod tests {
    use tokio::time::advance;
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_fixed_timeout_decider() {
        let mut decider = FixedTimeoutDecider::new_for_threshold(Duration::from_millis(100));

        assert!(decider.is_reachable());

        advance(Duration::from_millis(100)).await;
        assert!(!decider.is_reachable());

        for _ in 0..20 {
            decider.on_heartbeat(Duration::from_millis(10000));
            advance(Duration::from_millis(99)).await;
            assert!(decider.is_reachable());
        }

        advance(Duration::from_millis(1)).await;
        assert!(!decider.is_reachable());
    }
}