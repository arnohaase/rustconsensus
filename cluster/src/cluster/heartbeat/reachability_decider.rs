//! NB: Akka claims to use a 'phi accrual' timeout decider, but that algorithm is not a good fit 
//!  for data centers, so we use a simple 'fixed timeout' approach. Btw., Akka apparently reached
//!  the same conclusion since it heavily modified the original phi accrual approach to make it
//!  work.
//!
//! In detail, phi accrual assumes that the time between two received heartbeats (i.e. including
//!  the regular heartbeat interval) follows a Gaussian distribution, and it does a sliding-window
//!  fit of mean and standard deviation based on actually measured arrival intervals.
//!
//! This model describes the probability distribution of delays between heartbeats, and for a given
//!  delay t the integral from t to +∞ predicts the probability that a next heartbeat is going to
//!  arrive if we wait long enough (as opposed to the system being permanently unreachable).
//!
//! An application can set a threshold for the probability, which is then dynamically mapped to
//!  up-to-date measurements on the network - application configuration does not need to take
//!  network RTT etc. into account explicitly. This approach and the algorithm used here is
//!  based on https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf.
//!
//! This strategy is included mainly because it is widely used in Akka. There are some conceptual
//!  concerns to consider when using it:
//! * The original paper is based on flaky international connections rather than reliable,
//!    pretty deterministic networks inside a single data center.
//! * 'Good' networks can lead to very repeatable heartbeat intervals, causing extremely narrow
//!    Gaussian peaks, causing even small anomalies to be treated as permanent unavailabilities.
//!    We mitigate this by setting a lower bound of 100ms for std deviation (following Akkas
//!    implementation), giving up much of the precision of the approach
//! * Inside data centers, heartbeat messages tend to arrive in multiples of the heartbeat send
//!    interval (with comparatively little jitter of network RTT) - and a Gaussian distribution
//!    is not a good fit for this kind of disjoint peaks
//! * The sliding-window approach does not take occasional delays caused by load peaks on one of the
//!    machines etc. into account - the Gaussian fit is based on the most recent N intervals, and
//!    things that did not happen in that time frame don't exist as far as the algorithm is
//!    concerned. While it is possible to add a configurable interval for this, it pretty much
//!    countermands the benefits of the phi accrual algorithm.


use std::time::Duration;
use tokio::time::Instant;
use crate::cluster::cluster_config::ClusterConfig;

/// A [ReachabilityDecider] is a stateful component for one other node. It is notified of every
///  incoming heartbeat, and it decides based on heartbeat history and the current timestamp
///  whether the node is reachable or not
pub trait ReachabilityDecider {
    fn new(config: &ClusterConfig, initial_rtt: Duration) -> Self;

    fn on_heartbeat(&mut self, rtt: Duration);

    fn is_reachable(&self) -> bool;
}

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