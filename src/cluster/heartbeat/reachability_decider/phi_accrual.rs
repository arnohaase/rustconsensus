use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;
use crate::util::rolling_data::RollingData;
use ordered_float::OrderedFloat;
use std::cmp::max;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};


/// The [PhiAccrualDecider] assumes that the time between two received heartbeats (i.e. including
///  the regular heartbeat interval) follows a Gaussian distribution, and it does a sliding-window
///  fit of mean and standard deviation based on actually measured arrival intervals.
///
/// This model describes the probability distribution of delays between heartbeats, and for a given
///  delay t the integral from t to +∞ predicts the probability that a next heartbeat is going to
///  arrive if we wait long enough (as opposed to the system being permanently unreachable).
///
/// An application can set a threshold for the probability, which is then dynamically mapped to
///  up-to-date measurements on the network - application configuration does not need to take
///  network RTT etc. into account explicitly. This approach and the algorithm used here is
///  based on https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf.
///
/// This strategy is included mainly because it is widely used in Akka. There are some conceptual
///  concerns to consider when using it:
/// * The original paper is based on flaky international connections rather than reliable,
///    pretty deterministic networks inside a single data center.
/// * 'Good' networks can lead to very repeatable heartbeat intervals, causing extremely narrow
///    Gaussian peaks, causing even small anomalies to be treated as permanent unavailabilities.
///    We mitigate this by setting a lower bound of 100ms for std deviation (following Akkas
///    implementation), giving up much of the precision of the approach
/// * Inside data centers, heartbeat messages tend to arrive in multiples of the heartbeat send
///    interval (with comparatively little jitter of network RTT) - and a Gaussian distribution
///    is not a good fit for this kind of disjoint peaks
/// * The sliding-window approach does not take occasional delays caused by load peaks on one of the
///    machines etc. into account - the Gaussian fit is based on the most recent N intervals, and
///    things that did not happen in that time frame don't exist as far as the algorithm is
///    concerned. While it is possible to add a configurable interval for this, it pretty much
///    countermands the benefits of the phi accrual algorithm.

/// The [PhiAccrualDecider] estimates the probability that a node is permanently unreachable based
///  on the elapsed time since the last heartbeat was received, allowing an application to set
///  a threshold based on that probability rather than some technical values (like a timeout
///  period).
///
/// The estimation approach used here is based on ,
///  calculating mean and standard deviation of the interval between heartbeats from past measurements
///  and approximating the probability distribution by a Gaussian distribution.
pub struct PhiAccrualDecider {
    buf: RollingData<256>,
    last_timestamp: Instant,
    ignore_heartbeat_response_after: Duration,
    reachability_phi_threshold: f64,
    min_std_dev: f64,
}

impl ReachabilityDecider for PhiAccrualDecider {
    fn new(config: &ClusterConfig, initial_rtt: Duration) -> PhiAccrualDecider {
        // we start with three values scattered around the heartbeat interval to avoid starting
        //  in an overly picky fashion and report unreachability due to regular scatter
        let mut buf = RollingData::new(config.heartbeat_interval.as_secs_f64());
        buf.add_value(config.heartbeat_interval.as_secs_f64() * 0.5);
        buf.add_value(config.heartbeat_interval.as_secs_f64() * 1.5);

        // and now for the actual initial value
        buf.add_value(initial_rtt.as_secs_f64());

        PhiAccrualDecider {
            buf,
            last_timestamp: Instant::now(),
            ignore_heartbeat_response_after: config.ignore_heartbeat_response_after,
            reachability_phi_threshold: config.reachability_phi_threshold,
            min_std_dev: config.reachability_phi_min_stddev.as_secs_f64(),
        }
    }

    fn on_heartbeat(&mut self, rtt: Duration) {
        if rtt > self.ignore_heartbeat_response_after {
            warn!("heartbeat after RTT of {:?} - ignoring because it exceeds the threshold of {:?}", rtt, self.ignore_heartbeat_response_after);
            return;
        }

        let now = Instant::now();
        let interval_since_last_heartbeat = now.duration_since(self.last_timestamp);
        self.last_timestamp = now;

        self.buf.add_value(interval_since_last_heartbeat.as_secs_f64());
    }

    fn is_reachable(&self) -> bool {
        let seconds_since_last_heartbeat = self.last_timestamp.elapsed().as_secs_f64();

        // we use a configurable lower bound for standard deviation to avoid a hard cut-off
        let std_dev = max(OrderedFloat(self.buf.std_dev()), OrderedFloat(self.min_std_dev)).0;

        let phi = phi(seconds_since_last_heartbeat, self.buf.mean(), std_dev);
        phi < self.reachability_phi_threshold
    }
}

/// Calculate an estimate for the probability that a heartbeat should have arrived already, given
///  the interval since the last heartbeat and the mean and standard deviation of past heartbeat
///  intervals. That is the one's complement of the probability a heartbeat should yet arrive.
fn phi(interval_since_last: f64, mean: f64, std_dev: f64) -> f64 {
    if std_dev < 1e-9 {
        return if interval_since_last < mean { 0.0 } else { 1.0 };
    }

    // We use the logistic approximation for the cumulative normalized Gaussian distribution
    // (e.g. https://www.econstor.eu/bitstream/10419/188388/1/v02-i01-p114_60-313-1-PB.pdf).

    // normalize
    let y = (interval_since_last - mean) / std_dev;

    // we return the probability that a heartbeat should have arrived already, i.e. the one's
    //  complement of the probability that a heartbeat should yet arrive
    let e = (-1.5976*y + -0.070566*(y.powi(3))).exp();
    1.0 / (1.0 + e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::node::test_node_addr_from_number;
    use crate::test_util::*;
    use rstest::rstest;
    use tokio::time;

    #[tokio::test(start_paused = true)]
    async fn test_phi_accrual_decider() {
        let mut config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr);
        config.reachability_phi_threshold = 0.99999999;
        config.reachability_phi_min_stddev = Duration::from_millis(100);

        let mut decider = PhiAccrualDecider::new(&config, Duration::from_secs(2));
        assert!(decider.is_reachable());

        time::advance(Duration::from_secs(4)).await;
        assert!(decider.is_reachable());
        time::advance(Duration::from_secs(1)).await;
        assert!(!decider.is_reachable());

        // saturate the decider with very regular heartbeats
        for _ in 0..1000 {
            time::advance(Duration::from_secs(1)).await;
            decider.on_heartbeat(Duration::from_millis(20));
        }

        // the decider is now trained for a mean of a little above 1 second, with zero std deviation

        assert!(decider.is_reachable());
        time::advance(Duration::from_millis(999)).await;
        assert!(decider.is_reachable());

        time::advance(Duration::from_millis(2)).await;
        // the minimum std deviation kicks in, otherwise we should be unreachable already
        assert!(decider.is_reachable());

        // waiting for six sigma takes us to unreachability
        time::advance(Duration::from_millis(600)).await;
        assert!(!decider.is_reachable());
    }

    #[tokio::test(start_paused = true)]
    async fn test_phi_accrual_decider_ignore_long_rtt() {
        let mut config = ClusterConfig::new(test_node_addr_from_number(1).socket_addr);
        config.ignore_heartbeat_response_after = Duration::from_secs(5);
        let mut decider = PhiAccrualDecider::new(&config, Duration::from_secs(1));

        // a heartbeat arrives after a long interval, but its RTT is above the 'ignore' threshold
        // --> the heartbeat should be ignored, and the decider return unreachability

        time::advance(Duration::from_secs(10)).await;
        decider.on_heartbeat(Duration::from_secs(6));
        assert!(!decider.is_reachable());
    }

    #[rstest]
    #[case(0.0, 0.0, 1.0, 0.5)]
    #[case(1.0, 1.0, 1.0, 0.5)]
    #[case(6.0, 0.0, 10.0, 0.725876)]
    #[case(15.0, 0.0, 10.0, 0.933053)]
    #[case(10.0, 0.0, 1.0, 1.0)] // corner case: handle extreme values robustly
    #[case(0.0, 10.0, 1.0, 0.0)] // corner case: handle extreme values robustly
    #[case(0.99, 1.0, 0.0000000001, 0.0)] // corner case: near-zero std deviation
    #[case(1.01, 1.0, 0.0000000001, 1.0)] // corner case: near-zero std deviation
    #[case(0.99, 1.0, 0.0, 0.0)] // corner case: zero std deviation
    #[case(1.01, 1.0, 0.0, 1.0)] // corner case: zero std deviation
    fn test_phi(#[case] input: f64, #[case] mean: f64, #[case] std_dev: f64, #[case] expected: f64) {
        let actual = phi(input, mean, std_dev);
        assert_approx_eq(actual, expected);
    }
}