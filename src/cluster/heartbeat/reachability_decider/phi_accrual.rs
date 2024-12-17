use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;
use crate::util::rolling_data::RollingData;
use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;


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
}

impl ReachabilityDecider for PhiAccrualDecider {
    //TODO unit test
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
        }
    }

    //TODO unit test
    fn on_heartbeat(&mut self, rtt: Duration) {
        let interval_since_last_heartbeat = self.last_timestamp.elapsed();
        if rtt > self.ignore_heartbeat_response_after {
            warn!("heartbeat after RTT of {:?} - ignoring because it exceeds the threshold of {:?}", rtt, self.ignore_heartbeat_response_after);
            return;
        }
        self.buf.add_value(interval_since_last_heartbeat.as_secs_f64());
    }

    //TODO unit test
    fn is_reachable(&self) -> bool {
        let seconds_since_last_heartbeat = self.last_timestamp.elapsed().as_secs_f64();

        //TODO lower bound for std deviation?

        let phi = phi(seconds_since_last_heartbeat, self.buf.mean(), self.buf.std_dev());
        phi < self.reachability_phi_threshold
    }
}

/// Calculate an estimate for the probability that a heartbeat will arrive in the future, given
///  the interval since the last heartbeat and the mean and standard deviation of past heartbeat
///  intervals.
///
/// More precisely, calculate -log_10 of the probability, to have a logarithmic number that grows
///  as time goes by.
//TODO unit test
fn phi(interval_since_last: f64, mean: f64, std_dev: f64) -> f64 {
    // We use the logistic approximation for the cumulative normalized Gaussian distribution
    // (e.g. https://www.econstor.eu/bitstream/10419/188388/1/v02-i01-p114_60-313-1-PB.pdf).

    // normalize
    let y = (interval_since_last - mean) / std_dev;

    // NB: The logistic approximation is for the integral from -∞ to y, and it has negative
    //      coefficients. We invert the sign of y (by inverting the coefficients, which yields
    //      the same result), making this an approximation for the integral
    //      from 'interval_since_last' to +∞ (since the Gaussian distribution is symmetric)
    let e = (1.5976*y + 0.070566*(y.powi(3))).exp();

    // we return -log_10 of the probability to have a logarithmic number that grows
    -(1.0 / (1.0 + e)).log10()
}
