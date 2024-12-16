


//TODO phi accrual decider
// paper on phi accrual: https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf
// logistic approximation of Gaussian integral: https://www.econstor.eu/bitstream/10419/188388/1/v02-i01-p114_60-313-1-PB.pdf

use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;
use crate::cluster::cluster_config::ClusterConfig;
use crate::cluster::heartbeat::reachability_decider::ReachabilityDecider;


/// The [PhiAccrualDecider] estimates the probability that a node is permanently unreachable based
///  on the elapsed time since the last heartbeat was received, allowing an application to set
///  a threshold based on that probability rather than some technical values (like a timeout
///  period).
///
/// The estimation approach used here is based on https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf,
///  calculating mean and standard deviation of the interval between heartbeats from past measurements
///  and approximating the probability distribution by a Gaussian distribution.
pub struct PhiAccrualDecider {
    buf: MeasurementBuffer,
    last_timestamp: Instant,
    ignore_heartbeat_response_after: Duration,
    reachability_phi_threshold: f64,
}

impl ReachabilityDecider for PhiAccrualDecider {
    //TODO unit test
    fn new(config: &ClusterConfig, initial_rtt: Duration) -> PhiAccrualDecider {
        let mut buf = MeasurementBuffer::new();

        // we start with three values scattered around the heartbeat interval to avoid starting
        //  in an overly picky fashion and report unreachability due to regular scatter
        let _ = buf.add_value(config.heartbeat_interval.as_secs_f64());
        let _ = buf.add_value(config.heartbeat_interval.as_secs_f64() * 0.5);
        let _ = buf.add_value(config.heartbeat_interval.as_secs_f64() * 1.5);

        // and we add the initial value - and we ensure that the buffer has a capacity of at
        //  least 4 :-)
        assert!(buf.add_value(initial_rtt.as_secs_f64()).is_none());

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


enum MeasurementBuffer {
    Growing(Vec<f64>),
    Ring {
        buf: Vec<f64>,
        next: usize,
    },
}
impl MeasurementBuffer {
    const MAX_BUF_LEN: usize = 256;

    fn new() -> MeasurementBuffer {
        MeasurementBuffer::Growing(vec![])
    }

    //TODO unit test
    /// adds a new value, returning the value that was evicted in its place (if any)
    fn add_value(&mut self, duration_secs: f64) -> Option<f64> {
        match self {
            MeasurementBuffer::Growing(buf) => {
                buf.push(duration_secs);
                if buf.len() == Self::MAX_BUF_LEN {
                    let buf = std::mem::take(buf);
                    *self = MeasurementBuffer::Ring { buf, next: 0 };
                }
                None
            }
            MeasurementBuffer::Ring { buf, next } => {
                let evicted = buf[*next];
                buf[*next] = duration_secs;
                *next = (*next + 1) % Self::MAX_BUF_LEN;
                Some(evicted)
            }
        }
    }

    //TODO caching of mean?
    //TODO unit test
    fn mean(&self) -> f64 {
        match self {
            MeasurementBuffer::Growing(buf) => buf.iter().sum::<f64>() / buf.len() as f64,
            MeasurementBuffer::Ring { buf, .. } => buf.iter().sum::<f64>() / buf.len() as f64,
        }
    }

    //TODO unit test
    fn std_dev(&self) -> f64 {
        let mean = self.mean();
        match self {
            MeasurementBuffer::Growing(buf) => buf.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / buf.len() as f64,
            MeasurementBuffer::Ring { buf, .. } => buf.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / buf.len() as f64,
        }.sqrt()
    }
}
