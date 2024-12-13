use std::sync::Arc;

/// A [HeartbeatTracker] keeps track of heartbeat responses for a single remote node tracked by this
///  node.
struct HeartbeatTracker {
    moving_mean_rtt_millis: Option<f64>,
    moving_variance_rtt_millis_squared: f64, //TODO lower bound during evaluation to avoid anomalies
    last_seen: Instant,
    config: Arc<ClusterConfig>,
}
impl HeartbeatTracker {
    fn new(config: Arc<ClusterConfig>) -> HeartbeatTracker {
        HeartbeatTracker {
            moving_mean_rtt_millis: None,
            moving_variance_rtt_millis_squared: 0.0,
            last_seen: Instant::now(),
            config,
        }
    }

    fn on_heartbeat_roundtrip(&mut self, rtt_nanos: u64) {
        self.last_seen = Instant::now();

        let rtt_millis = (rtt_nanos as f64) / 1000000.0;

        if let Some(prev) = self.moving_mean_rtt_millis {
            // calculate moving avg / variance
            let alpha = self.config.rtt_moving_avg_new_weight;

            let mean = rtt_millis * alpha + prev * (1.0 - alpha);
            self.moving_mean_rtt_millis = Some(mean);

            let s = (mean - rtt_millis).powi(2);
            self.moving_variance_rtt_millis_squared = s * alpha + self.moving_variance_rtt_millis_squared * (1.0 - alpha);
        }
        else {
            // first RTT response
            self.moving_mean_rtt_millis = Some(rtt_millis);
            self.moving_variance_rtt_millis_squared = 0.0;
        }
    }

    const MAX_PHI: f64 = 1e9;


    //TODO unit test
    fn phi(&self) -> f64 {
        let nanos_since_last_seen = Instant::now().duration_since(self.last_seen).as_nanos() as f64;

        let rtt_mean_nanos = self.moving_mean_rtt_millis.unwrap_or(0.0) * 1000000.0;

        let mut rtt_std_dev_millis = self.moving_variance_rtt_millis_squared.sqrt();
        let min_std_dev_millis = (self.config.rtt_min_std_dev.as_nanos() as f64) / 1000000.0;
        if rtt_std_dev_millis < min_std_dev_millis {
            rtt_std_dev_millis = min_std_dev_millis;
        }

        let millis_overdue = (
            nanos_since_last_seen
                - self.config.heartbeat_interval.as_nanos() as f64
                - self.config.heartbeat_grace_period.as_nanos() as f64
                - rtt_mean_nanos
        ) / 1000000.0;

        if millis_overdue < 0.0 {
            return 0.0;
        }

        fn gaussian(x: f64, sigma: f64) -> f64 {
            let exponent = -x.powi(2) / (2.0 * sigma.powi(2));
            let coefficient = 1.0 / (sigma * (2.0 * PI).sqrt());
            coefficient * exponent.exp()
        }

        //TODO logarithm?

        let g = gaussian(millis_overdue, rtt_std_dev_millis);
        if g < 1.0 / Self::MAX_PHI {
            return Self::MAX_PHI;
        }
        1.0 / g
    }

    pub fn is_reachable(&self) -> bool {
        let phi = self.phi();
        let result = phi < self.config.reachability_phi_threshold;

        // trace!("heartbeat for {:?}: rtt={}ms, variance={}ms, phi={} -> {}",
        //     self.tracked_node,
        //     self.moving_mean_rtt.unwrap_or(0.0)/ 1000000.0,
        //     self.moving_variance_rtt / 1000000.0,
        //     phi,
        //     result,
        // );

        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_tracker_on_heartbeat_roundtrip() {
        let config = Arc::new(new_config());

        let mut tracker = HeartbeatTracker::new(config.clone());

        tracker.on_heartbeat_roundtrip(1_000_000);
        assert_eq!(tracker.last_seen, Instant::now());
        assert_relative_eq!(tracker.moving_mean_rtt_millis.unwrap(), 1.0);
        assert_relative_eq!(tracker.moving_variance_rtt_millis_squared, 0.0);

        advance(Duration::from_secs(1)).await;

        tracker.on_heartbeat_roundtrip(1_500_000);
        assert_eq!(tracker.last_seen, Instant::now());
        assert_relative_eq!(tracker.moving_mean_rtt_millis.unwrap(), 1.25);
        assert_relative_eq!(tracker.moving_variance_rtt_millis_squared, 0.03125);

        advance(Duration::from_secs(5)).await;

        tracker.on_heartbeat_roundtrip(1_500_000);
        assert_eq!(tracker.last_seen, Instant::now());
        assert_relative_eq!(tracker.moving_mean_rtt_millis.unwrap(), 1.375);
        assert_relative_eq!(tracker.moving_variance_rtt_millis_squared, 0.0234375);
    }

}
