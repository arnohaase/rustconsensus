//! This is an adaptation of Sally Floyd's High Speed TCP (RFC 3649) congestion control.
//! See https://www.icir.org/floyd/hstcp.html
//!
//! The idea is to use AIMD with 'adaptive' increments / decrement factors, adding a bigger
//!  amount and decreasing by a smaller factor the larger cwnd is.

use crate::safe_converter::{PrecheckedCast};
use std::cmp::{max, min};
use tracing::{debug, instrument, trace};

/// From AIMD tables in RFC 3649 appendix B, with fixed-point MD scaled at << 8
/// (implementation idea from John Heffner's Linux kernel implementation).
///
/// The first value of the pair is the cwnd value up to which the entry applies. The index in the
///  list plus 1 is the additive increment (AI). The second value of the pair is the variable
///  part `b` for the multiplicative decrease  w <- w * (1 - b(w)), fixed-point encoded.
const AIMD_VALUES: [(u32, u32); 73] = [
    (     38,  128, /*  0.50 */ ),
    (    118,  112, /*  0.44 */ ),
    (    221,  104, /*  0.41 */ ),
    (    347,   98, /*  0.38 */ ),
    (    495,   93, /*  0.37 */ ),
    (    663,   89, /*  0.35 */ ),
    (    851,   86, /*  0.34 */ ),
    (   1058,   83, /*  0.33 */ ),
    (   1284,   81, /*  0.32 */ ),
    (   1529,   78, /*  0.31 */ ),
    (   1793,   76, /*  0.30 */ ),
    (   2076,   74, /*  0.29 */ ),
    (   2378,   72, /*  0.28 */ ),
    (   2699,   71, /*  0.28 */ ),
    (   3039,   69, /*  0.27 */ ),
    (   3399,   68, /*  0.27 */ ),
    (   3778,   66, /*  0.26 */ ),
    (   4177,   65, /*  0.26 */ ),
    (   4596,   64, /*  0.25 */ ),
    (   5036,   62, /*  0.25 */ ),
    (   5497,   61, /*  0.24 */ ),
    (   5979,   60, /*  0.24 */ ),
    (   6483,   59, /*  0.23 */ ),
    (   7009,   58, /*  0.23 */ ),
    (   7558,   57, /*  0.22 */ ),
    (   8130,   56, /*  0.22 */ ),
    (   8726,   55, /*  0.22 */ ),
    (   9346,   54, /*  0.21 */ ),
    (   9991,   53, /*  0.21 */ ),
    (  10661,   52, /*  0.21 */ ),
    (  11358,   52, /*  0.20 */ ),
    (  12082,   51, /*  0.20 */ ),
    (  12834,   50, /*  0.20 */ ),
    (  13614,   49, /*  0.19 */ ),
    (  14424,   48, /*  0.19 */ ),
    (  15265,   48, /*  0.19 */ ),
    (  16137,   47, /*  0.19 */ ),
    (  17042,   46, /*  0.18 */ ),
    (  17981,   45, /*  0.18 */ ),
    (  18955,   45, /*  0.18 */ ),
    (  19965,   44, /*  0.17 */ ),
    (  21013,   43, /*  0.17 */ ),
    (  22101,   43, /*  0.17 */ ),
    (  23230,   42, /*  0.17 */ ),
    (  24402,   41, /*  0.16 */ ),
    (  25618,   41, /*  0.16 */ ),
    (  26881,   40, /*  0.16 */ ),
    (  28193,   39, /*  0.16 */ ),
    (  29557,   39, /*  0.15 */ ),
    (  30975,   38, /*  0.15 */ ),
    (  32450,   38, /*  0.15 */ ),
    (  33986,   37, /*  0.15 */ ),
    (  35586,   36, /*  0.14 */ ),
    (  37253,   36, /*  0.14 */ ),
    (  38992,   35, /*  0.14 */ ),
    (  40808,   35, /*  0.14 */ ),
    (  42707,   34, /*  0.13 */ ),
    (  44694,   33, /*  0.13 */ ),
    (  46776,   33, /*  0.13 */ ),
    (  48961,   32, /*  0.13 */ ),
    (  51258,   32, /*  0.13 */ ),
    (  53677,   31, /*  0.12 */ ),
    (  56230,   30, /*  0.12 */ ),
    (  58932,   30, /*  0.12 */ ),
    (  61799,   29, /*  0.12 */ ),
    (  64851,   28, /*  0.11 */ ),
    (  68113,   28, /*  0.11 */ ),
    (  71617,   27, /*  0.11 */ ),
    (  75401,   26, /*  0.10 */ ),
    (  79517,   26, /*  0.10 */ ),
    (  84035,   25, /*  0.10 */ ),
    (  89053,   24, /*  0.10 */ ),
    ( u32::MAX, 23, /*  0.09 */ ),
];

#[derive(Debug)]
pub struct HsCongestionControl {
    ai: usize,
    send_window_limit: u32,
    cwnd: u32,
    cwnd_cnt: u32,
}

impl HsCongestionControl {
    pub fn new(send_window_limit: u32) -> HsCongestionControl {
        assert!(send_window_limit >= 2);

        const START_CWND: u32 = 100;  //TODO good starting point? We are in a DC after all

        let (ai, cwnd) = if send_window_limit <= AIMD_VALUES[0].0 {
            (0, send_window_limit)
        }
        else {
            (1, min(send_window_limit, START_CWND))
        };

        HsCongestionControl {
            send_window_limit,
            ai,
            cwnd,
            cwnd_cnt: 0,
        }
    }

    #[cfg(test)]
    pub fn set_internals(&mut self, ai: usize, cwnd: u32, cwnd_cnt: u32) {
        self.ai = ai;
        self.cwnd = cwnd;
        self.cwnd_cnt = cwnd_cnt;
    }
    
    pub fn cwnd(&self) -> u32 {
        self.cwnd
    }

    fn is_slow_start(&self) -> bool {
        self.cwnd <= AIMD_VALUES[0].0
    }

    #[instrument]
    pub fn on_ack(&mut self, num_packets_in_flight: u32) {
        if self.cwnd == self.send_window_limit {
            // no point for cwnd to become greater than the send window limit
            return;
        }

        if self.is_slow_start() {
            // in slow start mode, we are somewhat lenient with regard to cwnd usage: We increase
            //  cwnd if it has at least 50% utilization
            if self.cwnd > 2*num_packets_in_flight {
                trace!("slow start - less than cwnd/2 packets in flight -> no adjustment");
                return;
            }

            self.cwnd += 1;
        }
        else {
            // in regular congestion avoidance mode, we follow the spirit of RFC2861 and increase
            //  cwnd only when we are actually using it fully: Otherwise, the ACK does not really
            //  signify the presence of additional bandwidth
            if self.cwnd > num_packets_in_flight {
                trace!("regular congestion control - less than cwnd packets in flight -> no adjustment");
                return;
            }

            let increment: u32 = (self.ai + 1).prechecked_cast();

            self.cwnd_cnt += increment;
            while self.cwnd_cnt >= self.cwnd {
                self.cwnd_cnt -= self.cwnd;
                self.cwnd += 1;
            }
        }

        while self.cwnd > AIMD_VALUES[self.ai].0 {
            self.ai += 1;
        }

        self.cwnd = min(self.cwnd, self.send_window_limit);
        debug!("adjusted cwnd to {} packets", self.cwnd);
    }

    pub fn on_nak(&mut self) {
        let capped_product = self.cwnd.checked_mul(AIMD_VALUES[self.ai].1)
            .unwrap_or(u32::MAX);

        self.cwnd = max(2, self.cwnd - (capped_product >> 8));

        debug!("NAK -> adjusting cwnd downwards to {}", self.cwnd);

        // we reset cwnd_cnt for robustness - without this, the counter could cause cwnd to jump
        // right up again
        self.cwnd_cnt = 0;

        while self.ai > 0 && self.cwnd < AIMD_VALUES[self.ai-1].0 {
            self.ai -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case(10, 0, 10)]
    #[case(30, 0, 30)]
    #[case(38, 0, 38)]
    #[case(39, 1, 39)]
    #[case(99, 1, 99)]
    #[case(100, 1, 100)]
    #[case(101, 1, 100)]
    #[case(1000, 1, 100)]
    fn test_new(#[case] send_window_limit: u32, #[case] expected_ai: usize, #[case] expected_cwnd: u32) {
        let hgc = HsCongestionControl::new(send_window_limit);

        assert_eq!(hgc.ai, expected_ai);
        assert_eq!(hgc.cwnd, expected_cwnd);
        assert_eq!(hgc.cwnd(), expected_cwnd);
    }

    #[rstest]
    #[case(2, true)]
    #[case(10, true)]
    #[case(37, true)]
    #[case(38, true)]
    #[case(39, false)]
    #[case(100, false)]
    #[case(100000, false)]
    fn test_is_slow_start(#[case] cwnd: u32, #[case] expected: bool) {
        let mut hgc = HsCongestionControl::new(100);
        hgc.cwnd = cwnd;
        assert_eq!(hgc.is_slow_start(), expected);
    }

    #[rstest]
    #[case::slow_start_small_load       (0, 1000, 30, 0,  3, 0, 30, 0)]
    #[case::slow_start_belowmedium_load (0, 1000, 30, 0, 14, 0, 30, 0)]
    #[case::slow_start_medium_load      (0, 1000, 30, 0, 15, 0, 31, 0)]
    #[case::slow_start_almost_full_load (0, 1000, 30, 0, 29, 0, 31, 0)]
    #[case::slow_start_full_load        (0, 1000, 30, 0, 30, 0, 31, 0)]

    #[case::slow_start_almostleaving (0, 1000, 37, 0, 37, 0, 38, 0)]
    #[case::slow_start_leaving       (0, 1000, 38, 0, 38, 1, 39, 0)]

    #[case::regular2_small_load       (1, 1000, 110, 0, 1,   1, 110, 0)]
    #[case::regular2_almost_full_load (1, 1000, 110, 0, 109, 1, 110, 0)]
    #[case::regular2_full_load        (1, 1000, 110, 0, 110, 1, 110, 2)]

    #[case::regular_small_load       (5, 1000, 500, 0, 1,   5, 500, 0)]
    #[case::regular_almost_full_load (5, 1000, 500, 0, 499, 5, 500, 0)]
    #[case::regular_full_load        (5, 1000, 500, 0, 500, 5, 500, 6)]

    #[case::regular_cnt              (5, 1000, 500, 400, 500, 5, 500, 406)]
    #[case::regular_cnt_almost_full  (5, 1000, 500, 493, 500, 5, 500, 499)]
    #[case::regular_cnt_exactly_full (5, 1000, 500, 494, 500, 5, 501, 0)]
    #[case::regular_cnt_overflow1    (5, 1000, 500, 495, 500, 5, 501, 1)]
    #[case::regular_cnt_overflow5    (5, 1000, 500, 499, 500, 5, 501, 5)]

    #[case::regular_next_bin_exactly   (5, 1000, 663, 657, 663, 6, 664, 0)]
    #[case::regular_next_bin_overflow1 (5, 1000, 663, 658, 663, 6, 664, 1)]
    #[case::regular_next_bin_overflow5 (5, 1000, 663, 662, 663, 6, 664, 5)]

    #[case::saturated_at_send_window_slow_start (0, 30, 30, 0, 30, 0, 30, 0)]
    #[case::saturated_at_send_window_regular    (5, 500, 500, 3, 500, 5, 500, 3)]

    #[case::huge      (72, 2_000_000_000, 1_000_000_000, 3, 1_000_000_000, 72, 1_000_000_000, 76)]
    #[case::huge_full (72, 2_000_000_000, 1_000_000_000, 999_999_999, 1_000_000_000, 72, 1_000_000_001, 72)]
    fn test_on_ack(
        #[case] ai: usize,
        #[case] send_window_limit: u32,
        #[case] cwnd: u32,
        #[case] cwnd_cnt: u32,
        #[case] num_packets_in_flight: u32,
        #[case] expected_ai: usize,
        #[case] expected_cwnd: u32,
        #[case] expected_cwnd_cnt: u32,
    ) {
        let mut hgc = HsCongestionControl::new(send_window_limit);
        hgc.ai = ai;
        hgc.cwnd = cwnd;
        hgc.cwnd_cnt = cwnd_cnt;

        hgc.on_ack(num_packets_in_flight);

        assert_eq!(hgc.ai, expected_ai);
        assert_eq!(hgc.cwnd, expected_cwnd);
        assert_eq!(hgc.cwnd_cnt, expected_cwnd_cnt);
    }

    #[rstest]
    #[case::regular_inside_bin(1, 220, 1, 124)]
    #[case::regular_across_two_bins(5, 500, 3, 327)]

    #[case::slow_start(0, 30, 0, 15)]
    #[case::slow_start_almost_upper_bound(0, 37, 0, 19)]
    #[case::slow_start_upper_bound(0, 38, 0, 19)]
    #[case::slow_start_near_lower_bound(0, 3, 0, 2)]
    #[case::slow_start_lower_bound(0, 2, 0, 2)]

    #[case::regular_into_slow_start(1, 40, 0, 23)]

    #[case::huge(72, 1_000_000_000, 72, 983_222_785)]
    fn test_on_nak(
        #[case] ai: usize,
        #[case] cwnd: u32,
        #[case] expected_ai: usize,
        #[case] expected_cwnd: u32,
    ) {
        let mut hgc = HsCongestionControl::new(2_000_000_000);
        hgc.ai = ai;
        hgc.cwnd = cwnd;
        hgc.cwnd_cnt = 3;

        hgc.on_nak();

        assert_eq!(hgc.ai, expected_ai);
        assert_eq!(hgc.cwnd, expected_cwnd);
        assert_eq!(hgc.cwnd_cnt, 0);
    }
}
