use std::cmp::min;

pub struct ExponentialBackoff {
    counter: usize,
    send_threshold: usize,
    
    config_initial_threshold: usize,
    config_max_threshold: usize,
    config_threshold_factor_eighths: usize,
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        let result = ExponentialBackoff {
            counter: 0,
            send_threshold: 2,
            config_initial_threshold: 2, //TODO make this configurable?
            config_max_threshold: 1000,
            config_threshold_factor_eighths: 8*2,
        };
        
        assert!(result.config_threshold_factor_eighths >= 8);
        
        result
    }
    
    #[must_use]
    pub fn should_send(&mut self, reset: bool) -> bool {
        if reset {
            self.counter = 0;
            self.send_threshold = self.config_initial_threshold;
            return true;
        }
        
        self.counter += 1;
        if self.counter < self.send_threshold {
            return false;
        }
        
        self.counter = 0;
        self.send_threshold = (self.send_threshold * self.config_threshold_factor_eighths) >> 3;
        self.send_threshold = min(self.send_threshold, self.config_max_threshold);
        true
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;
    
    #[rstest]
    #[case::initial(0, 2, 2, 1000, 16, false, false, 1, 2)]
    #[case::initial_threshold(1, 2, 2, 1000, 16, false, true, 0, 4)]
    #[case::later(1, 8, 2, 1000, 16, false, false, 2, 8)]
    #[case::later_threshold(7, 8, 2, 1000, 16, false, true, 0, 16)]
    #[case::half(510, 512, 2, 1000, 16, false, false, 511, 512)]
    #[case::half_threshold(511, 512, 2, 1000, 16, false, true, 0, 1000)]
    #[case::max(998, 1000, 2, 1000, 16, false, false, 999, 1000)]
    #[case::max_threshold(999, 1000, 2, 1000, 16, false, true, 0, 1000)]
    
    #[case::factor_1_5                (0,   2, 2, 1000, 12, false, false, 1, 2)]
    #[case::factor_1_5_threshold      (1,   2, 2, 1000, 12, false, true,  0, 3)]
    #[case::factor_1_5_later          (1,  12, 2, 1000, 12, false, false, 2, 12)]
    #[case::factor_1_5_later_threshold(11, 12, 2, 1000, 12, false, true,  0, 18)]
    #[case::factor_1_5_max            (800, 1000, 2, 1000, 12, false, false, 801, 1000)]
    #[case::factor_1_5_max_threshold  (999, 1000, 2, 1000, 12, false, true,  0,   1000)]
    
    #[case::initial_reset(0, 2, 2, 1000, 16, true, true, 0, 2)]
    #[case::initial_2_reset(1, 2, 2, 1000, 16, true, true, 0, 2)]
    #[case::later_reset(3, 8, 2, 1000, 16, true, true, 0, 2)]
    #[case::half_reset(510, 512, 2, 1000, 16, true, true, 0, 2)]
    #[case::half_reset_2(511, 512, 2, 1000, 16, true, true, 0, 2)]
    #[case::max_reset(900, 1000, 2, 1000, 16, true, true, 0, 2)]
    #[case::max_reset_2(999, 1000, 2, 1000, 16, true, true, 0, 2)]
    fn test_should_send(
        #[case] counter: usize, 
        #[case] send_threshold: usize,
        #[case] config_initial_threshold: usize,
        #[case] config_max_threshold: usize,
        #[case] config_factor: usize,
        #[case] reset: bool,
        #[case] expected_result: bool,
        #[case] expected_counter: usize,
        #[case] expected_threshold: usize,
    ) {
        let mut eb = ExponentialBackoff {
            counter,
            send_threshold,
            config_initial_threshold,
            config_max_threshold,
            config_threshold_factor_eighths:config_factor,
        };
        
        assert_eq!(eb.should_send(reset), expected_result);
        assert_eq!(eb.counter, expected_counter);
        assert_eq!(eb.send_threshold, expected_threshold);
    }
}