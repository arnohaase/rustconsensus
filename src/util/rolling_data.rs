


/// This is a buffer implementation that provides mean and standard deviation on a sliding window
///  in a stream of data, in an efficient way.
///
/// The generic parameter is the size of the sliding window.
pub struct RollingData<const N: usize> {
    buf: BufferImpl<N>,
    cached_sum: f64,
    cached_square_sum: f64,
}
impl<const N: usize> RollingData<N> {
    pub fn new(initial_value: f64) -> Self {
        let mut buf = BufferImpl::new();
        assert!(buf.add_value(initial_value).is_none());

        RollingData {
            buf,
            cached_sum: initial_value,
            cached_square_sum: initial_value * initial_value,
        }
    }

    pub fn add_value(&mut self, value: f64) {
        if let Some(evicted) = self.buf.add_value(value) {
            self.cached_sum -= evicted;
            self.cached_square_sum -= evicted * evicted;
        }

        self.cached_sum += value;
        self.cached_square_sum += value * value;
    }

    pub fn mean(&self) -> f64 {
        self.cached_sum / self.buf.len() as f64
    }

    pub fn std_dev(&self) -> f64 {
        if self.buf.len() < 2 {
            // pragmatic value that serves the purpose of standard deviation in this context
            return 0.0;
        }

        let mean = self.mean();
        let diff_of_squares = self.cached_square_sum - mean * mean * self.buf.len() as f64;

        (diff_of_squares / (self.buf.len() - 1) as f64).sqrt()
    }
}


enum BufferImpl<const N: usize> {
    Growing(Vec<f64>),
    Ring {
        buf: Vec<f64>,
        next: usize,
    },
}
impl <const N: usize> BufferImpl<N> {
    fn new() -> BufferImpl<N> {
        BufferImpl::Growing(vec![])
    }

    fn len(&self) -> usize {
        match self {
            BufferImpl::Growing(buf) => buf.len(),
            BufferImpl::Ring { buf, .. } => buf.len(),
        }
    }

    /// adds a new value, returning the value that was evicted in its place (if any)
    #[must_use]
    fn add_value(&mut self, value: f64) -> Option<f64> {
        match self {
            BufferImpl::Growing(buf) => {
                buf.push(value);
                if buf.len() == N {
                    let buf = std::mem::take(buf);
                    *self = BufferImpl::Ring { buf, next: 0 };
                }
                None
            }
            BufferImpl::Ring { buf, next } => {
                let evicted = buf[*next];
                buf[*next] = value;
                *next = (*next + 1) % N;
                Some(evicted)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::assert_approx_eq;
    use super::*;

    #[test]
    fn test_rolling_data() {
        let mut data = RollingData::<4>::new(1.0);
        assert_approx_eq(data.mean(), 1.0);
        assert_approx_eq(data.std_dev(), 0.0);

        data.add_value(2.0);
        assert_approx_eq(data.mean(), 1.5);
        assert_approx_eq(data.std_dev(), 0.5f64.sqrt());

        data.add_value(1.5);
        assert_approx_eq(data.mean(), 1.5);
        assert_approx_eq(data.std_dev(), 0.5);

        data.add_value(4.0);
        assert_approx_eq(data.mean(), 2.125);
        assert_approx_eq(data.std_dev(), 1.3149778198382918);

        data.add_value(5.0);
        assert_approx_eq(data.mean(), 3.125);
        assert_approx_eq(data.std_dev(), 1.6520189667999174);

        data.add_value(4.0);
        assert_approx_eq(data.mean(), 3.625);
        assert_approx_eq(data.std_dev(), 1.4930394055974097);

        data.add_value(5.0);
        assert_approx_eq(data.mean(), 4.5);
        assert_approx_eq(data.std_dev(), (1.0f64/3.0).sqrt());
    }
}
