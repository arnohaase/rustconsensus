


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

    //TODO unit test
    pub fn mean(&self) -> f64 {
        self.cached_sum / self.buf.len() as f64
    }

    //TODO unit test
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

    //TODO unit test
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
