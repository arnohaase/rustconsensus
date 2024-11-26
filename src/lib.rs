pub mod messaging;
pub mod cluster;
pub mod util;
pub mod test_util;


#[cfg(test)]
mod test {
    use tracing::{error, Level};

    #[ctor::ctor]
    fn init_test_logging() {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(Level::TRACE)
            .try_init()
            .ok();
    }
}
