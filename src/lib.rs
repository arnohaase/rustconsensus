pub mod node;
pub mod node_addr;
pub mod comm;



#[cfg(test)]
mod test {
    use tracing::Level;

    #[ctor::ctor]
    fn init_test_logging() {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .try_init()
            .ok();
    }
}
