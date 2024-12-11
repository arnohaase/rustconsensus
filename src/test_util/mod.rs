//! This module contains utilities that are useful for testing code based Cluster functionality.
//!  They are used for testing cluster functionality itself, but they are also exported for
//!  application testing.
//!
//! There are macros in this test utilities, and Rust does not provide a way to reference a type T
//!  from a macro that works both in the crate defining T, and a third party crate. Making test
//!  utilities part of the crate's regular (non-#[cfg(test)]) code is the compromise I picked.

pub mod node;
pub mod message;
pub mod event;


#[cfg(test)]
mod tests {
    use tracing::Level;

    #[ctor::ctor]
    fn init_test_logging() {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(Level::DEBUG)
            // .with_max_level(Level::TRACE)
            .try_init()
            .ok();
    }
}
