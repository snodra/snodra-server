//! Tracing setup.

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::Config;

/// Installs the JSON `tracing` subscriber filtered by `config.log`.
pub fn init(config: &Config) {
    tracing_subscriber::registry()
        .with(EnvFilter::new(&config.log))
        .with(fmt::layer().json())
        .init();
}
