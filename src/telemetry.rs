//! Tracing setup.

use std::sync::Arc;
use std::time::Duration;

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::Config;
use crate::registry::Kind;

/// Installs the JSON `tracing` subscriber filtered by `config.log`.
pub fn init(config: &Config) {
    tracing_subscriber::registry()
        .with(EnvFilter::new(&config.log))
        .with(fmt::layer().json())
        .init();
}

/// How a routed request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    NoHandler,
    InvalidRequest,
    Timeout,
    HandlerDisconnected,
    HandlerError,
}

/// Router metrics. No-op until the telemetry task fills in the bodies.
pub struct Metrics;

impl Metrics {
    pub fn noop() -> Arc<Self> {
        Arc::new(Self)
    }

    pub fn request_started(&self, _kind: Kind) {}

    pub fn request_finished(
        &self,
        _kind: Kind,
        _type_name: &str,
        _outcome: Outcome,
        _duration: Duration,
    ) {
    }

    pub fn buffer_full(&self, _service: &str) {}
}
