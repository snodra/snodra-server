//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

mod config;
mod registry;
mod service;

pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

use std::sync::Arc;

use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::Config;
use crate::proto::hub_server::HubServer;
use crate::registry::Registry;
use crate::service::HubService;

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().json())
        .init();
    // TODO: when KRAKEN_HUB_OTLP_ENDPOINT is set, add a tracing-opentelemetry layer exporting
    // over opentelemetry-otlp (grpc-tonic) and register a metrics exporter.
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = Config::from_env()?;
    let registry = Arc::new(Registry::new());
    info!(listen = %config.listen, "kraken-hub starting");

    let svc = HubService { registry, config: config.clone() };
    Server::builder()
        .add_service(HubServer::new(svc))
        .serve(config.listen)
        .await?;
    Ok(())
}
