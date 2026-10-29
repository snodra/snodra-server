//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

pub mod config;
pub mod registry;
pub mod router;
pub mod service;
pub mod session;
pub mod telemetry;

pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

use config::Config;
use tokio::net::TcpListener;

/// Runs the hub on `listener` until shut down. Body is filled in by the routing tasks.
pub async fn serve(_config: Config, _listener: TcpListener) -> anyhow::Result<()> {
    Ok(())
}
