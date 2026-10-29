//! kraken-hub binary: configuration, telemetry, then `kraken_hub::serve`.

use kraken_hub::config::Config;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let _telemetry = kraken_hub::telemetry::init(&config)?;
    let listener = TcpListener::bind(config.listen).await?;
    kraken_hub::serve(config, listener).await
}
