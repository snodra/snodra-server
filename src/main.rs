//! snodra-server binary: configuration, telemetry, then `snodra_server::serve_with_shutdown`.
//!
//! `snodra-server --health-check` instead probes the local hub's gRPC health service (for the
//! image's `HEALTHCHECK`; the distroless image has no shell or curl) and exits 0 when it is
//! `SERVING`, 1 otherwise. `snodra-server --topology [--json]` prints the local hub's topology; both
//! take `--address` and `--tls-ca` to probe another hub or speak TLS (ADR 0018).
//! `snodra-server acl hash <token|->` prints the digest an ACL file holds for a token.

use std::io::Read;
use std::process::ExitCode;
use std::time::Duration;

use snodra_server::cli::{self, Mode, Remote};
use snodra_server::client::{ClientIdentity, Target};
use snodra_server::config::Config;
use snodra_server::tls::ClientAuthMode;
use tokio::net::TcpListener;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let mode = match cli::parse(std::env::args().skip(1)) {
        Ok(mode) => mode,
        Err(e) => {
            eprintln!("{e}\n{}", cli::USAGE);
            return Ok(ExitCode::from(2));
        }
    };
    if let Mode::AclHash { token } = mode {
        return Ok(acl_hash(token));
    }
    let config = Config::from_env()?;
    match mode {
        Mode::HealthCheck(remote) => return Ok(health_check(&config, &remote).await),
        Mode::Topology { json, remote } => return Ok(topology(&config, &remote, json).await),
        Mode::Serve | Mode::AclHash { .. } => {}
    }
    let telemetry = snodra_server::telemetry::init(&config)?;
    for warning in config.security_warnings() {
        tracing::warn!("{warning}");
    }
    snodra_server::reload_on_sighup();
    let listener = TcpListener::bind(config.listen).await?;
    let http = snodra_server::bind_http(&config).await?;
    snodra_server::serve_with_shutdown(config, listener, http, snodra_server::shutdown_signal())
        .await?;
    // Flushes pending spans and metrics.
    drop(telemetry);
    Ok(ExitCode::SUCCESS)
}

/// Prints the ACL digest of a token, never the token. With `-` the token comes from stdin, so it
/// stays out of the shell history and the process list.
fn acl_hash(token: Option<String>) -> ExitCode {
    let token = match token {
        Some(token) => token,
        None => {
            let mut input = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut input) {
                eprintln!("cannot read the token from stdin: {e}");
                return ExitCode::FAILURE;
            }
            input.trim_end_matches(['\n', '\r']).to_string()
        }
    };
    if token.is_empty() {
        eprintln!("the token is empty");
        return ExitCode::from(2);
    }
    println!("{}", snodra_server::auth::hash_token(&token));
    ExitCode::SUCCESS
}

/// The hub to probe: `--address` or the local hub, with TLS as `--tls-ca`, the address scheme or
/// the environment says.
fn target(config: &Config, remote: &Remote) -> Target {
    let identity = ClientIdentity::resolve(
        remote
            .tls_client_cert
            .as_deref()
            .zip(remote.tls_client_key.as_deref()),
        remote.address.is_none(),
        config.grpc_tls(),
        config.client_auth() == Some(ClientAuthMode::Required),
    );
    Target::resolve(
        remote.address.as_deref(),
        remote.tls_ca.as_deref(),
        config.listen.port(),
        config.grpc_tls().map(|(cert, _key)| cert),
    )
    .with_identity(identity)
}

async fn health_check(config: &Config, remote: &Remote) -> ExitCode {
    let target = target(config, remote);
    let addr = &target.address;
    match tokio::time::timeout(PROBE_TIMEOUT, snodra_server::health_probe_target(&target)).await {
        Ok(Ok(true)) => {
            println!("snodra-server at {addr}: SERVING");
            ExitCode::SUCCESS
        }
        Ok(Ok(false)) => {
            println!("snodra-server at {addr}: NOT_SERVING");
            ExitCode::FAILURE
        }
        Ok(Err(e)) => {
            println!("snodra-server at {addr}: health check failed: {e:#}");
            ExitCode::FAILURE
        }
        Err(_) => {
            println!("snodra-server at {addr}: health check timed out after {PROBE_TIMEOUT:?}");
            ExitCode::FAILURE
        }
    }
}

async fn topology(config: &Config, remote: &Remote, json: bool) -> ExitCode {
    let target = target(config, remote);
    let addr = &target.address;
    // The dashboard token of the hub being probed; the same variable the hub reads.
    let token = std::env::var("KRAKEN_HUB_DASHBOARD_TOKEN")
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let probe = snodra_server::topology_probe_target(&target, token.as_deref());
    let result = match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(result) => result.map_err(|e| format!("{e:#}")),
        Err(_) => Err(format!("timed out after {PROBE_TIMEOUT:?}")),
    };
    let rendered = result.and_then(|t| {
        if json {
            serde_json::to_string_pretty(&t).map_err(|e| e.to_string())
        } else {
            Ok(cli::render_table(&t).trim_end().to_string())
        }
    });
    match rendered {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("snodra-server at {addr}: topology failed: {e}");
            ExitCode::FAILURE
        }
    }
}
