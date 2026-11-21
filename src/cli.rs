//! Command-line modes of the binary: argument parsing and the `--topology` table renderer (with
//! each service's event processors).

use std::fmt;
use std::path::PathBuf;

use crate::topology::{InstanceTopology, Topology};

pub const USAGE: &str = "\
usage:
  snodra-server [SETTINGS]                     run the hub
  snodra-server --health-check [REMOTE]        probe the hub's gRPC health service
  snodra-server --topology [--json] [REMOTE]   print the hub's topology (table, or JSON with --json)
  snodra-server acl hash <token|->             print the SHA-256 (hex) of a token for the ACL file
  snodra-server --version | --help

SETTINGS: every one is also an SNODRA_SERVER_* environment variable, and a flag wins over it.
  --listen <addr>                 gRPC bind address                 (SNODRA_SERVER_LISTEN, 0.0.0.0:7000);
                                  with --health-check/--topology, the hub to probe
  --http-listen <addr|\"\">         HTTP bind address, empty: off     (SNODRA_SERVER_HTTP_LISTEN, 0.0.0.0:7001)
  --id <id>                       this hub's id                    (SNODRA_SERVER_ID)
  --log <filter>                  tracing filter                   (SNODRA_SERVER_LOG, info)
  --log-format <text|json>        log lines for a terminal or a collector (SNODRA_SERVER_LOG_FORMAT, text)
  --otlp-endpoint <url|\"\">        OTLP/gRPC endpoint, empty: no telemetry (SNODRA_SERVER_OTLP_ENDPOINT, off)
  --auth-mode <off|permissive|enforce>                             (SNODRA_SERVER_AUTH_MODE, permissive)
  --acl-file <path>               token digests and their rights   (SNODRA_SERVER_ACL_FILE)
  --dashboard-token-file <path>   bearer token of the read API     (SNODRA_SERVER_DASHBOARD_TOKEN_FILE)
  --write-token-file <path>       bearer token of the controls     (SNODRA_SERVER_DASHBOARD_WRITE_TOKEN_FILE)
  --jwt-issuer <iss>              accepted JWT issuer              (SNODRA_SERVER_JWT_ISSUER)
  --jwt-jwks-uri <url>            where its signing keys live      (SNODRA_SERVER_JWT_JWKS_URI)
  --jwt-audience <aud>            required aud, when set            (SNODRA_SERVER_JWT_AUDIENCE)
  --tls-cert <pem> --tls-key <pem>            TLS for gRPC          (SNODRA_SERVER_TLS_CERT / _TLS_KEY)
  --http-tls-cert <pem> --http-tls-key <pem>  TLS for HTTP          (SNODRA_SERVER_HTTP_TLS_CERT / _KEY)
  --grafana-url <url>             Grafana link in the dashboard    (SNODRA_SERVER_GRAFANA_URL)
  --peers <url[,url]>             other hubs' dashboards           (SNODRA_SERVER_PEERS)

REMOTE (default: the local hub, TLS when the environment configures it):
  --address <host:port|https://host:port>  the hub to probe
  --tls-ca <pem>                          trust this CA (turns TLS on); https:// alone uses the system roots
  --tls-client-cert <pem> --tls-client-key <pem>  present this client certificate

A hub with SNODRA_SERVER_DASHBOARD_TOKEN needs the same variable in the probe's environment for --topology.
Without --address or --listen the probes use SNODRA_SERVER_LISTEN's port on 127.0.0.1 (7000 by default);
on macOS port 7000 is AirPlay's, so pass --listen when the hub runs elsewhere.";

/// Which hub a probe talks to; see [`crate::client::Target::resolve`].
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Remote {
    /// `--address`: `host:port`, `http://host:port` or `https://host:port`. `None`: the local hub.
    pub address: Option<String>,
    /// `--tls-ca`: PEM file with the CA(s) to trust; turns TLS on.
    pub tls_ca: Option<PathBuf>,
    /// `--tls-client-cert` and `--tls-client-key`: the client certificate and key (PEM) to
    /// present, for a hub that asks for one. Both or neither.
    pub tls_client_cert: Option<PathBuf>,
    pub tls_client_key: Option<PathBuf>,
    /// `--listen` in a probe mode: the address the hub was started on, when it is not the default.
    pub local: Option<String>,
}

/// Settings given as flags for the serving mode; each overrides the environment variable of the
/// same name (see [`USAGE`]). Empty strings for `--http-listen` and `--otlp-endpoint` turn the
/// listener off and telemetry off respectively.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Settings {
    pub listen: Option<String>,
    pub http_listen: Option<String>,
    pub id: Option<String>,
    pub log: Option<String>,
    pub log_format: Option<String>,
    pub otlp_endpoint: Option<String>,
    pub auth_mode: Option<String>,
    pub acl_file: Option<PathBuf>,
    pub dashboard_token_file: Option<PathBuf>,
    pub write_token_file: Option<PathBuf>,
    pub jwt_issuer: Option<String>,
    pub jwt_jwks_uri: Option<String>,
    pub jwt_audience: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub http_tls_cert: Option<PathBuf>,
    pub http_tls_key: Option<PathBuf>,
    pub grafana_url: Option<String>,
    pub peers: Option<String>,
}

impl Settings {
    /// No flag was given; the environment alone decides.
    pub fn is_empty(&self) -> bool {
        *self == Settings::default()
    }

    /// Applies the flags on top of the environment-derived configuration.
    pub fn apply(&self, c: &mut crate::config::Config) -> anyhow::Result<()> {
        if let Some(v) = &self.listen {
            c.listen = v.parse().map_err(|e| anyhow::anyhow!("--listen: {e}"))?;
        }
        if let Some(v) = &self.http_listen {
            c.http_listen = if v.trim().is_empty() {
                None
            } else {
                Some(
                    v.parse()
                        .map_err(|e| anyhow::anyhow!("--http-listen: {e}"))?,
                )
            };
        }
        if let Some(v) = &self.id {
            c.hub_id = v.trim().to_string();
        }
        if let Some(v) = &self.log {
            c.log = v.clone();
        }
        if let Some(v) = &self.log_format {
            c.log_format = v.parse()?;
        }
        if let Some(v) = &self.otlp_endpoint {
            c.otlp_endpoint = Some(v.clone()).filter(|v| !v.trim().is_empty());
        }
        if let Some(v) = &self.auth_mode {
            c.auth_mode = v.parse()?;
        }
        if let Some(v) = &self.acl_file {
            c.acl_file = Some(v.clone());
        }
        if let Some(v) = &self.dashboard_token_file {
            c.dashboard_token_file = Some(v.clone());
        }
        if let Some(v) = &self.write_token_file {
            c.dashboard_write_token_file = Some(v.clone());
        }
        if let Some(v) = &self.jwt_issuer {
            c.jwt_issuer = Some(v.clone()).filter(|v| !v.trim().is_empty());
        }
        if let Some(v) = &self.jwt_jwks_uri {
            c.jwt_jwks_uri = Some(v.clone()).filter(|v| !v.trim().is_empty());
        }
        if let Some(v) = &self.jwt_audience {
            c.jwt_audience = Some(v.clone()).filter(|v| !v.trim().is_empty());
        }
        if let Some(v) = &self.tls_cert {
            c.tls_cert = Some(v.clone());
        }
        if let Some(v) = &self.tls_key {
            c.tls_key = Some(v.clone());
        }
        if let Some(v) = &self.http_tls_cert {
            c.http_tls_cert = Some(v.clone());
        }
        if let Some(v) = &self.http_tls_key {
            c.http_tls_key = Some(v.clone());
        }
        if let Some(v) = &self.grafana_url {
            c.grafana_url = Some(v.clone()).filter(|v| !v.trim().is_empty());
        }
        if let Some(v) = &self.peers {
            c.peers = v
                .split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect();
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Mode {
    Serve(Box<Settings>),
    Help,
    Version,
    HealthCheck(Remote),
    Topology {
        json: bool,
        remote: Remote,
    },
    /// `acl hash`: the token is the argument, or read from stdin when it is `-`.
    AclHash {
        token: Option<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct UsageError(pub String);

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UsageError {}

/// Parses the arguments after the program name.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Mode, UsageError> {
    let args: Vec<String> = args.into_iter().collect();
    if args.first().map(String::as_str) == Some("acl") {
        return match &args[1..] {
            [sub, token] if sub == "hash" => Ok(Mode::AclHash {
                token: (token != "-").then(|| token.clone()),
            }),
            _ => Err(UsageError("usage: snodra-server acl hash <token|->".into())),
        };
    }
    let (mut health, mut topology, mut json) = (false, false, false);
    let mut remote = Remote::default();
    let mut settings = Settings::default();
    let mut listen: Option<String> = None;
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let value = |name: &str, it: &mut dyn Iterator<Item = String>| {
            it.next()
                .ok_or_else(|| UsageError(format!("{name} needs a value")))
        };
        match arg.as_str() {
            "--help" | "-h" => return Ok(Mode::Help),
            "--version" | "-V" => return Ok(Mode::Version),
            "--health-check" => health = true,
            "--topology" => topology = true,
            "--json" => json = true,
            "--address" => remote.address = Some(value("--address", &mut it)?),
            "--tls-ca" => remote.tls_ca = Some(value("--tls-ca", &mut it)?.into()),
            "--tls-client-cert" => {
                remote.tls_client_cert = Some(value("--tls-client-cert", &mut it)?.into())
            }
            "--tls-client-key" => {
                remote.tls_client_key = Some(value("--tls-client-key", &mut it)?.into())
            }
            "--listen" => listen = Some(value("--listen", &mut it)?),
            "--http-listen" => settings.http_listen = Some(value("--http-listen", &mut it)?),
            "--id" => settings.id = Some(value("--id", &mut it)?),
            "--log" => settings.log = Some(value("--log", &mut it)?),
            "--log-format" => settings.log_format = Some(value("--log-format", &mut it)?),
            "--otlp-endpoint" => settings.otlp_endpoint = Some(value("--otlp-endpoint", &mut it)?),
            "--auth-mode" => settings.auth_mode = Some(value("--auth-mode", &mut it)?),
            "--acl-file" => settings.acl_file = Some(value("--acl-file", &mut it)?.into()),
            "--dashboard-token-file" => {
                settings.dashboard_token_file =
                    Some(value("--dashboard-token-file", &mut it)?.into())
            }
            "--write-token-file" => {
                settings.write_token_file = Some(value("--write-token-file", &mut it)?.into())
            }
            "--jwt-issuer" => settings.jwt_issuer = Some(value("--jwt-issuer", &mut it)?),
            "--jwt-jwks-uri" => settings.jwt_jwks_uri = Some(value("--jwt-jwks-uri", &mut it)?),
            "--jwt-audience" => settings.jwt_audience = Some(value("--jwt-audience", &mut it)?),
            "--tls-cert" => settings.tls_cert = Some(value("--tls-cert", &mut it)?.into()),
            "--tls-key" => settings.tls_key = Some(value("--tls-key", &mut it)?.into()),
            "--http-tls-cert" => {
                settings.http_tls_cert = Some(value("--http-tls-cert", &mut it)?.into())
            }
            "--http-tls-key" => {
                settings.http_tls_key = Some(value("--http-tls-key", &mut it)?.into())
            }
            "--grafana-url" => settings.grafana_url = Some(value("--grafana-url", &mut it)?),
            "--peers" => settings.peers = Some(value("--peers", &mut it)?),
            _ => return Err(UsageError(format!("unknown argument: {arg}"))),
        }
    }
    if remote.tls_client_cert.is_some() != remote.tls_client_key.is_some() {
        return Err(UsageError(
            "--tls-client-cert and --tls-client-key must be given together".into(),
        ));
    }
    if topology && health {
        return Err(UsageError(
            "--topology and --health-check are mutually exclusive".into(),
        ));
    }
    if json && !topology {
        return Err(UsageError("--json requires --topology".into()));
    }
    if remote != Remote::default() && !topology && !health {
        return Err(UsageError(
            "--address, --tls-ca and --tls-client-cert/--tls-client-key need --topology or --health-check"
                .into(),
        ));
    }
    if (topology || health) && listen.is_some() {
        remote.local = listen.take();
    } else {
        settings.listen = listen;
    }
    if !settings.is_empty() && (topology || health) {
        return Err(UsageError(
            "SETTINGS apply when running the hub; --topology and --health-check take --address and --tls-*"
                .into(),
        ));
    }
    Ok(if topology {
        Mode::Topology { json, remote }
    } else if health {
        Mode::HealthCheck(remote)
    } else {
        Mode::Serve(Box::new(settings))
    })
}

/// `800ms`, `45s`, `3m05s`, `1h02m`, `2d03h`; negative values read as `0ms`.
pub fn fmt_duration_ms(ms: i64) -> String {
    let ms = ms.max(0);
    let s = ms / 1000;
    if s == 0 {
        format!("{ms}ms")
    } else if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else if s < 86_400 {
        format!("{}h{:02}m", s / 3600, s % 3600 / 60)
    } else {
        format!("{}d{:02}h", s / 86_400, s % 86_400 / 3600)
    }
}

fn join_or_dash(items: &[String]) -> String {
    if items.is_empty() {
        "-".into()
    } else {
        items.join(",")
    }
}

/// Fixed-width rows (first row is the header), indented, trailing spaces trimmed. Every cell
/// goes through [`one_line`]: its text comes from clients (names, ids, types, errors).
fn table(rows: &[Vec<String>]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|cell| one_line(cell)).collect())
        .collect();
    let cols = rows[0].len();
    let widths: Vec<usize> = (0..cols)
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in &rows {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, w)| format!("{cell:<w$}"))
            .collect();
        out.push_str(format!("  {}", cells.join(" | ")).trim_end());
        out.push('\n');
    }
    out
}

fn instance_row(i: &InstanceTopology, now_ms: i64) -> Vec<String> {
    vec![
        i.instance_id.clone(),
        fmt_duration_ms(now_ms - i.connected_since_ms),
        fmt_duration_ms(i.last_heartbeat_age_ms),
        i.stale.to_string(),
        format!("{}/{}", i.in_flight_as_handler, i.in_flight_as_sender),
        join_or_dash(&i.command_types),
        join_or_dash(&i.query_types),
    ]
}

/// Segment names (sorted) with runs of consecutive decimal names collapsed: `0-2,5,7-8`; other
/// names as they are; `-` when none.
fn segment_ranges(segments: &[&str]) -> String {
    enum Run<'a> {
        Numbers(u64, u64),
        Name(&'a str),
    }
    let mut runs: Vec<Run> = Vec::new();
    for &s in segments {
        // Only a canonical decimal name joins a run: `01` stays as it is.
        let number = s.parse::<u64>().ok().filter(|n| n.to_string() == s);
        match (runs.last_mut(), number) {
            (Some(Run::Numbers(_, end)), Some(n)) if n == end.saturating_add(1) => *end = n,
            (_, Some(n)) => runs.push(Run::Numbers(n, n)),
            (_, None) => runs.push(Run::Name(s)),
        }
    }
    if runs.is_empty() {
        return "-".into();
    }
    runs.iter()
        .map(|run| match run {
            Run::Numbers(a, b) if a == b => a.to_string(),
            Run::Numbers(a, b) => format!("{a}-{b}"),
            Run::Name(s) => s.to_string(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Longest last error the table shows, in characters; longer ones end with `...`.
const LAST_ERROR_CHARS: usize = 60;

/// `text` on one line: control characters (a newline, a tab, an ANSI escape) become spaces and
/// every run of whitespace one space, so free text from a library never breaks a table row.
fn one_line(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// The state of a processor as the table shows it: the state, then `replaying` while a replay
/// range is open and `op <first 8>` of the reset holding its lock.
fn state_cell(p: &crate::processors::ProcessorReport) -> String {
    let mut cell = p.state.as_str().to_string();
    if p.replaying {
        cell.push_str(" replaying");
    }
    if !p.operation_id.is_empty() {
        cell.push_str(" op ");
        cell.extend(p.operation_id.chars().take(8));
    }
    cell
}

/// One row per (processor, instance) of a service whose instances report processors,
/// sorted by processor name, then instance id.
fn processor_rows(instances: &[InstanceTopology], now_ms: i64) -> Vec<Vec<String>> {
    let mut rows: Vec<(String, String, Vec<String>)> = Vec::new();
    for i in instances {
        for p in &i.processors {
            let known: Vec<i64> = p
                .segments
                .iter()
                .map(|x| x.lag)
                .filter(|l| *l >= 0)
                .collect();
            let lag = if known.is_empty() {
                "-".to_string()
            } else {
                known
                    .iter()
                    .fold(0i64, |a, l| a.saturating_add(*l))
                    .to_string()
            };
            let dead_letters =
                if p.dead_letter_mode == "collection" || p.dead_letter_mode.is_empty() {
                    p.dead_letters.to_string()
                } else {
                    format!("({})", p.dead_letter_mode)
                };
            let error = one_line(&p.last_error);
            let last_error = if error.is_empty() {
                "-".to_string()
            } else {
                let mut text: String = error.chars().take(LAST_ERROR_CHARS).collect();
                if error.chars().count() > LAST_ERROR_CHARS {
                    text.push_str("...");
                }
                if p.last_error_at_ms > 0 {
                    text.push_str(&format!(
                        " ({} ago)",
                        fmt_duration_ms(now_ms - p.last_error_at_ms)
                    ));
                }
                text
            };
            let segments: Vec<&str> = p.segments.iter().map(|x| x.segment.as_str()).collect();
            rows.push((
                p.name.clone(),
                i.instance_id.clone(),
                vec![
                    p.name.clone(),
                    i.instance_id.clone(),
                    state_cell(p),
                    p.subscription.clone(),
                    p.source.clone(),
                    segment_ranges(&segments),
                    lag,
                    p.processed_total.to_string(),
                    p.failed_total.to_string(),
                    dead_letters,
                    last_error,
                ],
            ));
        }
    }
    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

/// Human-readable topology. Every duration derives from `t.now_ms`, never the local clock.
pub fn render_table(t: &Topology) -> String {
    let mut out = String::new();
    if t.services.is_empty() {
        out.push_str("no services connected\n");
    }
    for service in &t.services {
        out.push_str(&format!(
            "service {} ({} instances)\n",
            one_line(&service.service_name),
            service.instances.len()
        ));
        let mut rows = vec![
            [
                "instance",
                "connected for",
                "heartbeat age",
                "stale",
                "in-flight h/s",
                "commands",
                "queries",
            ]
            .map(String::from)
            .to_vec(),
        ];
        rows.extend(service.instances.iter().map(|i| instance_row(i, t.now_ms)));
        out.push_str(&table(&rows));
        let processors = processor_rows(&service.instances, t.now_ms);
        if !processors.is_empty() {
            let mut rows = vec![
                [
                    "processor",
                    "instance",
                    "state",
                    "subscription",
                    "source",
                    "segments",
                    "lag",
                    "processed",
                    "failed",
                    "dead letters",
                    "last error",
                ]
                .map(String::from)
                .to_vec(),
            ];
            rows.extend(processors);
            out.push_str(&table(&rows));
        }
        out.push('\n');
    }
    let orphans: Vec<_> = t.types.iter().filter(|ty| ty.orphaned).collect();
    if !orphans.is_empty() {
        out.push_str("orphaned types:\n");
        for ty in orphans {
            let kind = match ty.kind {
                crate::topology::KindName::Command => "COMMAND",
                crate::topology::KindName::Query => "QUERY",
            };
            out.push_str(&format!(
                "  {} {} owner={} for {}\n",
                one_line(&ty.type_name),
                kind,
                // Every owner of a shared query type, comma-separated.
                one_line(&ty.owner_services.join(",")),
                fmt_duration_ms(ty.orphaned_for_ms)
            ));
        }
        out.push('\n');
    }
    let c = &t.totals;
    out.push_str(&format!(
        "totals: services={} instances={} commandTypes={} queryTypes={} inFlight={}\n",
        c.services, c.instances, c.command_types, c.query_types, c.in_flight
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{KindName, ServiceTopology, Totals, TypeOwnership};

    fn args(a: &[&str]) -> Result<Mode, UsageError> {
        parse(a.iter().map(|s| s.to_string()))
    }

    fn err(a: &[&str]) -> String {
        args(a).unwrap_err().0
    }

    #[test]
    fn parse_modes_and_usage_errors() {
        assert_eq!(args(&[]), Ok(Mode::Serve(Box::default())));
        let local = Remote::default;
        assert_eq!(args(&["--health-check"]), Ok(Mode::HealthCheck(local())));
        assert_eq!(
            args(&["--topology"]),
            Ok(Mode::Topology {
                json: false,
                remote: local()
            })
        );
        assert_eq!(
            args(&["--topology", "--json"]),
            Ok(Mode::Topology {
                json: true,
                remote: local()
            })
        );
        assert_eq!(
            args(&["--json", "--topology"]),
            Ok(Mode::Topology {
                json: true,
                remote: local()
            })
        );
        assert_eq!(err(&["--json"]), "--json requires --topology");
        assert_eq!(
            err(&["--topology", "--health-check"]),
            "--topology and --health-check are mutually exclusive"
        );
        assert_eq!(
            err(&["--health-check", "--topology", "--json"]),
            "--topology and --health-check are mutually exclusive"
        );
        assert_eq!(
            args(&["--topology", "--listen", "0.0.0.0:7010"]),
            Ok(Mode::Topology {
                json: false,
                remote: Remote {
                    local: Some("0.0.0.0:7010".into()),
                    ..Remote::default()
                },
            })
        );
        assert_eq!(
            err(&["--topology", "--log-format", "json"]),
            "SETTINGS apply when running the hub; --topology and --health-check take --address and --tls-*"
        );
        assert_eq!(err(&["--bogus"]), "unknown argument: --bogus");
        assert_eq!(err(&["--topology", "extra"]), "unknown argument: extra");
        assert!(USAGE.contains("--topology [--json]") && USAGE.contains("--health-check"));
    }

    #[test]
    fn parse_the_client_certificate_options_in_pairs() {
        assert_eq!(
            args(&[
                "--health-check",
                "--tls-client-cert",
                "/c.pem",
                "--tls-client-key",
                "/k.pem"
            ]),
            Ok(Mode::HealthCheck(Remote {
                tls_client_cert: Some("/c.pem".into()),
                tls_client_key: Some("/k.pem".into()),
                ..Remote::default()
            }))
        );
        for one in [
            ["--tls-client-cert", "/c.pem"],
            ["--tls-client-key", "/k.pem"],
        ] {
            let mut a = vec!["--topology"];
            a.extend(one);
            assert_eq!(
                err(&a),
                "--tls-client-cert and --tls-client-key must be given together"
            );
        }
        assert_eq!(
            err(&["--topology", "--tls-client-cert"]),
            "--tls-client-cert needs a value"
        );
        assert!(USAGE.contains("--tls-client-cert") && USAGE.contains("--tls-client-key"));
    }

    #[test]
    fn parse_remote_options_and_acl_hash() {
        assert_eq!(
            args(&[
                "--topology",
                "--address",
                "https://hub:7000",
                "--tls-ca",
                "/ca.pem"
            ]),
            Ok(Mode::Topology {
                json: false,
                remote: Remote {
                    address: Some("https://hub:7000".into()),
                    tls_ca: Some("/ca.pem".into()),
                    ..Remote::default()
                },
            })
        );
        assert_eq!(
            args(&["--health-check", "--tls-ca", "/ca.pem"]),
            Ok(Mode::HealthCheck(Remote {
                tls_ca: Some("/ca.pem".into()),
                ..Remote::default()
            }))
        );
        assert_eq!(err(&["--topology", "--address"]), "--address needs a value");
        assert_eq!(
            err(&["--address", "hub:7000"]),
            "--address, --tls-ca and --tls-client-cert/--tls-client-key need --topology or --health-check"
        );
        assert_eq!(
            err(&["--tls-client-cert", "/c.pem", "--tls-client-key", "/k.pem"]),
            "--address, --tls-ca and --tls-client-cert/--tls-client-key need --topology or --health-check"
        );
        assert_eq!(
            args(&["acl", "hash", "abc"]),
            Ok(Mode::AclHash {
                token: Some("abc".into())
            })
        );
        assert_eq!(
            args(&["acl", "hash", "-"]),
            Ok(Mode::AclHash { token: None })
        );
        assert!(err(&["acl"]).contains("acl hash"));
        assert!(err(&["acl", "hash"]).contains("acl hash"));
        assert!(err(&["acl", "hash", "a", "b"]).contains("acl hash"));
    }

    #[test]
    fn fmt_duration_ms_cases() {
        for (ms, want) in [
            (-5, "0ms"),
            (0, "0ms"),
            (800, "800ms"),
            (999, "999ms"),
            (1000, "1s"),
            (45_000, "45s"),
            (60_000, "1m00s"),
            (185_000, "3m05s"),
            (3_599_000, "59m59s"),
            (3_720_000, "1h02m"),
            (86_399_000, "23h59m"),
            (183_600_000, "2d03h"),
        ] {
            assert_eq!(fmt_duration_ms(ms), want, "{ms}");
        }
    }

    fn instance(
        id: &str,
        age_ms: i64,
        hb_ms: i64,
        stale: bool,
        flight: (u32, u32),
        commands: &[&str],
        queries: &[&str],
    ) -> InstanceTopology {
        InstanceTopology {
            instance_id: id.into(),
            connected_since_ms: 200_000_000 - age_ms,
            last_heartbeat_age_ms: hb_ms,
            stale,
            in_flight_as_handler: flight.0,
            in_flight_as_sender: flight.1,
            command_types: commands.iter().map(|s| s.to_string()).collect(),
            query_types: queries.iter().map(|s| s.to_string()).collect(),
            client_version: "snodra-messaging/0.1.0".into(),
            permits_available: 0,
            queued: 0,
            load_factor: 100,
            tags: Default::default(),
            connected_hub_ids: vec![],
            status_age_ms: -1,
            shared_query_types: vec![],
            auth: None,
            authenticated_service: None,
            auth_kind: None,
            expires_at: None,
            cert_fingerprint: None,
            processors: vec![],
        }
    }

    fn fixture() -> Topology {
        Topology {
            hub_id: "hub-1".into(),
            hub_started_at_ms: 0,
            now_ms: 200_000_000,
            heartbeat_timeout_ms: 30_000,
            services: vec![
                ServiceTopology {
                    service_name: "billing".into(),
                    instances: vec![
                        instance(
                            "b-1",
                            3_905_000,
                            800,
                            false,
                            (0, 1),
                            &["Pay", "Refund"],
                            &["GetInvoice"],
                        ),
                        instance("b-2", 185_000, 45_000, true, (2, 0), &[], &[]),
                    ],
                },
                ServiceTopology {
                    service_name: "catalog".into(),
                    instances: vec![instance(
                        "c-1",
                        183_600_000,
                        1_000,
                        false,
                        (0, 0),
                        &[],
                        &["Find"],
                    )],
                },
            ],
            types: vec![
                TypeOwnership {
                    type_name: "Pay".into(),
                    kind: KindName::Command,
                    live_instances: 2,
                    orphaned: false,
                    orphaned_for_ms: 0,
                    shared: false,
                    owner_services: vec!["billing".into()],
                },
                TypeOwnership {
                    type_name: "Archive".into(),
                    kind: KindName::Command,
                    live_instances: 0,
                    orphaned: true,
                    orphaned_for_ms: 125_000,
                    shared: false,
                    owner_services: vec!["archive".into()],
                },
            ],
            totals: Totals {
                services: 2,
                instances: 3,
                command_types: 3,
                query_types: 2,
                queued: 0,
                in_flight: 3,
            },
        }
    }

    #[test]
    fn render_table_fixture_snapshot() {
        let want = "\
service billing (2 instances)
  instance | connected for | heartbeat age | stale | in-flight h/s | commands   | queries
  b-1      | 1h05m         | 800ms         | false | 0/1           | Pay,Refund | GetInvoice
  b-2      | 3m05s         | 45s           | true  | 2/0           | -          | -

service catalog (1 instances)
  instance | connected for | heartbeat age | stale | in-flight h/s | commands | queries
  c-1      | 2d03h         | 1s            | false | 0/0           | -        | Find

orphaned types:
  Archive COMMAND owner=archive for 2m05s

totals: services=2 instances=3 commandTypes=3 queryTypes=2 inFlight=3
";
        assert_eq!(render_table(&fixture()), want);
    }

    #[test]
    fn render_table_lists_the_processors_of_a_service_that_reports_them() {
        use crate::processors::{ProcessorReport, ProcessorStateName, SegmentReport};
        let parts = |list: &[(&str, i64)]| {
            list.iter()
                .map(|(s, lag)| SegmentReport {
                    segment: s.to_string(),
                    lag: *lag,
                    ..Default::default()
                })
                .collect::<Vec<_>>()
        };
        let mut t = fixture();
        t.services[0].instances[0].processors = vec![
            ProcessorReport {
                name: "default".into(),
                subscription: "billing".into(),
                source: "snodra.events".into(),
                state: ProcessorStateName::Running,
                segments: parts(&[("0", 2), ("1", -1), ("2", 10), ("5", 0)]),
                processed_total: 120,
                dead_letters: 3,
                dead_letter_mode: "collection".into(),
                ..Default::default()
            },
            ProcessorReport {
                name: "invoices".into(),
                subscription: "billing.invoices".into(),
                source: "snodra.events".into(),
                state: ProcessorStateName::Failing,
                segments: parts(&[("3", -1)]),
                processed_total: 7,
                failed_total: 2,
                last_error: format!("handler failed: {}", "x".repeat(80)),
                last_error_at_ms: 200_000_000 - 125_000,
                dead_letter_mode: "event-store".into(),
                ..Default::default()
            },
        ];
        t.services[0].instances[1].processors = vec![ProcessorReport {
            name: "default".into(),
            subscription: "billing".into(),
            source: "snodra.events".into(),
            state: ProcessorStateName::Starting,
            dead_letter_mode: "collection".into(),
            ..Default::default()
        }];
        let want = "\
service billing (2 instances)
  instance | connected for | heartbeat age | stale | in-flight h/s | commands   | queries
  b-1      | 1h05m         | 800ms         | false | 0/1           | Pay,Refund | GetInvoice
  b-2      | 3m05s         | 45s           | true  | 2/0           | -          | -
  processor | instance | state    | subscription     | source        | segments | lag | processed | failed | dead letters  | last error
  default   | b-1      | RUNNING  | billing          | snodra.events | 0-2,5    | 12  | 120       | 0      | 3             | -
  default   | b-2      | STARTING | billing          | snodra.events | -        | -   | 0         | 0      | 0             | -
  invoices  | b-1      | FAILING  | billing.invoices | snodra.events | 3        | -   | 7         | 2      | (event-store) | handler failed: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx... (2m05s ago)

service catalog (1 instances)
  instance | connected for | heartbeat age | stale | in-flight h/s | commands | queries
  c-1      | 2d03h         | 1s            | false | 0/0           | -        | Find

orphaned types:
  Archive COMMAND owner=archive for 2m05s

totals: services=2 instances=3 commandTypes=3 queryTypes=2 inFlight=3
";
        assert_eq!(render_table(&t), want);
    }

    #[test]
    fn the_state_cell_shows_a_replay_and_the_reset_holding_the_lock() {
        use crate::processors::{ProcessorReport, ProcessorStateName};
        let report = |name: &str, state, replaying, op: &str| ProcessorReport {
            name: name.into(),
            state,
            replaying,
            operation_id: op.into(),
            dead_letter_mode: "collection".into(),
            ..Default::default()
        };
        let mut t = fixture();
        t.services[0].instances[0].processors = vec![
            report("audit", ProcessorStateName::Paused, false, ""),
            report(
                "orders",
                ProcessorStateName::Stopped,
                false,
                "0f3c9a1e-7d2b-4c1a-9e8f-5a6b7c8d9e0f",
            ),
            report("default", ProcessorStateName::Running, true, ""),
        ];
        let table = render_table(&t);
        let row = |name: &str| {
            table
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("{name} ")))
                .unwrap_or_else(|| panic!("{name} missing in {table}"))
                .to_string()
        };
        assert!(row("audit").contains("| PAUSED "), "{table}");
        assert!(row("orders").contains("| STOPPED op 0f3c9a1e "), "{table}");
        assert!(row("default").contains("| RUNNING replaying "), "{table}");
    }

    /// A last error is free text from a library (a stack trace, an ANSI-coloured message): the
    /// table shows it on one line, control characters as spaces, runs of them as one, cut after
    /// [`LAST_ERROR_CHARS`] (H-3).
    #[test]
    fn a_multi_line_last_error_stays_on_its_row() {
        use crate::processors::{ProcessorReport, ProcessorStateName};
        let mut t = fixture();
        t.services[0].instances[0].processors = vec![ProcessorReport {
            name: "audit".into(),
            state: ProcessorStateName::Failing,
            last_error: format!(
                "boom:\r\n\tat Handler.on(Handler.kt:12)\n\u{1b}[31mred\u{7f}{}",
                "y".repeat(80)
            ),
            dead_letter_mode: "collection".into(),
            ..Default::default()
        }];
        let table = render_table(&t);
        let row = table
            .lines()
            .find(|l| l.trim_start().starts_with("audit "))
            .unwrap_or_else(|| panic!("no row in {table}"));
        let cell = row.rsplit(" | ").next().unwrap();
        assert_eq!(
            cell,
            "boom: at Handler.on(Handler.kt:12) [31mred yyyyyyyyyyyyyyyyy..."
        );
        assert!(!table.contains('\u{1b}') && !table.contains('\t') && !table.contains('\r'));
        assert_eq!(
            table.lines().count(),
            render_table(&fixture()).lines().count() + 2,
            "a header and one row: {table}"
        );
    }

    /// Every name in the table comes from a client (a service name, an instance id, a type, a
    /// processor's name, subscription and source, an operation id): none of them can break a line or send
    /// an escape to the terminal.
    #[test]
    fn client_text_in_any_cell_or_header_stays_on_its_line() {
        use crate::processors::{ProcessorReport, ProcessorStateName};
        let bad = |s: &str| format!("{s}\u{1b}[2J\r\nx");
        let mut t = fixture();
        t.services[0].service_name = bad("billing");
        let i = &mut t.services[0].instances[0];
        i.instance_id = bad("b-1");
        i.command_types = vec![bad("Pay")];
        i.query_types = vec![bad("GetInvoice")];
        i.processors = vec![ProcessorReport {
            name: bad("audit"),
            subscription: bad("billing.audit"),
            source: bad("snodra.events"),
            operation_id: bad("op"),
            state: ProcessorStateName::Running,
            dead_letter_mode: bad("log"),
            ..Default::default()
        }];
        t.types[1].type_name = bad("Archive");
        t.types[1].owner_services = vec![bad("archive")];
        let table = render_table(&t);
        assert!(
            !table.contains('\u{1b}') && !table.contains('\r'),
            "{table:?}"
        );
        assert_eq!(
            table.lines().count(),
            render_table(&fixture()).lines().count() + 2,
            "a processor header and one row: {table}"
        );
        assert!(
            table.starts_with("service billing [2J x (2 instances)\n"),
            "{table}"
        );
        assert!(table.contains("\n  b-1 [2J x "), "{table}");
        assert!(table.contains("| Pay [2J x "), "{table}");
        assert!(
            table.contains("\n  audit [2J x | b-1 [2J x | RUNNING op op [2J | billing.audit [2J x | snodra.events [2J x |"),
            "{table}"
        );
        assert!(table.contains("| (log [2J x)  |"), "{table}");
        assert!(
            table.contains("\n  Archive [2J x COMMAND owner=archive [2J x for 2m05s\n"),
            "{table}"
        );
    }

    #[test]
    fn segment_lists_collapse_decimal_runs() {
        assert_eq!(segment_ranges(&[]), "-");
        assert_eq!(segment_ranges(&["4"]), "4");
        assert_eq!(segment_ranges(&["0", "1", "2", "5", "7", "8"]), "0-2,5,7-8");
        assert_eq!(
            segment_ranges(&["0", "1", "01", "2", "events-a", "events-b"]),
            "0-1,01,2,events-a,events-b"
        );
    }

    #[test]
    fn render_table_empty_topology() {
        let t = Topology {
            hub_id: "hub-1".into(),
            hub_started_at_ms: 0,
            now_ms: 1,
            heartbeat_timeout_ms: 30_000,
            services: vec![],
            types: vec![],
            totals: Totals {
                services: 0,
                instances: 0,
                command_types: 0,
                query_types: 0,
                queued: 0,
                in_flight: 0,
            },
        };
        assert_eq!(
            render_table(&t),
            "no services connected\ntotals: services=0 instances=0 commandTypes=0 queryTypes=0 inFlight=0\n"
        );
    }
}
