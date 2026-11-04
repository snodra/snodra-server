//! Command-line modes of the binary: argument parsing and the `--topology` table renderer (with
//! each service's event processors, ADR 0022).

use std::fmt;
use std::path::PathBuf;

use crate::topology::{InstanceTopology, Topology};

pub const USAGE: &str = "usage:\n  kraken-hub                     run the hub\n  kraken-hub --health-check [REMOTE]      probe the hub's gRPC health service\n  kraken-hub --topology [--json] [REMOTE] print the hub's topology (table, or JSON with --json)\n  kraken-hub acl hash <token|->  print the SHA-256 (hex) of a token for the ACL file; `-` reads it from stdin\n\nREMOTE (default: the local hub, TLS when the environment configures it):\n  --address <host:port|https://host:port>  the hub to probe\n  --tls-ca <pem>                          trust this CA (turns TLS on); https:// alone uses the system roots\n  --tls-client-cert <pem> --tls-client-key <pem>  present this client certificate (a hub with KRAKEN_HUB_TLS_CLIENT_AUTH=required)\n\nA hub with KRAKEN_HUB_DASHBOARD_TOKEN needs the same variable in the probe's environment for --topology.";

/// Which hub a probe talks to; see [`crate::client::Target::resolve`].
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Remote {
    /// `--address`: `host:port`, `http://host:port` or `https://host:port`. `None`: the local hub.
    pub address: Option<String>,
    /// `--tls-ca`: PEM file with the CA(s) to trust; turns TLS on.
    pub tls_ca: Option<PathBuf>,
    /// `--tls-client-cert` and `--tls-client-key`: the client certificate and key (PEM) to
    /// present, for a hub that asks for one (ADR 0020). Both or neither.
    pub tls_client_cert: Option<PathBuf>,
    pub tls_client_key: Option<PathBuf>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Mode {
    Serve,
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
            _ => Err(UsageError("usage: kraken-hub acl hash <token|->".into())),
        };
    }
    let (mut health, mut topology, mut json) = (false, false, false);
    let mut remote = Remote::default();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let value = |name: &str, it: &mut dyn Iterator<Item = String>| {
            it.next()
                .ok_or_else(|| UsageError(format!("{name} needs a value")))
        };
        match arg.as_str() {
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
    Ok(if topology {
        Mode::Topology { json, remote }
    } else if health {
        Mode::HealthCheck(remote)
    } else {
        Mode::Serve
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

/// Fixed-width rows (first row is the header), indented, trailing spaces trimmed.
fn table(rows: &[Vec<String>]) -> String {
    let cols = rows[0].len();
    let widths: Vec<usize> = (0..cols)
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in rows {
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

/// Partition numbers (sorted) with runs collapsed: `0-2,5,7-8`; `-` when none.
fn partition_ranges(partitions: &[i32]) -> String {
    let mut runs: Vec<(i32, i32)> = Vec::new();
    for &p in partitions {
        match runs.last_mut() {
            Some((_, end)) if p == end.saturating_add(1) => *end = p,
            _ => runs.push((p, p)),
        }
    }
    if runs.is_empty() {
        return "-".into();
    }
    runs.iter()
        .map(|(a, b)| {
            if a == b {
                a.to_string()
            } else {
                format!("{a}-{b}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Longest last error the table shows, in characters; longer ones end with `...`.
const LAST_ERROR_CHARS: usize = 60;

/// The state of a processor as the table shows it: the state, then `replaying` while a replay
/// range is open and `op <first 8>` of the reset holding its lock (ADR 0023).
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

/// One row per (processor, instance) of a service whose instances report processors (ADR
/// 0022), sorted by processor name, then instance id.
fn processor_rows(instances: &[InstanceTopology], now_ms: i64) -> Vec<Vec<String>> {
    let mut rows: Vec<(String, String, Vec<String>)> = Vec::new();
    for i in instances {
        for p in &i.processors {
            let known: Vec<i64> = p
                .partitions
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
            let last_error = if p.last_error.is_empty() {
                "-".to_string()
            } else {
                let mut text: String = p.last_error.chars().take(LAST_ERROR_CHARS).collect();
                if p.last_error.chars().count() > LAST_ERROR_CHARS {
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
            let partitions: Vec<i32> = p.partitions.iter().map(|x| x.partition).collect();
            rows.push((
                p.name.clone(),
                i.instance_id.clone(),
                vec![
                    p.name.clone(),
                    i.instance_id.clone(),
                    state_cell(p),
                    p.group_id.clone(),
                    p.topic.clone(),
                    partition_ranges(&partitions),
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
            service.service_name,
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
                    "group",
                    "topic",
                    "partitions",
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
                ty.type_name,
                kind,
                // Every owner of a shared query type (ADR 0017), comma-separated.
                ty.owner_services.join(","),
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
        assert_eq!(args(&[]), Ok(Mode::Serve));
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
            client_version: "kraken-messaging/0.1.0".into(),
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
        use crate::processors::{PartitionReport, ProcessorReport, ProcessorStateName};
        let parts = |list: &[(i32, i64)]| {
            list.iter()
                .map(|(p, lag)| PartitionReport {
                    partition: *p,
                    position: 0,
                    committed: 0,
                    high_watermark: 0,
                    lag: *lag,
                    replay_until: 0,
                })
                .collect::<Vec<_>>()
        };
        let mut t = fixture();
        t.services[0].instances[0].processors = vec![
            ProcessorReport {
                name: "default".into(),
                group_id: "billing".into(),
                topic: "kraken.events".into(),
                state: ProcessorStateName::Running,
                partitions: parts(&[(0, 2), (1, -1), (2, 10), (5, 0)]),
                processed_total: 120,
                dead_letters: 3,
                dead_letter_mode: "collection".into(),
                ..Default::default()
            },
            ProcessorReport {
                name: "invoices".into(),
                group_id: "billing.invoices".into(),
                topic: "kraken.events".into(),
                state: ProcessorStateName::Failing,
                partitions: parts(&[(3, -1)]),
                processed_total: 7,
                failed_total: 2,
                last_error: format!("handler failed: {}", "x".repeat(80)),
                last_error_at_ms: 200_000_000 - 125_000,
                dead_letter_mode: "topic".into(),
                ..Default::default()
            },
        ];
        t.services[0].instances[1].processors = vec![ProcessorReport {
            name: "default".into(),
            group_id: "billing".into(),
            topic: "kraken.events".into(),
            state: ProcessorStateName::Starting,
            dead_letter_mode: "collection".into(),
            ..Default::default()
        }];
        let want = "\
service billing (2 instances)
  instance | connected for | heartbeat age | stale | in-flight h/s | commands   | queries
  b-1      | 1h05m         | 800ms         | false | 0/1           | Pay,Refund | GetInvoice
  b-2      | 3m05s         | 45s           | true  | 2/0           | -          | -
  processor | instance | state    | group            | topic         | partitions | lag | processed | failed | dead letters | last error
  default   | b-1      | RUNNING  | billing          | kraken.events | 0-2,5      | 12  | 120       | 0      | 3            | -
  default   | b-2      | STARTING | billing          | kraken.events | -          | -   | 0         | 0      | 0            | -
  invoices  | b-1      | FAILING  | billing.invoices | kraken.events | 3          | -   | 7         | 2      | (topic)      | handler failed: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx... (2m05s ago)

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

    #[test]
    fn partition_lists_collapse_runs() {
        assert_eq!(partition_ranges(&[]), "-");
        assert_eq!(partition_ranges(&[4]), "4");
        assert_eq!(partition_ranges(&[0, 1, 2, 5, 7, 8]), "0-2,5,7-8");
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
