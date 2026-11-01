//! Command-line modes of the binary: argument parsing and the `--topology` table renderer.

use std::fmt;

use crate::topology::{InstanceTopology, Topology};

pub const USAGE: &str = "usage:\n  kraken-hub                     run the hub\n  kraken-hub --health-check      probe the local hub's gRPC health service\n  kraken-hub --topology [--json] print the local hub's topology (table, or JSON with --json)";

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Mode {
    Serve,
    HealthCheck,
    Topology { json: bool },
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
    let (mut health, mut topology, mut json) = (false, false, false);
    for arg in args {
        match arg.as_str() {
            "--health-check" => health = true,
            "--topology" => topology = true,
            "--json" => json = true,
            _ => return Err(UsageError(format!("unknown argument: {arg}"))),
        }
    }
    if topology && health {
        return Err(UsageError(
            "--topology and --health-check are mutually exclusive".into(),
        ));
    }
    if json && !topology {
        return Err(UsageError("--json requires --topology".into()));
    }
    Ok(if topology {
        Mode::Topology { json }
    } else if health {
        Mode::HealthCheck
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
                ty.owner_service,
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
        assert_eq!(args(&["--health-check"]), Ok(Mode::HealthCheck));
        assert_eq!(args(&["--topology"]), Ok(Mode::Topology { json: false }));
        assert_eq!(
            args(&["--topology", "--json"]),
            Ok(Mode::Topology { json: true })
        );
        assert_eq!(
            args(&["--json", "--topology"]),
            Ok(Mode::Topology { json: true })
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
            protocol_version: 2,
            client_version: "kraken-messaging/0.1.0".into(),
            flow_controlled: false,
            permits_available: 0,
            queued: 0,
            load_factor: 100,
            tags: Default::default(),
            connected_hub_ids: vec![],
            status_age_ms: -1,
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
                    owner_service: "billing".into(),
                    live_instances: 2,
                    orphaned: false,
                    orphaned_for_ms: 0,
                },
                TypeOwnership {
                    type_name: "Archive".into(),
                    kind: KindName::Command,
                    owner_service: "legacy".into(),
                    live_instances: 0,
                    orphaned: true,
                    orphaned_for_ms: 125_000,
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
  Archive COMMAND owner=legacy for 2m05s

totals: services=2 instances=3 commandTypes=3 queryTypes=2 inFlight=3
";
        assert_eq!(render_table(&fixture()), want);
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
