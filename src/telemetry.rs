//! Telemetry: JSON logs, OTLP traces and metrics, and the router's instruments.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider, UpDownCounter};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::Config;
use crate::registry::{Kind, RegistrySnapshot};

const SERVICE_NAME: &str = "kraken-hub";

/// Owns the OTLP providers; dropping it flushes and shuts them down.
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(tracer) = self.tracer.take()
            && let Err(e) = tracer.shutdown()
        {
            tracing::debug!(error = %e, "tracer provider shutdown");
        }
        if let Some(meter) = self.meter.take()
            && let Err(e) = meter.shutdown()
        {
            tracing::debug!(error = %e, "meter provider shutdown");
        }
    }
}

/// Installs the JSON `tracing` subscriber filtered by `config.log`; with an OTLP endpoint it also
/// exports spans and metrics over OTLP gRPC. Must run inside a tokio runtime when exporting.
pub fn init(config: &Config) -> anyhow::Result<TelemetryGuard> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let (filter, bad_filter) = match EnvFilter::try_new(&config.log) {
        Ok(f) => (f, None),
        Err(e) => (EnvFilter::new("info"), Some(e)),
    };
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json());

    let guard = match &config.otlp_endpoint {
        None => {
            registry.init();
            TelemetryGuard {
                tracer: None,
                meter: None,
            }
        }
        Some(endpoint) => {
            let resource = resource(config);
            let span_exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.clone())
                .build()?;
            let tracer_provider = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_batch_exporter(span_exporter)
                .build();
            let metric_exporter = MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.clone())
                .build()?;
            let meter_provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_periodic_exporter(metric_exporter)
                .build();
            global::set_meter_provider(meter_provider.clone());
            registry
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(tracer_provider.tracer(SERVICE_NAME)),
                )
                .init();
            TelemetryGuard {
                tracer: Some(tracer_provider),
                meter: Some(meter_provider),
            }
        }
    };
    if let Some(e) = bad_filter {
        tracing::warn!(error = %e, directive = %config.log, "invalid log filter, using info");
    }
    Ok(guard)
}

/// The OTLP resource of every span and metric: `service.name` `kraken-hub` and
/// `service.instance.id` the hub id, which Prometheus turns into the `job` and `instance` labels
/// dashboards select hubs by (ADR 0026); no `service.namespace`, which would change `job`.
pub fn resource(config: &Config) -> Resource {
    Resource::builder()
        .with_service_name(SERVICE_NAME)
        .with_attribute(KeyValue::new("service.instance.id", config.hub_id.clone()))
        .build()
}

/// Makes `span` a child of the trace context carried in `headers` (`traceparent`, `tracestate`).
/// A missing or malformed header leaves the span a root span.
pub fn set_parent_from_headers(span: &tracing::Span, headers: &HashMap<String, String>) {
    struct Headers<'a>(&'a HashMap<String, String>);
    impl Extractor for Headers<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(String::as_str)
        }
        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(String::as_str).collect()
        }
    }
    if !headers.contains_key("traceparent") {
        return;
    }
    let parent =
        global::get_text_map_propagator(|p| TextMapPropagator::extract(p, &Headers(headers)));
    if let Err(e) = span.set_parent(parent) {
        tracing::debug!(error = %e, "could not set span parent");
    }
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
    /// The hub rejected the request for capacity (full queue or session buffer):
    /// RESOURCE_EXHAUSTED.
    ResourceExhausted,
    /// A streaming query ended before its handler did: the sender cancelled it, or it outlived
    /// `KRAKEN_HUB_MAX_STREAM_DURATION` (CANCELLED).
    Cancelled,
    /// The hub refused the request because the sender's token may not send its type (ADR 0018,
    /// PERMISSION_DENIED or UNAUTHENTICATED, mode `enforce` only).
    Denied,
}

impl Outcome {
    /// Label value for the `outcome` metric attribute and the `kraken.outcome` span field.
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::NoHandler => "no_handler",
            Outcome::Timeout => "timeout",
            Outcome::HandlerDisconnected => "handler_disconnected",
            Outcome::HandlerError => "handler_error",
            Outcome::InvalidRequest => "invalid_request",
            Outcome::ResourceExhausted => "resource_exhausted",
            Outcome::Cancelled => "cancelled",
            Outcome::Denied => "denied",
        }
    }
}

pub fn kind_label(kind: Kind) -> &'static str {
    match kind {
        Kind::Command => "command",
        Kind::Query => "query",
    }
}

/// Router metrics. The `connected_instances` and `registered_types` gauges are set imperatively
/// (not observable callbacks); services that disappeared are written back as 0 so the exported
/// series does not go stale at its last value.
pub struct Metrics {
    requests: Counter<u64>,
    duration: Histogram<f64>,
    in_flight: UpDownCounter<i64>,
    connected_instances: Gauge<u64>,
    registered_types: Gauge<u64>,
    buffer_full: Counter<u64>,
    queue_depth: Gauge<u64>,
    permits_available: Gauge<u64>,
    queue_wait: Histogram<f64>,
    rejected: Counter<u64>,
    scatter_targets: Histogram<u64>,
    streams_open: UpDownCounter<i64>,
    stream_parts: Counter<u64>,
    subscriptions_open: UpDownCounter<i64>,
    subscription_updates: Counter<u64>,
    auth_violations: Counter<u64>,
    acl_generation: Gauge<u64>,
    acl_reload_errors: Counter<u64>,
    tls_handshakes_shed: Counter<u64>,
    tls_handshake_failures: Counter<u64>,
    tls_client_ca_reload_errors: Counter<u64>,
    client_certificates: Counter<u64>,
    jwt_validations: Counter<u64>,
    jwks_fetches: Counter<u64>,
    jwks_keys: Gauge<u64>,
    credentials_refreshes: Counter<u64>,
    control: Counter<u64>,
    control_unknown_replies: Counter<u64>,
    event_browser_requests: Counter<u64>,
    event_browser_scanned: Histogram<u64>,
    event_tails: UpDownCounter<i64>,
    event_payload_views: Counter<u64>,
    seen: Mutex<Seen>,
}

#[derive(Default)]
struct Seen {
    instances: HashSet<String>,
    types: HashSet<String>,
    flows: HashSet<String>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Arc<Self> {
        Arc::new(Self {
            requests: meter
                .u64_counter("kraken_hub_requests_total")
                .with_description("Requests routed by the hub")
                .build(),
            duration: meter
                .f64_histogram("kraken_hub_request_duration_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0, 60.0,
                ])
                .with_description("Time from request receipt to terminal message")
                .build(),
            in_flight: meter
                .i64_up_down_counter("kraken_hub_in_flight")
                .with_description("Requests awaiting a reply")
                .build(),
            connected_instances: meter
                .u64_gauge("kraken_hub_connected_instances")
                .with_description("Instances with a stream to this hub, per service")
                .build(),
            registered_types: meter
                .u64_gauge("kraken_hub_registered_types")
                .with_description("Registered message types per service and kind")
                .build(),
            buffer_full: meter
                .u64_counter("kraken_hub_session_buffer_full_total")
                .with_description("Messages not delivered because a session buffer was full (busy handler, or a terminal message the sender did not read in time)")
                .build(),
            queue_depth: meter
                .u64_gauge("kraken_hub_queue_depth")
                .with_description("Requests queued for instances, per service")
                .build(),
            permits_available: meter
                .u64_gauge("kraken_hub_permits_available")
                .with_description("Permits instances have left, per service")
                .build(),
            queue_wait: meter
                .f64_histogram("kraken_hub_queue_wait_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0, 60.0,
                ])
                .with_description("Time a request waited in a queue before it was forwarded")
                .build(),
            rejected: meter
                .u64_counter("kraken_hub_rejected_total")
                .with_description("Requests rejected for capacity (queue_full, queue_bytes, queue_bytes_total, buffer_full)")
                .build(),
            scatter_targets: meter
                .u64_histogram("kraken_hub_scatter_targets")
                .with_boundaries(vec![
                    1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                ])
                .with_description("Targets of one scatter-gather query")
                .build(),
            streams_open: meter
                .i64_up_down_counter("kraken_hub_streams_open")
                .with_description("Streaming queries open, per handler service and instance")
                .build(),
            stream_parts: meter
                .u64_counter("kraken_hub_stream_parts_total")
                .with_description("Parts of streaming queries relayed, per handler service and type")
                .build(),
            subscriptions_open: meter
                .i64_up_down_counter("kraken_hub_subscriptions_open")
                .with_description("Subscription query targets open, per handler service and instance")
                .build(),
            subscription_updates: meter
                .u64_counter("kraken_hub_subscription_updates_total")
                .with_description("Updates of subscription queries relayed, per handler service and type")
                .build(),
            auth_violations: meter
                .u64_counter("kraken_hub_auth_violations_total")
                .with_description("Access-control violations (ADR 0018), refused in enforce mode and allowed in permissive mode, per reason and service (`unknown` for a service name the ACL does not hold)")
                .build(),
            acl_generation: meter
                .u64_gauge("kraken_hub_acl_generation")
                .with_description("Generation of the loaded ACL: 1 at startup, plus one per reload that changed it")
                .build(),
            acl_reload_errors: meter
                .u64_counter("kraken_hub_acl_reload_errors_total")
                .with_description("ACL reloads that failed to read or parse; the previous ACL stays in force")
                .build(),
            tls_handshakes_shed: meter
                .u64_counter("kraken_hub_tls_handshakes_shed_total")
                .with_description("TCP connections dropped before their TLS handshake because every handshake slot was busy (reason `slots`) or the peer IP already held its share (reason `per_ip`), per listener")
                .build(),
            tls_handshake_failures: meter
                .u64_counter("kraken_hub_tls_handshake_failures_total")
                .with_description("TLS handshakes that failed, per listener and reason: client_cert_missing (a required client certificate was not presented), client_cert_invalid (it did not verify against the client CA or was expired), timeout, other (plain text, scans, protocol errors)")
                .build(),
            tls_client_ca_reload_errors: meter
                .u64_counter("kraken_hub_tls_client_ca_reload_errors_total")
                .with_description("Client CA bundle reloads that failed to read or parse; the previous CAs stay in force")
                .build(),
            client_certificates: meter
                .u64_counter("kraken_hub_client_certificates_total")
                .with_description("Client certificates judged at Register (ADR 0020), per result: ok, no_service, multiple_services, mismatch (credentials name another service), expired (the session was closed or, in permissive, counted at notAfter)")
                .build(),
            jwt_validations: meter
                .u64_counter("kraken_hub_jwt_validations_total")
                .with_description("JWT credentials validated (ADR 0019), per result: ok, or why not (expired, not_yet_valid, bad_signature, bad_alg, unknown_kid, key_pending, wrong_issuer, wrong_audience, wrong_type, missing_claim, malformed, not_loaded)")
                .build(),
            jwks_fetches: meter
                .u64_counter("kraken_hub_jwks_fetches_total")
                .with_description("Signing-key set fetches, per result: ok, network, tls, timeout, status, too_large, invalid (a failed fetch keeps the last good key set)")
                .build(),
            jwks_keys: meter
                .u64_gauge("kraken_hub_jwks_keys")
                .with_description("Signing keys in the loaded key set; absent until the first successful fetch")
                .build(),
            credentials_refreshes: meter
                .u64_counter("kraken_hub_credentials_refreshes_total")
                .with_description("ClientMessage.credentials refreshes, per result: ok, refused, wrong_service, throttled, unavailable")
                .build(),
            control: meter
                .u64_counter("kraken_hub_control_total")
                .with_description("Processor control calls of the HTTP API (ADR 0023), per action (pause, resume, reset, dead_letter_list, dead_letter_retry, dead_letter_retry_all, dead_letter_discard) and outcome (ok, bad_request, not_found, conflict, failed, unavailable, timeout, unauthenticated, forbidden, disabled)")
                .build(),
            control_unknown_replies: meter
                .u64_counter("kraken_hub_control_unknown_replies_total")
                .with_description("ControlReply messages dropped because no request waits for them (unknown id, a reply after the timeout, or from another session than the one asked)")
                .build(),
            event_browser_requests: meter
                .u64_counter("kraken_hub_event_browser_requests_total")
                .with_description("Event browser calls (ADR 0025), per kind (search, record, tail, groups) and outcome (ok, bad_request, forbidden, not_found, too_many, unavailable, disabled)")
                .build(),
            event_browser_scanned: meter
                .u64_histogram("kraken_hub_event_browser_scanned_records")
                .with_boundaries(vec![
                    0.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 25000.0, 50000.0, 100000.0,
                ])
                .with_description("Records one event search read from Kafka")
                .build(),
            event_tails: meter
                .i64_up_down_counter("kraken_hub_event_tails")
                .with_description("Event tails reading Kafka now")
                .build(),
            event_payload_views: meter
                .u64_counter("kraken_hub_event_payload_views_total")
                .with_description("Event payloads served by the record detail (each also writes an audit line)")
                .build(),
            seen: Mutex::new(Seen::default()),
        })
    }

    /// A violation of the access-control rules (refused or, in permissive mode, allowed).
    pub fn auth_violation(&self, reason: &'static str, service: &str) {
        self.auth_violations.add(
            1,
            &[
                KeyValue::new("reason", reason),
                KeyValue::new("service", service.to_string()),
            ],
        );
    }

    /// One processor control call of the HTTP API (ADR 0023).
    pub fn control(&self, action: &'static str, outcome: &'static str) {
        self.control.add(
            1,
            &[
                KeyValue::new("action", action),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    /// One event browser call (ADR 0025); `kind` and `outcome` are from fixed sets.
    pub fn event_request(&self, kind: &'static str, outcome: &'static str) {
        self.event_browser_requests.add(
            1,
            &[
                KeyValue::new("kind", kind),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    /// The records one search read.
    pub fn event_scanned(&self, records: u64) {
        self.event_browser_scanned.record(records, &[]);
    }

    /// A tail started (`1`) or ended (`-1`).
    pub fn event_tails_add(&self, delta: i64) {
        self.event_tails.add(delta, &[]);
    }

    /// One payload served.
    pub fn event_payload_view(&self) {
        self.event_payload_views.add(1, &[]);
    }

    /// A `ControlReply` nobody waited for, dropped.
    pub fn control_unknown_reply(&self) {
        self.control_unknown_replies.add(1, &[]);
    }

    pub fn acl_generation_set(&self, generation: u64) {
        self.acl_generation.record(generation, &[]);
    }

    /// A connection dropped unhandshaken because the handshake limits were reached.
    pub fn tls_handshake_shed(&self, listener: &'static str, reason: &'static str) {
        self.tls_handshakes_shed.add(
            1,
            &[
                KeyValue::new("listener", listener),
                KeyValue::new("reason", reason),
            ],
        );
    }

    /// A TLS handshake that failed; `reason` is one of a fixed set, never text a peer chose.
    pub fn tls_handshake_failure(&self, listener: &'static str, reason: &'static str) {
        self.tls_handshake_failures.add(
            1,
            &[
                KeyValue::new("listener", listener),
                KeyValue::new("reason", reason),
            ],
        );
    }

    /// A client CA bundle reload that failed.
    pub fn client_ca_reload_failed(&self) {
        self.tls_client_ca_reload_errors.add(1, &[]);
    }

    /// A client certificate judged (ADR 0020); `result` is one of a fixed set.
    pub fn client_certificate(&self, result: &'static str) {
        self.client_certificates
            .add(1, &[KeyValue::new("result", result)]);
    }

    /// One JWT validated (or refused): `result` is one of a fixed set of labels, never text a
    /// client chose.
    pub fn jwt_validation(&self, result: &'static str) {
        self.jwt_validations
            .add(1, &[KeyValue::new("result", result)]);
    }

    /// One signing-key set fetch finished.
    pub fn jwks_fetch(&self, result: &'static str) {
        self.jwks_fetches.add(1, &[KeyValue::new("result", result)]);
    }

    pub fn jwks_keys_set(&self, keys: u64) {
        self.jwks_keys.record(keys, &[]);
    }

    /// One `ClientMessage.credentials` handled.
    pub fn credentials_refresh(&self, result: &'static str) {
        self.credentials_refreshes
            .add(1, &[KeyValue::new("result", result)]);
    }

    pub fn acl_reload_failed(&self) {
        self.acl_reload_errors.add(1, &[]);
    }

    /// Queue depth and permits left per service (its instances summed); services
    /// that had flows before and have none now are written back as 0.
    pub fn flows_set(&self, by_service: HashMap<String, (u64, u64)>) {
        let mut seen = self.seen.lock().unwrap();
        for gone in seen.flows.iter().filter(|s| !by_service.contains_key(*s)) {
            let attrs = [KeyValue::new("service", gone.clone())];
            self.queue_depth.record(0, &attrs);
            self.permits_available.record(0, &attrs);
        }
        seen.flows = by_service.keys().cloned().collect();
        drop(seen);
        for (service, (depth, permits)) in by_service {
            let attrs = [KeyValue::new("service", service)];
            self.queue_depth.record(depth, &attrs);
            self.permits_available.record(permits, &attrs);
        }
    }

    pub fn queue_waited(&self, service: &str, type_name: &str, waited: Duration) {
        self.queue_wait.record(
            waited.as_secs_f64(),
            &[
                KeyValue::new("service", service.to_string()),
                KeyValue::new("type", type_name.to_string()),
            ],
        );
    }

    pub fn rejected(&self, service: &str, reason: &'static str) {
        self.rejected.add(
            1,
            &[
                KeyValue::new("service", service.to_string()),
                KeyValue::new("reason", reason),
            ],
        );
    }

    /// Records nothing: the provider has no reader. For tests that do not look at metrics.
    pub fn noop() -> Arc<Self> {
        Self::new(SdkMeterProvider::builder().build().meter(SERVICE_NAME))
    }

    pub fn request_started(&self, kind: Kind) {
        self.in_flight_add(kind, 1);
    }

    pub fn in_flight_add(&self, kind: Kind, delta: i64) {
        self.in_flight
            .add(delta, &[KeyValue::new("kind", kind_label(kind))]);
    }

    /// A unary request ended.
    pub fn request_finished(
        &self,
        kind: Kind,
        type_name: &str,
        outcome: Outcome,
        duration: Duration,
    ) {
        self.request_finished_mode(kind, type_name, outcome, duration, "unary");
    }

    /// A request of `mode` (`unary`, `scatter_gather`, ...) ended. A scatter-gather counts once,
    /// whatever its number of targets.
    pub fn request_finished_mode(
        &self,
        kind: Kind,
        type_name: &str,
        outcome: Outcome,
        duration: Duration,
        mode: &'static str,
    ) {
        self.in_flight_add(kind, -1);
        // The duration keeps the mode (a gather's latency is to its ReplyEnd, not comparable
        // with a unary request of the same type) but never the outcome.
        let attrs = [
            KeyValue::new("kind", kind_label(kind)),
            KeyValue::new("type", type_name.to_string()),
            KeyValue::new("mode", mode),
            KeyValue::new("outcome", outcome.as_str()),
        ];
        self.requests.add(1, &attrs);
        self.duration.record(duration.as_secs_f64(), &attrs[..3]);
    }

    /// The number of targets one scatter-gather query went to.
    pub fn scatter_targets(&self, type_name: &str, targets: usize) {
        self.scatter_targets.record(
            targets as u64,
            &[KeyValue::new("type", type_name.to_string())],
        );
    }

    /// A streaming query was routed to `instance` of `service`.
    pub fn stream_opened(&self, service: &str, instance: &str) {
        self.streams_open.add(1, &stream_attrs(service, instance));
    }

    /// A streaming query served by `instance` of `service` ended, however.
    pub fn stream_closed(&self, service: &str, instance: &str) {
        self.streams_open.add(-1, &stream_attrs(service, instance));
    }

    /// A part of a streaming query of `type_name`, served by `service`, was relayed.
    pub fn stream_part(&self, service: &str, type_name: &str) {
        self.stream_parts.add(
            1,
            &[
                KeyValue::new("service", service.to_string()),
                KeyValue::new("type", type_name.to_string()),
            ],
        );
    }

    /// A subscription query (ADR 0017, P4c) got `instance` of `service` as a target.
    pub fn subscription_opened(&self, service: &str, instance: &str) {
        self.subscriptions_open
            .add(1, &stream_attrs(service, instance));
    }

    /// A subscription target served by `instance` of `service` ended, however.
    pub fn subscription_closed(&self, service: &str, instance: &str) {
        self.subscriptions_open
            .add(-1, &stream_attrs(service, instance));
    }

    /// An update of a subscription query of `type_name`, pushed by `service`, was relayed.
    pub fn subscription_update(&self, service: &str, type_name: &str) {
        self.subscription_updates.add(
            1,
            &[
                KeyValue::new("service", service.to_string()),
                KeyValue::new("type", type_name.to_string()),
            ],
        );
    }

    pub fn buffer_full(&self, service: &str) {
        self.buffer_full
            .add(1, &[KeyValue::new("service", service.to_string())]);
    }

    /// Instances connected per service (one session each); a service whose last instance left
    /// is written back as 0.
    pub fn connected_instances_set(&self, by_service: HashMap<String, usize>) {
        let mut seen = self.seen.lock().unwrap();
        for gone in seen
            .instances
            .iter()
            .filter(|s| !by_service.contains_key(*s))
        {
            self.connected_instances
                .record(0, &[KeyValue::new("service", gone.clone())]);
        }
        seen.instances = by_service.keys().cloned().collect();
        drop(seen);
        for (service, n) in by_service {
            self.connected_instances
                .record(n as u64, &[KeyValue::new("service", service)]);
        }
    }

    pub fn registered_types_set(&self, snapshot: &RegistrySnapshot) {
        let mut seen = self.seen.lock().unwrap();
        let current: HashSet<String> = snapshot
            .services
            .iter()
            .map(|s| s.service.clone())
            .collect();
        for gone in seen.types.difference(&current) {
            for kind in [Kind::Command, Kind::Query] {
                self.registered_types.record(
                    0,
                    &[
                        KeyValue::new("service", gone.clone()),
                        KeyValue::new("kind", kind_label(kind)),
                    ],
                );
            }
        }
        seen.types = current;
        drop(seen);
        for s in &snapshot.services {
            for (kind, n) in [
                (Kind::Command, s.command_types),
                (Kind::Query, s.query_types),
            ] {
                self.registered_types.record(
                    n as u64,
                    &[
                        KeyValue::new("service", s.service.clone()),
                        KeyValue::new("kind", kind_label(kind)),
                    ],
                );
            }
        }
    }
}

/// The handler's service and instance. The instance is `instance_id`: Prometheus overwrites a
/// data-point `instance` (and `job`) with the resource's (ADR 0026).
fn stream_attrs(service: &str, instance: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("instance_id", instance.to_string()),
    ]
}

/// In-memory metrics for unit tests elsewhere in the crate.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    /// One log event captured by [`capture_logs`].
    #[derive(Debug, Clone, Default)]
    pub(crate) struct LogLine {
        pub message: String,
        pub fields: Vec<(String, String)>,
    }

    impl LogLine {
        pub(crate) fn field(&self, name: &str) -> Option<&str> {
            self.fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        }
    }

    thread_local! {
        /// Every event logged on this thread since [`capture_logs`] (a test and its current-thread
        /// runtime share one thread).
        static LINES: std::cell::RefCell<Vec<LogLine>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    impl tracing::field::Visit for LogLine {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            match field.name() {
                "message" => self.message = value.to_string(),
                name => self.fields.push((name.to_string(), value.to_string())),
            }
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let text = format!("{value:?}");
            match field.name() {
                "message" => self.message = text,
                name => self.fields.push((name.to_string(), text)),
            }
        }
    }

    struct Capture;

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut line = LogLine::default();
            event.record(&mut line);
            LINES.with_borrow_mut(|lines| lines.push(line));
        }
    }

    /// Installs a recorder as the global subscriber (once per test binary: a scoped one is racy,
    /// as any thread's first hit of a callsite rebuilds the global interest cache and the max
    /// level against the global default) and clears this thread's lines.
    pub(crate) fn capture_logs() {
        use tracing_subscriber::layer::SubscriberExt;
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::registry().with(Capture);
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        LINES.with_borrow_mut(Vec::clear);
    }

    /// The events logged on this thread since [`capture_logs`].
    pub(crate) fn logged() -> Vec<LogLine> {
        LINES.with_borrow(Vec::clone)
    }

    pub(crate) struct TestMetrics {
        pub metrics: Arc<Metrics>,
        exporter: InMemoryMetricExporter,
        provider: SdkMeterProvider,
    }

    pub(crate) fn test_metrics() -> TestMetrics {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        TestMetrics {
            metrics: Metrics::new(provider.meter("test")),
            exporter,
            provider,
        }
    }

    impl TestMetrics {
        /// The sum of the u64 counter `name` over the points whose attribute `key` is `value`.
        pub(crate) fn counter(&self, name: &str, key: &str, value: &str) -> Option<u64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                panic!("{name} is not a u64 counter");
            };
            let points: Vec<u64> = sum
                .data_points()
                .filter(|p| {
                    p.attributes()
                        .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                })
                .map(|p| p.value())
                .collect();
            (!points.is_empty()).then(|| points.iter().sum())
        }

        /// The sum of the u64 counter `name` over all its points.
        pub(crate) fn counter_total(&self, name: &str) -> Option<u64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                panic!("{name} is not a u64 counter");
            };
            Some(sum.data_points().map(|p| p.value()).sum())
        }

        /// The last value of the u64 gauge `name` that has no attributes.
        pub(crate) fn gauge_plain(&self, name: &str) -> Option<u64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::U64(MetricData::Gauge(g)) = metric.data() else {
                panic!("{name} is not a u64 gauge");
            };
            g.data_points().next().map(|p| p.value())
        }

        /// The sum of the i64 up-down counter `name` over the points whose attribute `key` is
        /// `value`.
        pub(crate) fn up_down(&self, name: &str, key: &str, value: &str) -> Option<i64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
                panic!("{name} is not an i64 up-down counter");
            };
            let points: Vec<i64> = sum
                .data_points()
                .filter(|p| {
                    p.attributes()
                        .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                })
                .map(|p| p.value())
                .collect();
            (!points.is_empty()).then(|| points.iter().sum())
        }

        /// The sum of the i64 up-down counter `name` over all its points.
        pub(crate) fn up_down_total(&self, name: &str) -> Option<i64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
                panic!("{name} is not an i64 up-down counter");
            };
            Some(sum.data_points().map(|p| p.value()).sum())
        }

        /// `(count, sum)` of the histogram `name` over all its points, as exported now.
        pub(crate) fn histogram_sum(&self, name: &str) -> Option<(u64, f64)> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Histogram(h)) => Some(
                    h.data_points()
                        .fold((0, 0.0), |(c, s), p| (c + p.count(), s + p.sum() as f64)),
                ),
                AggregatedMetrics::F64(MetricData::Histogram(h)) => Some(
                    h.data_points()
                        .fold((0, 0.0), |(c, s), p| (c + p.count(), s + p.sum())),
                ),
                _ => panic!("{name} is not a histogram"),
            }
        }

        /// The last value of the u64 gauge `name` for `service`, as exported now.
        pub(crate) fn gauge(&self, name: &str, service: &str) -> Option<u64> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let metric = finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)?;
            let AggregatedMetrics::U64(MetricData::Gauge(g)) = metric.data() else {
                panic!("{name} is not a u64 gauge");
            };
            g.data_points()
                .find(|p| {
                    p.attributes()
                        .any(|kv| kv.key.as_str() == "service" && kv.value.as_str() == service)
                })
                .map(|p| p.value())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    #[test]
    fn request_finished_records_counter_with_outcome_label() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = Metrics::new(provider.meter("test"));

        metrics.request_finished(Kind::Command, "a.B", Outcome::Ok, Duration::from_millis(10));
        provider.force_flush().unwrap();

        let finished = exporter.get_finished_metrics().unwrap();
        let metric = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "kraken_hub_requests_total")
            .expect("counter exported");
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
            panic!("expected u64 sum");
        };
        let point = sum.data_points().next().expect("one data point");
        let attrs: Vec<(String, String)> = point
            .attributes()
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect();
        assert!(
            attrs.contains(&("kind".into(), "command".into())),
            "{attrs:?}"
        );
        assert!(attrs.contains(&("type".into(), "a.B".into())), "{attrs:?}");
        assert!(
            attrs.contains(&("outcome".into(), "ok".into())),
            "{attrs:?}"
        );
        assert_eq!(point.value(), 1);
    }

    /// The subscription instruments (ADR 0017, P4c): a gauge per handler instance and a counter
    /// per handler service and type.
    #[test]
    fn subscription_instruments_count_open_targets_and_relayed_updates() {
        let m = test_support::test_metrics();
        m.metrics.subscription_opened("A", "a1");
        m.metrics.subscription_opened("A", "a1");
        m.metrics.subscription_opened("B", "b1");
        m.metrics.subscription_closed("A", "a1");
        m.metrics.subscription_update("A", "q.Watch");
        m.metrics.subscription_update("A", "q.Watch");
        m.metrics.subscription_update("B", "q.Watch");
        assert_eq!(
            m.up_down("kraken_hub_subscriptions_open", "instance_id", "a1"),
            Some(1)
        );
        assert_eq!(
            m.up_down("kraken_hub_subscriptions_open", "instance_id", "b1"),
            Some(1)
        );
        assert_eq!(
            m.counter("kraken_hub_subscription_updates_total", "service", "A"),
            Some(2)
        );
        assert_eq!(
            m.counter("kraken_hub_subscription_updates_total", "type", "q.Watch"),
            Some(3)
        );
    }

    fn exporter_and_metrics() -> (InMemoryMetricExporter, SdkMeterProvider, Arc<Metrics>) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = Metrics::new(provider.meter("test"));
        (exporter, provider, metrics)
    }

    /// The duration histogram has the `mode` label, so a gather's latency (to its `ReplyEnd`)
    /// does not mix with the unary latency of the same type; never the outcome.
    #[test]
    fn duration_histogram_has_kind_type_and_mode_labels_only() {
        let (exporter, provider, metrics) = exporter_and_metrics();
        metrics.request_finished(Kind::Command, "a.B", Outcome::Ok, Duration::from_millis(10));
        metrics.request_finished_mode(
            Kind::Query,
            "q.F",
            Outcome::Timeout,
            Duration::from_millis(20),
            "scatter_gather",
        );
        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();
        let metric = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "kraken_hub_request_duration_seconds")
            .expect("histogram exported");
        let AggregatedMetrics::F64(MetricData::Histogram(h)) = metric.data() else {
            panic!("expected f64 histogram");
        };
        let mut points: Vec<(Vec<(String, String)>, u64)> = h
            .data_points()
            .map(|point| {
                let mut attrs: Vec<(String, String)> = point
                    .attributes()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect();
                attrs.sort();
                (attrs, point.count())
            })
            .collect();
        points.sort();
        let labels = |kind: &str, type_name: &str, mode: &str| {
            vec![
                ("kind".to_string(), kind.to_string()),
                ("mode".to_string(), mode.to_string()),
                ("type".to_string(), type_name.to_string()),
            ]
        };
        assert_eq!(
            points,
            [
                (labels("command", "a.B", "unary"), 1),
                (labels("query", "q.F", "scatter_gather"), 1)
            ]
        );
        let point = h.data_points().next().expect("one data point");
        assert_eq!(point.bounds().next(), Some(0.001));
    }

    #[test]
    fn in_flight_returns_to_zero_after_finish() {
        let (exporter, provider, metrics) = exporter_and_metrics();
        metrics.request_started(Kind::Query);
        metrics.request_finished(Kind::Query, "q.X", Outcome::Ok, Duration::ZERO);
        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();
        let metric = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "kraken_hub_in_flight")
            .expect("up-down counter exported");
        let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
            panic!("expected i64 sum");
        };
        let total: i64 = sum.data_points().map(|p| p.value()).sum();
        assert_eq!(total, 0);
    }

    /// Dashboards select hubs by the resource (ADR 0026 hub ruling 1): `service.name` becomes
    /// `job` and `service.instance.id` becomes `instance` in Prometheus.
    #[test]
    fn the_resource_names_the_hub_and_its_id() {
        let config = Config {
            hub_id: "hub-7".into(),
            ..Config::default()
        };
        let resource = resource(&config);
        let get = |key: &'static str| {
            resource
                .get(&opentelemetry::Key::from_static_str(key))
                .map(|v| v.to_string())
        };
        assert_eq!(get("service.name").as_deref(), Some("kraken-hub"));
        assert_eq!(get("service.instance.id").as_deref(), Some("hub-7"));
        assert_eq!(get("service.namespace"), None, "it would change `job`");
    }

    /// One gauge per service; a service whose last instance left is written back as 0.
    #[test]
    fn connected_instances_are_counted_per_service() {
        let m = test_support::test_metrics();
        m.metrics
            .connected_instances_set(HashMap::from([("A".to_string(), 2), ("B".to_string(), 1)]));
        assert_eq!(m.gauge("kraken_hub_connected_instances", "A"), Some(2));
        assert_eq!(m.gauge("kraken_hub_connected_instances", "B"), Some(1));
        m.metrics
            .connected_instances_set(HashMap::from([("A".to_string(), 1)]));
        assert_eq!(m.gauge("kraken_hub_connected_instances", "A"), Some(1));
        assert_eq!(m.gauge("kraken_hub_connected_instances", "B"), Some(0));
    }

    /// A data-point attribute named `instance` (or `job`) is overwritten by the resource's in
    /// Prometheus, so the handler instance is `instance_id`.
    #[test]
    fn open_streams_and_subscriptions_name_the_handler_instance_instance_id() {
        let m = test_support::test_metrics();
        m.metrics.stream_opened("A", "a1");
        m.metrics.subscription_opened("A", "a2");
        assert_eq!(
            m.up_down("kraken_hub_streams_open", "instance_id", "a1"),
            Some(1)
        );
        assert_eq!(
            m.up_down("kraken_hub_subscriptions_open", "instance_id", "a2"),
            Some(1)
        );
        assert_eq!(m.up_down("kraken_hub_streams_open", "instance", "a1"), None);
    }

    #[test]
    fn outcome_labels_are_exact() {
        let labels: Vec<&str> = [
            Outcome::Ok,
            Outcome::NoHandler,
            Outcome::Timeout,
            Outcome::HandlerDisconnected,
            Outcome::HandlerError,
            Outcome::InvalidRequest,
        ]
        .iter()
        .map(|o| o.as_str())
        .collect();
        assert_eq!(
            labels,
            [
                "ok",
                "no_handler",
                "timeout",
                "handler_disconnected",
                "handler_error",
                "invalid_request"
            ]
        );
    }
}
