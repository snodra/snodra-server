//! Telemetry: JSON logs, OTLP traces and metrics, and the router's instruments.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{
    AsyncInstrument, Counter, Gauge, Histogram, Meter, MeterProvider, ObservableGauge,
    UpDownCounter,
};
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
use crate::processors::{self, ProcessorStateName};
use crate::registry::{Kind, RegistrySnapshot};
use crate::session::Sessions;

const SERVICE_NAME: &str = "snodra-server";

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

/// The OTLP resource of every span and metric: `service.name` `snodra-server` and
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
    /// `SNODRA_SERVER_MAX_STREAM_DURATION` (CANCELLED).
    Cancelled,
    /// The hub refused the request because the sender's token may not send its type (ADR 0018,
    /// PERMISSION_DENIED or UNAUTHENTICATED, mode `enforce` only).
    Denied,
}

impl Outcome {
    /// Label value for the `outcome` metric attribute and the `snodra.outcome` span field.
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

/// The `type` of a request metric whose type has no owner (ADR 0026): what a sender names is
/// not bounded, the registered types are.
pub const OTHER_TYPE: &str = "other";

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
    streams_open: Arc<OpenCounts>,
    stream_parts: Counter<u64>,
    subscriptions_open: Arc<OpenCounts>,
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
    _open_gauges: [ObservableGauge<i64>; 2],
}

#[derive(Default)]
struct Seen {
    instances: HashSet<String>,
    types: HashSet<String>,
    flows: HashSet<String>,
    queued: HashSet<(String, String)>,
}

/// The flow gauges of one pass over every instance: permits left per service, requests queued
/// per (service, type).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlowGauges {
    pub permits: HashMap<String, u64>,
    pub queued: HashMap<(String, String), u64>,
}

fn queue_attrs(service: &str, type_name: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("type", type_name.to_string()),
    ]
}

impl Metrics {
    pub fn new(meter: Meter) -> Arc<Self> {
        let streams_open = Arc::new(OpenCounts::default());
        let subscriptions_open = Arc::new(OpenCounts::default());
        let open_gauges = [
            OpenCounts::observe(
                &meter,
                "snodra_hub_streams_open",
                "Streaming queries open, per handler service and instance (an instance whose last one closed reads 0 once, then is not exported)",
                Arc::clone(&streams_open),
            ),
            OpenCounts::observe(
                &meter,
                "snodra_hub_subscriptions_open",
                "Subscription query targets open, per handler service and instance (an instance whose last one closed reads 0 once, then is not exported)",
                Arc::clone(&subscriptions_open),
            ),
        ];
        Arc::new(Self {
            requests: meter
                .u64_counter("snodra_hub_requests_total")
                .with_description("Requests routed by the hub")
                .build(),
            duration: meter
                .f64_histogram("snodra_hub_request_duration_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0, 60.0,
                ])
                .with_description("Time from request receipt to terminal message")
                .build(),
            in_flight: meter
                .i64_up_down_counter("snodra_hub_in_flight")
                .with_description("Requests awaiting a reply")
                .build(),
            connected_instances: meter
                .u64_gauge("snodra_hub_connected_instances")
                .with_description("Instances with a stream to this hub, per service")
                .build(),
            registered_types: meter
                .u64_gauge("snodra_hub_registered_types")
                .with_description("Registered message types per service and kind")
                .build(),
            buffer_full: meter
                .u64_counter("snodra_hub_session_buffer_full_total")
                .with_description("Messages not delivered because a session buffer was full (busy handler, or a terminal message the sender did not read in time)")
                .build(),
            queue_depth: meter
                .u64_gauge("snodra_hub_queue_depth")
                .with_description("Requests queued for instances, per service and type")
                .build(),
            permits_available: meter
                .u64_gauge("snodra_hub_permits_available")
                .with_description("Permits instances have left, per service")
                .build(),
            queue_wait: meter
                .f64_histogram("snodra_hub_queue_wait_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0, 60.0,
                ])
                .with_description("Time a request waited in a queue before it was forwarded")
                .build(),
            rejected: meter
                .u64_counter("snodra_hub_rejected_total")
                .with_description("Requests rejected for capacity (queue_full, queue_bytes, queue_bytes_total, buffer_full)")
                .build(),
            scatter_targets: meter
                .u64_histogram("snodra_hub_scatter_targets")
                .with_boundaries(vec![
                    1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                ])
                .with_description("Targets of one scatter-gather query")
                .build(),
            streams_open,
            stream_parts: meter
                .u64_counter("snodra_hub_stream_parts_total")
                .with_description("Parts of streaming queries relayed, per handler service and type")
                .build(),
            subscriptions_open,
            subscription_updates: meter
                .u64_counter("snodra_hub_subscription_updates_total")
                .with_description("Updates of subscription queries relayed, per handler service and type")
                .build(),
            auth_violations: meter
                .u64_counter("snodra_hub_auth_violations_total")
                .with_description("Access-control violations (ADR 0018), refused in enforce mode and allowed in permissive mode, per reason and service (`unknown` for a service name the ACL does not hold)")
                .build(),
            acl_generation: meter
                .u64_gauge("snodra_hub_acl_generation")
                .with_description("Generation of the loaded ACL: 1 at startup, plus one per reload that changed it")
                .build(),
            acl_reload_errors: meter
                .u64_counter("snodra_hub_acl_reload_errors_total")
                .with_description("ACL reloads that failed to read or parse; the previous ACL stays in force")
                .build(),
            tls_handshakes_shed: meter
                .u64_counter("snodra_hub_tls_handshakes_shed_total")
                .with_description("TCP connections dropped before their TLS handshake because every handshake slot was busy (reason `slots`) or the peer IP already held its share (reason `per_ip`), per listener")
                .build(),
            tls_handshake_failures: meter
                .u64_counter("snodra_hub_tls_handshake_failures_total")
                .with_description("TLS handshakes that failed, per listener and reason: client_cert_missing (a required client certificate was not presented), client_cert_invalid (it did not verify against the client CA or was expired), timeout, other (plain text, scans, protocol errors)")
                .build(),
            tls_client_ca_reload_errors: meter
                .u64_counter("snodra_hub_tls_client_ca_reload_errors_total")
                .with_description("Client CA bundle reloads that failed to read or parse; the previous CAs stay in force")
                .build(),
            client_certificates: meter
                .u64_counter("snodra_hub_client_certificates_total")
                .with_description("Client certificates judged (ADR 0020; not in off): one per Register that carries one, per result: ok (it names one service and has not ended; the Register may still be refused by the ACL), no_service, multiple_services, mismatch (credentials name another service), expired (it had ended at Register); plus one expired when a session reaches notAfter (closed, or in permissive kept)")
                .build(),
            jwt_validations: meter
                .u64_counter("snodra_hub_jwt_validations_total")
                .with_description("JWT credentials validated (ADR 0019), per result: ok, or why not (expired, not_yet_valid, bad_signature, bad_alg, unknown_kid, key_pending, wrong_issuer, wrong_audience, wrong_type, missing_claim, malformed, not_loaded)")
                .build(),
            jwks_fetches: meter
                .u64_counter("snodra_hub_jwks_fetches_total")
                .with_description("Signing-key set fetches, per result: ok, network, tls, timeout, status, too_large, invalid (a failed fetch keeps the last good key set)")
                .build(),
            jwks_keys: meter
                .u64_gauge("snodra_hub_jwks_keys")
                .with_description("Signing keys in the loaded key set; absent until the first successful fetch")
                .build(),
            credentials_refreshes: meter
                .u64_counter("snodra_hub_credentials_refreshes_total")
                .with_description("ClientMessage.credentials refreshes (not in off), per result: ok, refused (in permissive a refresh refused only for a handled type is still adopted), wrong_service, throttled, unavailable (not judgeable yet; the session keeps its credentials), ignored_certificate_session")
                .build(),
            control: meter
                .u64_counter("snodra_hub_control_total")
                .with_description("Processor control calls of the HTTP API (ADR 0023), per action (pause, resume, reset, dead_letter_list, dead_letter_retry, dead_letter_retry_all, dead_letter_discard) and outcome (ok, bad_request, not_found, conflict, failed, unavailable, timeout, unauthenticated, forbidden, disabled)")
                .build(),
            control_unknown_replies: meter
                .u64_counter("snodra_hub_control_unknown_replies_total")
                .with_description("ControlReply messages dropped because no request waits for them (unknown id, a reply after the timeout, or from another session than the one asked)")
                .build(),
            event_browser_requests: meter
                .u64_counter("snodra_hub_event_browser_requests_total")
                .with_description("Calls of the events API (ADR 0027), per kind (search, record, sources, lag, tail) and outcome (ok, bad_request, forbidden, not_found, too_many, unavailable, not_installed, timeout, failed)")
                .build(),
            event_browser_scanned: meter
                .u64_histogram("snodra_hub_event_browser_scanned_records")
                .with_boundaries(vec![
                    0.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 25000.0, 50000.0, 100000.0,
                ])
                .with_description("Records one event search read, as the event browser reports it")
                .build(),
            event_tails: meter
                .i64_up_down_counter("snodra_hub_event_tails")
                .with_description("Event tails open on this hub now")
                .build(),
            event_payload_views: meter
                .u64_counter("snodra_hub_event_payload_views_total")
                .with_description("Event payloads served by the record detail (each also writes an audit line)")
                .build(),
            seen: Mutex::new(Seen::default()),
            _open_gauges: open_gauges,
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

    /// One call of the events API (ADR 0027); `kind` and `outcome` are from fixed sets.
    pub fn event_request(&self, kind: &'static str, outcome: &'static str) {
        self.event_browser_requests.add(
            1,
            &[
                KeyValue::new("kind", kind),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    /// The records one search read, as the event browser reports it.
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

    /// Permits left per service and requests queued per service and type (their instances
    /// summed); a service or a (service, type) that had a value before and has none now is
    /// written back as 0.
    pub fn flows_set(&self, flows: FlowGauges) {
        let mut seen = self.seen.lock().unwrap();
        for gone in seen
            .flows
            .iter()
            .filter(|s| !flows.permits.contains_key(*s))
        {
            self.permits_available
                .record(0, &[KeyValue::new("service", gone.clone())]);
        }
        for (service, type_name) in seen
            .queued
            .iter()
            .filter(|k| !flows.queued.contains_key(*k))
        {
            self.queue_depth.record(0, &queue_attrs(service, type_name));
        }
        seen.flows = flows.permits.keys().cloned().collect();
        seen.queued = flows.queued.keys().cloned().collect();
        drop(seen);
        for (service, permits) in flows.permits {
            self.permits_available
                .record(permits, &[KeyValue::new("service", service)]);
        }
        for ((service, type_name), depth) in flows.queued {
            self.queue_depth
                .record(depth, &queue_attrs(&service, &type_name));
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

    /// A unary request ended; `service` is the handler's (`None` when it had none).
    pub fn request_finished(
        &self,
        kind: Kind,
        type_name: &str,
        service: Option<&str>,
        outcome: Outcome,
        duration: Duration,
    ) {
        self.request_finished_mode(kind, type_name, service, outcome, duration, "unary");
    }

    /// A request of `mode` (`unary`, `scatter_gather`, ...) ended. A scatter-gather counts once,
    /// whatever its number of targets. `service` is the service that handled it: `None` when
    /// no handler was found, or the targets of a gather or subscription span services.
    pub fn request_finished_mode(
        &self,
        kind: Kind,
        type_name: &str,
        service: Option<&str>,
        outcome: Outcome,
        duration: Duration,
        mode: &'static str,
    ) {
        self.in_flight_add(kind, -1);
        // The duration keeps the mode (a gather's latency is to its ReplyEnd, not comparable
        // with a unary request of the same type) but never the outcome, which is last.
        let mut attrs = Vec::with_capacity(5);
        attrs.extend([
            KeyValue::new("kind", kind_label(kind)),
            KeyValue::new("type", type_name.to_string()),
            KeyValue::new("mode", mode),
        ]);
        if let Some(service) = service {
            attrs.push(KeyValue::new("service", service.to_string()));
        }
        attrs.push(KeyValue::new("outcome", outcome.as_str()));
        self.requests.add(1, &attrs);
        self.duration
            .record(duration.as_secs_f64(), &attrs[..attrs.len() - 1]);
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
        self.streams_open.add(service, instance, 1);
    }

    /// A streaming query served by `instance` of `service` ended, however.
    pub fn stream_closed(&self, service: &str, instance: &str) {
        self.streams_open.add(service, instance, -1);
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
        self.subscriptions_open.add(service, instance, 1);
    }

    /// A subscription target served by `instance` of `service` ended, however.
    pub fn subscription_closed(&self, service: &str, instance: &str) {
        self.subscriptions_open.add(service, instance, -1);
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

/// Streams or subscription targets open per (handler service, handler instance id), kept by the
/// router's open and close calls and observed when the metrics are exported: only instances with
/// one open are held, so the label set is the live handler instances, not every instance id
/// seen since the hub started (an up-down counter would keep each one at 0 forever).
#[derive(Default)]
struct OpenCounts(Mutex<HashMap<(String, String), i64>>);

impl OpenCounts {
    fn add(&self, service: &str, instance: &str, delta: i64) {
        let mut open = self.0.lock().unwrap();
        let key = (service.to_string(), instance.to_string());
        let n = open.get(&key).copied().unwrap_or(0) + delta;
        if n > 0 {
            open.insert(key, n);
        } else {
            open.remove(&key);
        }
    }

    /// The gauge `name` of `counts`; an instance whose last one closed is exported once more as
    /// 0, then not at all ([`Departed`]).
    fn observe(
        meter: &Meter,
        name: &'static str,
        description: &'static str,
        counts: Arc<OpenCounts>,
    ) -> ObservableGauge<i64> {
        let departed = Departed::new();
        meter
            .i64_observable_gauge(name)
            .with_description(description)
            .with_callback(move |observer| {
                let open: Vec<((String, String), i64)> = counts
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, n)| (k.clone(), *n))
                    .collect();
                for (service, instance) in
                    departed.gone(open.iter().map(|(k, _)| k.clone()).collect())
                {
                    observer.observe(0, &stream_attrs(&service, &instance));
                }
                for ((service, instance), n) in &open {
                    observer.observe(*n, &stream_attrs(service, instance));
                }
            })
            .build()
    }
}

/// The keys an observed gauge exported last time. Such a gauge exports only what its callback
/// observes, and Prometheus keeps a series that stops at its last value until it goes stale
/// (minutes): a key that went away is exported once more as 0 instead.
struct Departed<K>(Mutex<HashSet<K>>);

impl<K: Eq + std::hash::Hash + Clone> Departed<K> {
    fn new() -> Self {
        Self(Mutex::new(HashSet::new()))
    }

    /// Keeps `present` as the keys of this export and returns the keys of the last one that
    /// are not in it.
    fn gone(&self, present: HashSet<K>) -> Vec<K> {
        let mut last = self.0.lock().unwrap();
        let gone = last.difference(&present).cloned().collect();
        *last = present;
        gone
    }
}

/// One processor of one service, aggregated over its instances ([`processors::by_service`]).
struct ProcessorRow {
    service: String,
    processor: String,
    lag: Option<i64>,
    dead_letters: u64,
    state: ProcessorStateName,
    processed: u64,
    failed: u64,
}

impl ProcessorRow {
    fn key(&self) -> (String, String) {
        (self.service.clone(), self.processor.clone())
    }
}

fn processor_attrs(service: &str, processor: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("service", service.to_string()),
        KeyValue::new("processor", processor.to_string()),
    ]
}

/// The processor gauges (ADR 0022, 0026), per service and processor, aggregated over the
/// instances as the Processors page does, from the latest `InstanceStatus` each session holds:
/// observed when the metrics are exported (by the exporter's thread), from memory: no I/O, no
/// event-store read. Every instance reports to every hub (ADR 0016), so hubs export the same
/// values: dashboards take `max` over hubs. Dropping the value unregisters nothing; keep it for
/// the life of the meter provider.
pub struct ProcessorGauges {
    _lag: ObservableGauge<i64>,
    _dead_letters: ObservableGauge<u64>,
    _state: ObservableGauge<u64>,
    _processed: ObservableGauge<u64>,
    _failed: ObservableGauge<u64>,
}

impl ProcessorGauges {
    pub fn register(meter: &Meter, sessions: Arc<Sessions>) -> Self {
        let rows = move || -> Vec<ProcessorRow> {
            let reports = sessions.processor_reports(std::time::Instant::now());
            processors::by_service(reports)
                .into_iter()
                .flat_map(|s| {
                    let service = s.service_name;
                    s.processors.into_iter().map(move |p| ProcessorRow {
                        service: service.clone(),
                        processor: p.name,
                        lag: p.lag,
                        dead_letters: p.dead_letters,
                        state: p.state,
                        processed: p.processed_total,
                        failed: p.failed_total,
                    })
                })
                .collect()
        };
        let rows = Arc::new(rows);
        // A gauge of one value per (service, processor); `None` exports nothing for that key
        // (an unknown lag) without counting it as gone.
        fn per_processor<T: Copy + Default + Send + Sync + 'static>(
            rows: &Arc<impl Fn() -> Vec<ProcessorRow> + Send + Sync + 'static>,
            value: fn(&ProcessorRow) -> Option<T>,
        ) -> impl Fn(&dyn AsyncInstrument<T>) + Send + Sync + 'static {
            let rows = Arc::clone(rows);
            let departed = Departed::new();
            move |observer| {
                let rows = rows();
                for (service, processor) in
                    departed.gone(rows.iter().map(ProcessorRow::key).collect())
                {
                    observer.observe(T::default(), &processor_attrs(&service, &processor));
                }
                for row in &rows {
                    if let Some(v) = value(row) {
                        observer.observe(v, &processor_attrs(&row.service, &row.processor));
                    }
                }
            }
        }
        let state = {
            let rows = Arc::clone(&rows);
            let departed = Departed::new();
            move |observer: &dyn AsyncInstrument<u64>| {
                let rows = rows();
                let gone = departed.gone(rows.iter().map(ProcessorRow::key).collect());
                let current = rows
                    .iter()
                    .map(|r| (r.service.clone(), r.processor.clone(), Some(r.state)));
                let gone = gone.into_iter().map(|(s, p)| (s, p, None));
                for (service, processor, current) in current.chain(gone) {
                    for state in ProcessorStateName::ALL {
                        let [s, p] = processor_attrs(&service, &processor);
                        observer.observe(
                            u64::from(current == Some(state)),
                            &[s, p, KeyValue::new("state", state.label())],
                        );
                    }
                }
            }
        };
        Self {
            _lag: meter
                .i64_observable_gauge("snodra_hub_processor_lag")
                .with_description("Records behind per service and processor: the sum over its distinct segments as the freshest report has them; absent while no lag is known")
                .with_callback(per_processor(&rows, |r| r.lag))
                .build(),
            _dead_letters: meter
                .u64_observable_gauge("snodra_hub_processor_dead_letters")
                .with_description("Dead letters per service and processor: the highest count its instances report")
                .with_callback(per_processor(&rows, |r| Some(r.dead_letters)))
                .build(),
            _state: meter
                .u64_observable_gauge("snodra_hub_processor_state")
                .with_description("1 for the state of the processor (the worst over its instances), 0 for the other states")
                .with_callback(state)
                .build(),
            _processed: meter
                .u64_observable_gauge("snodra_hub_processor_processed")
                .with_description("Records processed, summed over the instances reporting the processor, each since its own start (a gauge: it drops when an instance restarts; rates come from snodra_events_consumed_total)")
                .with_callback(per_processor(&rows, |r| Some(r.processed)))
                .build(),
            _failed: meter
                .u64_observable_gauge("snodra_hub_processor_failed")
                .with_description("Records whose handling failed, summed over the instances reporting the processor, each since its own start (a gauge, as snodra_hub_processor_processed)")
                .with_callback(per_processor(&rows, |r| Some(r.failed)))
                .build(),
        }
    }
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

    fn sorted_attrs<'a>(kvs: impl Iterator<Item = &'a KeyValue>) -> Vec<(String, String)> {
        let mut attrs: Vec<(String, String)> = kvs
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect();
        attrs.sort();
        attrs
    }

    /// One gauge point: its attributes, sorted, and its value.
    pub(crate) type Point = (Vec<(String, String)>, i64);

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
        /// A meter of the same provider, for the observed gauges.
        pub(crate) fn meter(&self) -> Meter {
            self.provider.meter("test")
        }

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

        /// The sum of the i64 or u64 gauge `name` over the points whose attribute `key` is
        /// `value`, from one export.
        pub(crate) fn gauge_sum(&self, name: &str, key: &str, value: &str) -> Option<i64> {
            let points: Vec<i64> = self
                .gauge_points(name)
                .into_iter()
                .filter(|(attrs, _)| attrs.iter().any(|(k, v)| k == key && v == value))
                .map(|(_, v)| v)
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

        /// `f` of the metric `name` as exported now; `None` when it is not.
        fn with_metric<R>(
            &self,
            name: &str,
            f: impl FnOnce(&opentelemetry_sdk::metrics::data::Metric) -> R,
        ) -> Option<R> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)
                .map(f)
        }

        /// The sum of the u64 counter `name` over the points that have no attribute `key`.
        pub(crate) fn counter_without(&self, name: &str, key: &str) -> Option<u64> {
            let points: Vec<u64> = self.with_metric(name, |metric| {
                let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                    panic!("{name} is not a u64 counter");
                };
                sum.data_points()
                    .filter(|p| p.attributes().all(|kv| kv.key.as_str() != key))
                    .map(|p| p.value())
                    .collect()
            })?;
            (!points.is_empty()).then(|| points.iter().sum())
        }

        /// The count of the f64 histogram `name` over the points whose attribute `key` is
        /// `value`.
        pub(crate) fn histogram_count(&self, name: &str, key: &str, value: &str) -> Option<u64> {
            let counts: Vec<u64> = self.with_metric(name, |metric| {
                let AggregatedMetrics::F64(MetricData::Histogram(h)) = metric.data() else {
                    panic!("{name} is not an f64 histogram");
                };
                h.data_points()
                    .filter(|p| {
                        p.attributes()
                            .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                    })
                    .map(|p| p.count())
                    .collect()
            })?;
            (!counts.is_empty()).then(|| counts.iter().sum())
        }

        /// The value of the u64 or i64 gauge `name` whose attributes are exactly `attrs`.
        pub(crate) fn gauge_where(&self, name: &str, attrs: &[(&str, &str)]) -> Option<i64> {
            let mut want: Vec<(String, String)> = attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            want.sort();
            self.gauge_points(name)
                .into_iter()
                .find(|(got, _)| *got == want)
                .map(|(_, v)| v)
        }

        /// Every point of every u64 or i64 gauge with at least one point, from one export.
        pub(crate) fn all_gauge_points(&self) -> std::collections::BTreeMap<String, Vec<Point>> {
            self.exporter.reset();
            self.provider.force_flush().unwrap();
            let finished = self.exporter.get_finished_metrics().unwrap();
            let mut out = std::collections::BTreeMap::new();
            for metric in finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
            {
                let mut points: Vec<Point> = match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(g)) => g
                        .data_points()
                        .map(|p| (sorted_attrs(p.attributes()), p.value() as i64))
                        .collect(),
                    AggregatedMetrics::I64(MetricData::Gauge(g)) => g
                        .data_points()
                        .map(|p| (sorted_attrs(p.attributes()), p.value()))
                        .collect(),
                    _ => continue,
                };
                if points.is_empty() {
                    continue;
                }
                points.sort();
                out.insert(metric.name().to_string(), points);
            }
            out
        }

        /// Every point of the u64 or i64 gauge `name` as (sorted attributes, value), sorted.
        pub(crate) fn gauge_points(&self, name: &str) -> Vec<Point> {
            let mut points: Vec<Point> = self
                .with_metric(name, |metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(g)) => g
                        .data_points()
                        .map(|p| (sorted_attrs(p.attributes()), p.value() as i64))
                        .collect(),
                    AggregatedMetrics::I64(MetricData::Gauge(g)) => g
                        .data_points()
                        .map(|p| (sorted_attrs(p.attributes()), p.value()))
                        .collect(),
                    _ => panic!("{name} is not a u64 or i64 gauge"),
                })
                .unwrap_or_default();
            points.sort();
            points
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

        metrics.request_finished(
            Kind::Command,
            "a.B",
            Some("A"),
            Outcome::Ok,
            Duration::from_millis(10),
        );
        provider.force_flush().unwrap();

        let finished = exporter.get_finished_metrics().unwrap();
        let metric = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "snodra_hub_requests_total")
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
        assert!(attrs.contains(&("service".into(), "A".into())), "{attrs:?}");
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
            m.gauge_sum("snodra_hub_subscriptions_open", "instance_id", "a1"),
            Some(1)
        );
        assert_eq!(
            m.gauge_sum("snodra_hub_subscriptions_open", "instance_id", "b1"),
            Some(1)
        );
        assert_eq!(
            m.counter("snodra_hub_subscription_updates_total", "service", "A"),
            Some(2)
        );
        assert_eq!(
            m.counter("snodra_hub_subscription_updates_total", "type", "q.Watch"),
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
    /// does not mix with the unary latency of the same type, and the handling service when
    /// there is one; never the outcome.
    #[test]
    fn duration_histogram_has_kind_type_mode_and_service_labels_only() {
        let (exporter, provider, metrics) = exporter_and_metrics();
        metrics.request_finished(
            Kind::Command,
            "a.B",
            Some("A"),
            Outcome::Ok,
            Duration::from_millis(10),
        );
        metrics.request_finished_mode(
            Kind::Query,
            "q.F",
            None,
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
            .find(|m| m.name() == "snodra_hub_request_duration_seconds")
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
        let labels = |kind: &str, type_name: &str, mode: &str, service: Option<&str>| {
            let mut labels = vec![
                ("kind".to_string(), kind.to_string()),
                ("mode".to_string(), mode.to_string()),
                ("type".to_string(), type_name.to_string()),
            ];
            if let Some(service) = service {
                labels.insert(2, ("service".to_string(), service.to_string()));
            }
            labels
        };
        assert_eq!(
            points,
            [
                (labels("command", "a.B", "unary", Some("A")), 1),
                (labels("query", "q.F", "scatter_gather", None), 1)
            ]
        );
        let point = h.data_points().next().expect("one data point");
        assert_eq!(point.bounds().next(), Some(0.001));
    }

    #[test]
    fn in_flight_returns_to_zero_after_finish() {
        let (exporter, provider, metrics) = exporter_and_metrics();
        metrics.request_started(Kind::Query);
        metrics.request_finished(Kind::Query, "q.X", None, Outcome::Ok, Duration::ZERO);
        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();
        let metric = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .find(|m| m.name() == "snodra_hub_in_flight")
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
        assert_eq!(get("service.name").as_deref(), Some("snodra-server"));
        assert_eq!(get("service.instance.id").as_deref(), Some("hub-7"));
        assert_eq!(get("service.namespace"), None, "it would change `job`");
    }

    /// One gauge per service; a service whose last instance left is written back as 0.
    #[test]
    fn connected_instances_are_counted_per_service() {
        let m = test_support::test_metrics();
        m.metrics
            .connected_instances_set(HashMap::from([("A".to_string(), 2), ("B".to_string(), 1)]));
        assert_eq!(m.gauge("snodra_hub_connected_instances", "A"), Some(2));
        assert_eq!(m.gauge("snodra_hub_connected_instances", "B"), Some(1));
        m.metrics
            .connected_instances_set(HashMap::from([("A".to_string(), 1)]));
        assert_eq!(m.gauge("snodra_hub_connected_instances", "A"), Some(1));
        assert_eq!(m.gauge("snodra_hub_connected_instances", "B"), Some(0));
    }

    /// The open streams and subscription targets are gauges of the live counts per handler
    /// instance: an instance whose last one closed reads 0 once, then is no longer exported, so
    /// the series of past instance ids (a fresh id per start) do not pile up (ADR 0026).
    #[test]
    fn an_instance_whose_last_stream_closed_reads_0_once_then_is_gone() {
        let m = test_support::test_metrics();
        let open = |m: &test_support::TestMetrics| {
            let export = m.all_gauge_points();
            ["snodra_hub_streams_open", "snodra_hub_subscriptions_open"].map(|name| {
                export
                    .get(name)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(attrs, v)| {
                        let id = attrs.iter().find(|(k, _)| k == "instance_id").unwrap();
                        (id.1.clone(), v)
                    })
                    .collect::<Vec<_>>()
            })
        };
        m.metrics.stream_opened("A", "a1");
        m.metrics.stream_opened("A", "a1");
        m.metrics.subscription_opened("A", "a1");
        assert_eq!(open(&m), [vec![("a1".into(), 2)], vec![("a1".into(), 1)]]);
        m.metrics.stream_closed("A", "a1");
        m.metrics.stream_closed("A", "a1");
        m.metrics.subscription_closed("A", "a1");
        m.metrics.stream_opened("A", "a2");
        assert_eq!(
            open(&m),
            [
                vec![("a1".into(), 0), ("a2".into(), 1)],
                vec![("a1".into(), 0)]
            ]
        );
        assert_eq!(open(&m), [vec![("a2".into(), 1)], vec![]]);
    }

    /// A data-point attribute named `instance` (or `job`) is overwritten by the resource's in
    /// Prometheus, so the handler instance is `instance_id`.
    #[test]
    fn open_streams_and_subscriptions_name_the_handler_instance_instance_id() {
        let m = test_support::test_metrics();
        m.metrics.stream_opened("A", "a1");
        m.metrics.subscription_opened("A", "a2");
        assert_eq!(
            m.gauge_sum("snodra_hub_streams_open", "instance_id", "a1"),
            Some(1)
        );
        assert_eq!(
            m.gauge_sum("snodra_hub_subscriptions_open", "instance_id", "a2"),
            Some(1)
        );
        assert_eq!(
            m.gauge_sum("snodra_hub_streams_open", "instance", "a1"),
            None
        );
    }

    fn processor(
        name: &str,
        state: crate::proto::ProcessorState,
        lags: &[(&str, i64)],
        dead_letters: u64,
        totals: (u64, u64),
    ) -> crate::proto::ProcessorStatus {
        crate::proto::ProcessorStatus {
            name: name.to_string(),
            subscription: "g".to_string(),
            source: "kraken.events".to_string(),
            state: state as i32,
            segments: lags
                .iter()
                .map(|(segment, lag)| crate::proto::SegmentStatus {
                    segment: segment.to_string(),
                    lag: *lag,
                    ..Default::default()
                })
                .collect(),
            dead_letters,
            processed_total: totals.0,
            failed_total: totals.1,
            ..Default::default()
        }
    }

    /// One reporting instance of `service`; returns its token.
    fn report(
        sessions: &Sessions,
        id: &str,
        service: &str,
        processors: Vec<crate::proto::ProcessorStatus>,
        at: std::time::Instant,
    ) -> u64 {
        let ctx = crate::context::ContextId::default();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let token = sessions.insert(
            &ctx,
            id,
            service,
            crate::session::SessionMeta::default(),
            tx,
            tokio_util::sync::CancellationToken::new(),
        );
        let status = crate::proto::InstanceStatus {
            processors,
            ..Default::default()
        };
        assert!(sessions.set_status(&ctx, id, token, &status, at));
        token
    }

    /// The processor gauges aggregate each (service, processor) over its instances as the
    /// Processors page does: lag over distinct segments as the freshest report has them, the
    /// worst state, the highest dead-letter count, the summed totals (ADR 0026).
    #[test]
    fn processor_gauges_aggregate_the_latest_reports_per_service_and_processor() {
        use crate::proto::ProcessorState::{ProcessorFailing, ProcessorRunning};
        let m = test_support::test_metrics();
        let sessions = Arc::new(Sessions::new());
        let _gauges = ProcessorGauges::register(&m.meter(), Arc::clone(&sessions));
        let now = std::time::Instant::now();
        let earlier = now - Duration::from_secs(3);
        report(
            &sessions,
            "a1",
            "svc",
            vec![processor(
                "default",
                ProcessorRunning,
                &[("0", 5), ("1", 7)],
                2,
                (100, 1),
            )],
            earlier,
        );
        report(
            &sessions,
            "a2",
            "svc",
            vec![
                processor(
                    "default",
                    ProcessorFailing,
                    &[("1", 3), ("2", 4)],
                    3,
                    (50, 2),
                ),
                processor("audit", ProcessorRunning, &[("0", -1)], 0, (0, 0)),
            ],
            now,
        );
        let labels = |processor: &str| {
            vec![
                ("processor".to_string(), processor.to_string()),
                ("service".to_string(), "svc".to_string()),
            ]
        };
        // Segment 1 as a2 (the freshest) has it: 5 + 3 + 4; audit's lag is unknown.
        assert_eq!(
            m.gauge_points("snodra_hub_processor_lag"),
            [(labels("default"), 12)]
        );
        assert_eq!(
            m.gauge_points("snodra_hub_processor_dead_letters"),
            [(labels("audit"), 0), (labels("default"), 3)]
        );
        assert_eq!(
            m.gauge_points("snodra_hub_processor_processed"),
            [(labels("audit"), 0), (labels("default"), 150)]
        );
        assert_eq!(
            m.gauge_points("snodra_hub_processor_failed"),
            [(labels("audit"), 0), (labels("default"), 3)]
        );
        let states: Vec<(String, i64)> = m
            .gauge_points("snodra_hub_processor_state")
            .into_iter()
            .filter(|(attrs, _)| attrs.contains(&("processor".into(), "default".into())))
            .map(|(attrs, v)| {
                let state = attrs.iter().find(|(k, _)| k == "state").unwrap().1.clone();
                (state, v)
            })
            .collect();
        assert_eq!(
            states,
            [
                ("failing".to_string(), 1),
                ("paused".to_string(), 0),
                ("running".to_string(), 0),
                ("starting".to_string(), 0),
                ("stopped".to_string(), 0),
                ("unspecified".to_string(), 0),
            ]
        );
    }

    /// A processor nobody reports any more is exported once more as 0 (an observed gauge
    /// otherwise keeps its last value in Prometheus until the series goes stale), then not at
    /// all.
    #[test]
    fn a_processor_that_went_away_reads_0_once_then_is_gone() {
        use crate::proto::ProcessorState::ProcessorRunning;
        let m = test_support::test_metrics();
        let sessions = Arc::new(Sessions::new());
        let _gauges = ProcessorGauges::register(&m.meter(), Arc::clone(&sessions));
        let token = report(
            &sessions,
            "a1",
            "svc",
            vec![processor(
                "default",
                ProcessorRunning,
                &[("0", 9)],
                1,
                (5, 0),
            )],
            std::time::Instant::now(),
        );
        assert_eq!(m.gauge_points("snodra_hub_processor_lag").len(), 1);
        assert_eq!(m.gauge_points("snodra_hub_processor_state").len(), 6);
        sessions.remove_if(&crate::context::ContextId::default(), "a1", token);
        // One export: every gauge of the processor once more, at 0.
        let export = m.all_gauge_points();
        for name in [
            "snodra_hub_processor_lag",
            "snodra_hub_processor_dead_letters",
            "snodra_hub_processor_processed",
            "snodra_hub_processor_failed",
        ] {
            assert_eq!(export[name].len(), 1, "{name}");
            assert_eq!(export[name][0].1, 0, "{name}");
        }
        let state = &export["snodra_hub_processor_state"];
        assert_eq!(state.len(), 6);
        assert!(state.iter().all(|(_, v)| *v == 0), "{state:?}");
        // The next export has none of them.
        let export = m.all_gauge_points();
        assert!(
            export
                .keys()
                .all(|name| !name.starts_with("snodra_hub_processor_")),
            "{export:?}"
        );
    }

    /// A processor whose lag becomes unknown is still reported: its lag is absent, not 0 (0
    /// means caught up), and the departure rule (0 once) is not applied to it (ADR 0026 hub
    /// ruling 9).
    #[test]
    fn a_lag_that_becomes_unknown_is_absent_not_0() {
        use crate::proto::ProcessorState::ProcessorRunning;
        let m = test_support::test_metrics();
        let sessions = Arc::new(Sessions::new());
        let _gauges = ProcessorGauges::register(&m.meter(), Arc::clone(&sessions));
        let p = |lag| {
            vec![processor(
                "default",
                ProcessorRunning,
                &[("0", lag)],
                0,
                (1, 0),
            )]
        };
        report(&sessions, "a1", "svc", p(9), std::time::Instant::now());
        assert_eq!(m.gauge_points("snodra_hub_processor_lag").len(), 1);
        report(&sessions, "a1", "svc", p(-1), std::time::Instant::now());
        for _ in 0..2 {
            let export = m.all_gauge_points();
            assert!(
                !export.contains_key("snodra_hub_processor_lag"),
                "{export:?}"
            );
            assert_eq!(export["snodra_hub_processor_state"].len(), 6);
        }
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

    /// The descriptions of the credential counters name every result the code counts, and when
    /// it counts them (H-2): a certificate is counted once per `Register` that carries one
    /// (`expired` included, when it ended before), and once more when a session reaches
    /// `notAfter`; never in `off`.
    #[test]
    fn the_credential_counters_describe_every_result_they_count() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = Metrics::new(provider.meter("test"));
        metrics.client_certificate("ok");
        metrics.credentials_refresh("ok");
        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();
        let description = |name: &str| {
            finished
                .iter()
                .flat_map(|rm| rm.scope_metrics())
                .flat_map(|sm| sm.metrics())
                .find(|m| m.name() == name)
                .map(|m| m.description().to_string())
                .expect(name)
        };
        let certificates = description("snodra_hub_client_certificates_total");
        for words in [
            "ok",
            "no_service",
            "multiple_services",
            "mismatch",
            "expired",
            "at Register",
            "notAfter",
            "not in off",
        ] {
            assert!(certificates.contains(words), "{words}: {certificates}");
        }
        let refreshes = description("snodra_hub_credentials_refreshes_total");
        for result in [
            "ok",
            "refused",
            "wrong_service",
            "throttled",
            "unavailable",
            "ignored_certificate_session",
        ] {
            assert!(refreshes.contains(result), "{result}: {refreshes}");
        }
    }
}
