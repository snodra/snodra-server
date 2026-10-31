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
            let resource = Resource::builder().with_service_name(SERVICE_NAME).build();
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
    /// The hub rejected the request for capacity (full queue or session buffer), whatever code
    /// the sender was sent (RESOURCE_EXHAUSTED, or HANDLER_ERROR "handler busy").
    ResourceExhausted,
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
        }
    }
}

pub fn kind_label(kind: Kind) -> &'static str {
    match kind {
        Kind::Command => "command",
        Kind::Query => "query",
    }
}

/// Router metrics: the six instruments of the hub spec. The `sessions` and `registered_types`
/// gauges are set imperatively (not observable callbacks); services that disappeared are
/// written back as 0 so the exported series does not go stale at its last value.
pub struct Metrics {
    requests: Counter<u64>,
    duration: Histogram<f64>,
    in_flight: UpDownCounter<i64>,
    sessions: Gauge<u64>,
    registered_types: Gauge<u64>,
    buffer_full: Counter<u64>,
    queue_depth: Gauge<u64>,
    permits_available: Gauge<u64>,
    queue_wait: Histogram<f64>,
    rejected: Counter<u64>,
    seen: Mutex<Seen>,
}

#[derive(Default)]
struct Seen {
    sessions: HashSet<String>,
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
            sessions: meter
                .u64_gauge("kraken_hub_sessions")
                .with_description("Connected sessions per service")
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
                .with_description("Requests queued for flow-controlled instances, per service")
                .build(),
            permits_available: meter
                .u64_gauge("kraken_hub_permits_available")
                .with_description("Permits flow-controlled instances have left, per service")
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
            seen: Mutex::new(Seen::default()),
        })
    }

    /// Queue depth and permits left per service (flow-controlled instances summed); services
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

    pub fn request_finished(
        &self,
        kind: Kind,
        type_name: &str,
        outcome: Outcome,
        duration: Duration,
    ) {
        self.in_flight_add(kind, -1);
        let attrs = [
            KeyValue::new("kind", kind_label(kind)),
            KeyValue::new("type", type_name.to_string()),
            KeyValue::new("outcome", outcome.as_str()),
        ];
        self.requests.add(1, &attrs);
        self.duration.record(duration.as_secs_f64(), &attrs[..2]);
    }

    pub fn buffer_full(&self, service: &str) {
        self.buffer_full
            .add(1, &[KeyValue::new("service", service.to_string())]);
    }

    pub fn sessions_set(&self, by_service: HashMap<String, usize>) {
        let mut seen = self.seen.lock().unwrap();
        for gone in seen
            .sessions
            .iter()
            .filter(|s| !by_service.contains_key(*s))
        {
            self.sessions
                .record(0, &[KeyValue::new("service", gone.clone())]);
        }
        seen.sessions = by_service.keys().cloned().collect();
        drop(seen);
        for (service, n) in by_service {
            self.sessions
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

/// In-memory metrics for unit tests elsewhere in the crate.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

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

    fn exporter_and_metrics() -> (InMemoryMetricExporter, SdkMeterProvider, Arc<Metrics>) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = Metrics::new(provider.meter("test"));
        (exporter, provider, metrics)
    }

    #[test]
    fn duration_histogram_has_kind_and_type_labels_only() {
        let (exporter, provider, metrics) = exporter_and_metrics();
        metrics.request_finished(Kind::Command, "a.B", Outcome::Ok, Duration::from_millis(10));
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
        let point = h.data_points().next().expect("one data point");
        let mut attrs: Vec<(String, String)> = point
            .attributes()
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect();
        attrs.sort();
        assert_eq!(
            attrs,
            [
                ("kind".to_string(), "command".to_string()),
                ("type".to_string(), "a.B".to_string())
            ]
        );
        assert_eq!(point.count(), 1);
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
