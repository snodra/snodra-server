//! The HTTP port (`KRAKEN_HUB_HTTP_LISTEN`, default 7001): `GET /topology` for the CLI and the
//! smoke tests, and, unless `KRAKEN_HUB_DASHBOARD_ENABLED=false`, the read-only dashboard
//! (`/ui/`, see [`crate::assets`]) and its API under `/api/v1/` (roadmap P2). Every route is
//! read-only and unauthenticated in this phase; the port belongs on the private network (P5a
//! adds a dashboard token).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::edges::{EdgeSnapshot, WindowTotals};
use crate::registry::Registry;
use crate::router::Router;
use crate::session::Sessions;
use crate::topology::{self, ClockAnchor, Topology};
use crate::{api, assets};

/// The live stream recomputes its summary this often and sends it when it changed...
pub const STREAM_CHECK: Duration = Duration::from_millis(500);
/// ...and at least this often.
pub const STREAM_EVERY: Duration = Duration::from_secs(2);
/// Open `/api/v1/stream` connections allowed at once; beyond it the stream answers 503 and the
/// page polls instead.
pub const MAX_STREAMS: usize = 64;

/// `Content-Security-Policy` of every dashboard and API response: only the hub's own scripts,
/// styles, images and API; no inline script or style, no framing, no plugins.
pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; \
connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; \
frame-ancestors 'none'";

/// Everything the HTTP handlers read; the same `Arc`s the gRPC service holds.
#[derive(Clone)]
pub struct HttpState {
    pub registry: Arc<Registry>,
    pub sessions: Arc<Sessions>,
    pub router: Arc<Router>,
    pub config: Arc<Config>,
    pub anchor: ClockAnchor,
    pub started: Instant,
    /// Content hash of the dashboard assets, reported by `version` and the stream.
    pub build_id: String,
    /// Cancelled at shutdown: open streams end within [`STREAM_CHECK`].
    pub stopping: CancellationToken,
    /// Open `/api/v1/stream` connections.
    pub streams: Arc<AtomicUsize>,
}

impl HttpState {
    fn topology(&self) -> Topology {
        topology::snapshot(
            &self.registry,
            &self.sessions,
            &self.router,
            self.anchor,
            self.started,
            &self.config.hub_id,
            self.config.heartbeat_timeout,
            Instant::now(),
        )
    }

    fn edges(&self) -> EdgeSnapshot {
        let now = tokio::time::Instant::now();
        match self.router.edges() {
            Some(edges) => edges.snapshot(now),
            None => EdgeSnapshot {
                window: self.config.edge_stats_window,
                max_edges: self.config.edge_stats_max_edges,
                evicted: 0,
                edges: vec![],
            },
        }
    }

    fn window_totals(&self) -> WindowTotals {
        self.router
            .edges()
            .map(|e| e.totals(tokio::time::Instant::now()))
            .unwrap_or_default()
    }
}

/// The HTTP routes: `/topology` always; the dashboard (`/`, `/ui` redirect to `/ui/`) and its
/// API when enabled. Unknown paths are 404.
pub fn app(state: HttpState) -> axum::Router {
    let mut app = axum::Router::new().route("/topology", get(get_topology));
    if state.config.dashboard_enabled {
        let api = axum::Router::new()
            .route("/", get(assets::to_index))
            .route("/ui", get(assets::to_index))
            .route("/ui/", get(assets::index))
            .route("/ui/v/{build}/{*path}", get(assets::asset))
            .route("/api/v1/topology", get(api_topology))
            .route("/api/v1/types", get(api_types))
            .route("/api/v1/graph", get(api_graph))
            .route("/api/v1/settings", get(api_settings))
            .route("/api/v1/version", get(api_version))
            .route("/api/v1/stream", get(api_stream))
            .layer(axum::middleware::map_response(security_headers));
        app = app.merge(api);
    }
    app.with_state(state)
}

/// Headers every dashboard and API response carries.
pub async fn security_headers(mut response: Response) -> Response {
    let h = response.headers_mut();
    let set = |h: &mut axum::http::HeaderMap, name: HeaderName, value: &'static str| {
        h.entry(name)
            .or_insert_with(|| HeaderValue::from_static(value));
    };
    set(h, header::CONTENT_SECURITY_POLICY, CSP);
    set(h, header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    set(h, header::REFERRER_POLICY, "no-referrer");
    set(h, header::X_FRAME_OPTIONS, "DENY");
    response
}

/// JSON that no cache keeps.
fn fresh_json<T: Serialize>(value: T) -> Response {
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(value),
    )
        .into_response()
}

async fn get_topology(State(s): State<HttpState>) -> Json<Topology> {
    Json(s.topology())
}

async fn api_topology(State(s): State<HttpState>) -> Response {
    fresh_json(s.topology())
}

async fn api_types(State(s): State<HttpState>) -> Response {
    let (t, e) = (s.topology(), s.edges());
    fresh_json(api::types(&t, &e))
}

async fn api_graph(State(s): State<HttpState>) -> Response {
    let (t, e) = (s.topology(), s.edges());
    fresh_json(api::graph(&t, &e))
}

async fn api_settings(State(s): State<HttpState>) -> Response {
    fresh_json(api::settings(&s.config))
}

async fn api_version(State(s): State<HttpState>) -> Response {
    fresh_json(api::version(&s.config, &s.topology(), &s.build_id))
}

/// Holds one of the [`MAX_STREAMS`] stream slots until dropped.
struct StreamSlot(Arc<AtomicUsize>);

impl StreamSlot {
    fn acquire(open: &Arc<AtomicUsize>) -> Option<Self> {
        open.try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < MAX_STREAMS).then_some(n + 1)
        })
        .ok()
        .map(|_| Self(Arc::clone(open)))
    }
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Server-sent events: a `summary` event (totals, window counts, a structural `revision`) at
/// once, then whenever it changed (checked every [`STREAM_CHECK`]) and at least every
/// [`STREAM_EVERY`]. Ends at shutdown.
async fn api_stream(State(s): State<HttpState>) -> Response {
    let Some(slot) = StreamSlot::acquire(&s.streams) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "too many open dashboard streams; poll /api/v1/topology instead",
        )
            .into_response();
    };
    let mut ticker = tokio::time::interval(STREAM_CHECK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last: Option<(api::Summary, Instant)> = None;
    let events = IntervalStream::new(ticker)
        .map_while(move |_| {
            let _slot = &slot;
            if s.stopping.is_cancelled() {
                return None;
            }
            let summary = api::summary(&s.topology(), &s.window_totals(), &s.build_id);
            let due = last.as_ref().is_none_or(|(prev, sent)| {
                sent.elapsed() >= STREAM_EVERY
                    || prev.revision != summary.revision
                    || prev.totals != summary.totals
                    || prev.stale_instances != summary.stale_instances
            });
            if !due {
                return Some(None);
            }
            let event = Event::default().event("summary").json_data(&summary).ok();
            last = Some((summary, Instant::now()));
            Some(event)
        })
        .filter_map(|event| event)
        .map(Ok::<_, Infallible>);
    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    // Proxies that buffer responses (nginx) would hold the events back.
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_slots_are_bounded_and_given_back() {
        let open = Arc::new(AtomicUsize::new(0));
        let slots: Vec<StreamSlot> = (0..MAX_STREAMS)
            .map(|_| StreamSlot::acquire(&open).expect("slot"))
            .collect();
        assert!(StreamSlot::acquire(&open).is_none());
        drop(slots);
        assert_eq!(open.load(Ordering::Acquire), 0);
        assert!(StreamSlot::acquire(&open).is_some());
    }
}
