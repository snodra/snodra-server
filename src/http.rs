//! The HTTP port (`KRAKEN_HUB_HTTP_LISTEN`, default 7001): `GET /topology` for the CLI and the
//! smoke tests, and, unless `KRAKEN_HUB_DASHBOARD_ENABLED=false`, the read-only dashboard
//! (`/ui/`, see [`crate::assets`]) and its API under `/api/v1/` (roadmap P2). Every route is
//! read-only and unauthenticated in this phase; the port belongs on the private network (P5a
//! adds a dashboard token).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::edges::{EdgeSnapshot, WindowTotals};
use crate::registry::Registry;
use crate::router::Router;
use crate::session::Sessions;
use crate::topology::{self, ClockAnchor, Topology};
use crate::{api, assets};

/// The shared live summary is recomputed this often while a stream is open; each stream sends
/// it when it changed...
pub const STREAM_CHECK: Duration = Duration::from_millis(500);
/// ...and at least this often.
pub const STREAM_EVERY: Duration = Duration::from_secs(2);
/// Open `/api/v1/stream` connections allowed at once; beyond it the stream answers 503 and the
/// page polls instead.
pub const MAX_STREAMS: usize = 64;

/// `Content-Security-Policy` of every dashboard and API response: only the hub's own scripts,
/// styles, images and API, plus the peer hubs' APIs (`KRAKEN_HUB_PEERS`, ADR 0016) for
/// `connect-src`; no inline script or style, no framing, no plugins.
pub fn csp(peers: &[String]) -> String {
    let mut connect = String::from("'self'");
    for peer in peers {
        connect.push(' ');
        connect.push_str(peer);
    }
    format!(
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; \
connect-src {connect}; font-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; \
frame-ancestors 'none'"
    )
}

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
    /// Cancelled at shutdown: open streams end at once.
    pub stopping: CancellationToken,
    /// Open `/api/v1/stream` connections.
    pub streams: Arc<AtomicUsize>,
    /// The summary every open stream reads (computed once per tick for all of them).
    pub summaries: Arc<SummaryFeed>,
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
/// API when enabled. Unknown paths are 404. The API answers CORS for the peer origins only.
pub fn app(state: HttpState) -> axum::Router {
    let mut app = axum::Router::new().route("/topology", get(get_topology));
    if state.config.dashboard_enabled {
        // Peer origins are validated at startup (no control characters), so the header is valid.
        let csp = HeaderValue::from_str(&csp(&state.config.peers))
            .expect("the CSP is a valid header value");
        let peers: Arc<[String]> = state.config.peers.clone().into();
        let api = axum::Router::new()
            .route("/api/v1/topology", get(api_topology))
            .route("/api/v1/types", get(api_types))
            .route("/api/v1/graph", get(api_graph))
            .route("/api/v1/settings", get(api_settings))
            .route("/api/v1/version", get(api_version))
            .route("/api/v1/stream", get(api_stream))
            .layer(axum::middleware::from_fn_with_state(peers, cors));
        let site = axum::Router::new()
            .route("/", get(assets::to_index))
            .route("/ui", get(assets::to_index))
            .route("/ui/", get(assets::index))
            .route("/ui/v/{build}/{*path}", get(assets::asset))
            .merge(api)
            .layer(axum::middleware::map_response(move |r: Response| {
                let csp = csp.clone();
                async move { security_headers(r, csp) }
            }));
        app = app.merge(site);
    }
    app.with_state(state)
}

/// Headers every dashboard and API response carries.
pub fn security_headers(mut response: Response, csp: HeaderValue) -> Response {
    let h = response.headers_mut();
    let set = |h: &mut axum::http::HeaderMap, name: HeaderName, value: &'static str| {
        h.entry(name)
            .or_insert_with(|| HeaderValue::from_static(value));
    };
    h.entry(header::CONTENT_SECURITY_POLICY).or_insert(csp);
    set(h, header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    set(h, header::REFERRER_POLICY, "no-referrer");
    set(h, header::X_FRAME_OPTIONS, "DENY");
    response
}

/// CORS for `/api/v1/*` (ADR 0016): a request whose `Origin` is exactly one of the peer origins
/// gets it back in `Access-Control-Allow-Origin` (never a wildcard, never credentials), so a
/// peer hub's dashboard can merge this hub in the browser. A preflight (`OPTIONS` with
/// `Access-Control-Request-Method`) is answered 204 for a peer asking for `GET`, else 403. Other
/// origins get no CORS header, so the browser keeps the answer from their scripts.
async fn cors(
    State(peers): State<Arc<[String]>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .filter(|o| peers.iter().any(|p| p == o))
        .map(str::to_string);
    let preflight = request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);
    let mut response = if preflight {
        let wants_get = request
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .is_some_and(|m| m == "GET");
        if origin.is_none() || !wants_get {
            return StatusCode::FORBIDDEN.into_response();
        }
        let mut r = StatusCode::NO_CONTENT.into_response();
        let h = r.headers_mut();
        h.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("accept"),
        );
        h.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("600"),
        );
        r
    } else {
        next.run(request).await
    };
    if !peers.is_empty() {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("origin"));
    }
    if let Some(origin) = origin.and_then(|o| HeaderValue::from_str(&o).ok()) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
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

type SharedSummary = Option<Arc<api::Summary>>;

/// The one producer of the live summary, shared by every open `/api/v1/stream`. While at least
/// one stream is subscribed, a task recomputes the summary every [`STREAM_CHECK`] (topology and
/// O(1) edge totals) and publishes it; with no subscriber the task ends and nothing is computed.
/// So the work under the registry, session and edge locks is per tick, not per stream.
#[derive(Default)]
pub struct SummaryFeed {
    tx: watch::Sender<SharedSummary>,
    /// Whether the producer task runs; taken together with the subscriber check, so a
    /// subscriber either keeps the running task alive or starts a new one.
    running: Mutex<bool>,
    computed: AtomicU64,
}

impl SummaryFeed {
    /// Summaries computed since the hub started.
    pub fn computed(&self) -> u64 {
        self.computed.load(Ordering::Relaxed)
    }

    /// A receiver of the shared summary; starts the producer if it is not running.
    fn subscribe(self: &Arc<Self>, s: &HttpState) -> watch::Receiver<SharedSummary> {
        let rx = self.tx.subscribe();
        let mut running = self.running.lock().unwrap();
        if !*running {
            *running = true;
            tokio::spawn(produce(Arc::clone(self), s.clone()));
        }
        rx
    }
}

async fn produce(feed: Arc<SummaryFeed>, s: HttpState) {
    let mut ticker = tokio::time::interval(STREAM_CHECK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = s.stopping.cancelled() => {}
        }
        {
            let mut running = feed.running.lock().unwrap();
            if s.stopping.is_cancelled() || feed.tx.receiver_count() == 0 {
                // A later subscriber waits for a fresh summary instead of reading this one,
                // however old it gets. Sending also wakes open streams to see the shutdown.
                feed.tx.send_replace(None);
                *running = false;
                return;
            }
        }
        feed.computed.fetch_add(1, Ordering::Relaxed);
        let summary = api::summary(&s.topology(), &s.window_totals(), &s.build_id);
        feed.tx.send_replace(Some(Arc::new(summary)));
    }
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
/// [`STREAM_EVERY`]. Ends at shutdown. Every stream reads the one [`SummaryFeed`].
async fn api_stream(State(s): State<HttpState>) -> Response {
    let Some(slot) = StreamSlot::acquire(&s.streams) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "too many open dashboard streams; poll /api/v1/topology instead",
        )
            .into_response();
    };
    let rx = s.summaries.subscribe(&s);
    let stopping = s.stopping.clone();
    let mut last: Option<(Arc<api::Summary>, Instant)> = None;
    let events = WatchStream::new(rx)
        .map_while(move |summary| {
            let _slot = &slot;
            if stopping.is_cancelled() {
                return None;
            }
            let Some(summary) = summary else {
                return Some(None);
            };
            let due = last.as_ref().is_none_or(|(prev, sent)| {
                sent.elapsed() >= STREAM_EVERY
                    || prev.revision != summary.revision
                    || prev.totals != summary.totals
                    || prev.stale_instances != summary.stale_instances
            });
            if !due {
                return Some(None);
            }
            let event = Event::default().event("summary").json_data(&*summary).ok();
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
    use crate::registry::Registry;
    use crate::telemetry::Metrics;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn state() -> HttpState {
        let config = Arc::new(Config {
            otlp_endpoint: None,
            ..Config::default()
        });
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        let router = Router::new(
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::clone(&config),
            Metrics::new(opentelemetry::global::meter("test")),
        );
        let anchor = ClockAnchor::now();
        HttpState {
            registry,
            sessions,
            router,
            config,
            anchor,
            started: anchor.instant,
            build_id: "test".into(),
            stopping: CancellationToken::new(),
            streams: Arc::default(),
            summaries: Arc::default(),
        }
    }

    fn computed(s: &HttpState) -> u64 {
        s.summaries.computed()
    }

    /// Opens `/api/v1/stream` and waits for its first `summary` event.
    async fn open_stream(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!("GET /api/v1/stream HTTP/1.1\r\nHost: {addr}\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut seen = String::new();
        let mut chunk = [0u8; 4096];
        while !seen.contains("event: summary") {
            let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk))
                .await
                .expect("no summary in time")
                .unwrap();
            assert!(n > 0, "stream closed: {seen}");
            seen.push_str(&String::from_utf8_lossy(&chunk[..n]));
        }
        stream
    }

    #[tokio::test]
    async fn many_streams_share_one_summary_per_tick_and_idle_streams_cost_nothing() {
        let s = state();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(axum::serve(listener, app(s.clone())).into_future());
        let mut streams = Vec::new();
        for _ in 0..8 {
            streams.push(open_stream(addr).await);
        }
        let before = computed(&s);
        tokio::time::sleep(STREAM_CHECK * 6).await;
        let during = computed(&s) - before;
        // One computation per tick for all eight streams (six ticks, with slack for timing);
        // a computation per stream would be about 48.
        assert!((4..=8).contains(&during), "{during} summaries in six ticks");

        // Without subscribers the producer stops: hyper notices the closed connections at the
        // next write (at most STREAM_EVERY), the producer at its next tick.
        drop(streams);
        tokio::time::sleep(STREAM_EVERY + STREAM_CHECK * 3).await;
        let idle = computed(&s);
        tokio::time::sleep(STREAM_CHECK * 4).await;
        assert_eq!(
            computed(&s),
            idle,
            "summaries computed without a subscriber"
        );

        // A new subscriber starts it again.
        let again = open_stream(addr).await;
        assert!(computed(&s) > idle);
        drop(again);
        server.abort();
    }

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
