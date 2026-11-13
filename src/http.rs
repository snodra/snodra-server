//! The HTTP port (`KRAKEN_HUB_HTTP_LISTEN`, default 7001): unless
//! `KRAKEN_HUB_DASHBOARD_ENABLED=false`, the dashboard (`/ui/`, see [`crate::assets`]) and its API
//! under `/api/v1/` (`processors`, ADR 0022; the processor controls under `/api/v1/services/`,
//! ADR 0023); with the dashboard disabled every path is 404. With `KRAKEN_HUB_DASHBOARD_TOKEN`
//! set (ADR 0018), every read under `/api/v1/` (the event stream included) needs
//! `Authorization: Bearer <token>` (the dashboard token, or the write token, which reads too);
//! the page and its assets stay public, because the page is what asks for the token. The mutating control calls need the write token or an ACL credential whose
//! entry has `control = true`, whatever the dashboard token; with neither configured they answer
//! 403 `controls are disabled`.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;

use crate::auth::{self, Authorizer, ControlRight, Secret};
use crate::browser::{Browser, Refusal};
use crate::config::Config;
use crate::context::ContextId;
use crate::control::{
    Action, Answer, AnswerView, LetterCall, LetterPage, RetryAllView, RetryView, Target, Verdict,
};
use crate::edges::{EdgeSnapshot, WindowTotals};
use crate::registry::Registry;
use crate::router::Router;
use crate::session::Sessions;
use crate::topology::{self, ClockAnchor, Topology};
use crate::{api, assets, events, processors};

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
    /// Access control (ADR 0018): an ACL credential with `control = true` may call the
    /// processor controls.
    pub auth: Arc<Authorizer>,
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
    /// The event browser (ADR 0025): off without `KRAKEN_HUB_KAFKA_BOOTSTRAP`.
    pub browser: Arc<Browser>,
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

/// The HTTP routes: the dashboard (`/`, `/ui` redirect to `/ui/`) and its API when enabled.
/// Unknown paths are 404. The API answers CORS for the peer origins only.
pub fn app(state: HttpState) -> axum::Router {
    let guard: Option<Arc<ReadGuard>> = state.config.dashboard_token.clone().map(|read| {
        Arc::new(ReadGuard {
            read,
            write: state.config.dashboard_write_token.clone(),
        })
    });
    let mut app = axum::Router::new();
    if state.config.dashboard_enabled {
        // Peer origins are validated at startup (no control characters), so the header is valid.
        let csp = HeaderValue::from_str(&csp(&state.config.peers))
            .expect("the CSP is a valid header value");
        let peers: Arc<[String]> = state.config.peers.clone().into();
        let mut api = axum::Router::new()
            .route("/api/v1/topology", get(api_topology))
            .route("/api/v1/types", get(api_types))
            .route("/api/v1/graph", get(api_graph))
            .route("/api/v1/settings", get(api_settings))
            .route("/api/v1/version", get(api_version))
            .route("/api/v1/processors", get(api_processors))
            .route(&format!("{PROCESSOR}/dead-letters"), get(api_dead_letters))
            .route("/api/v1/stream", get(api_stream))
            .route("/api/v1/events", get(api_events))
            .route("/api/v1/events/tail", get(api_events_tail))
            .route(
                "/api/v1/events/{topic}/{partition}/{offset}",
                get(api_event),
            )
            .route("/api/v1/consumer-groups", get(api_consumer_groups));
        // The token check sits inside the CORS layer: a peer's preflight carries no
        // `Authorization` header and must be answered, and a 401 must still carry the CORS
        // headers or the peer's page could not tell a wrong token from a network error.
        if let Some(guard) = &guard {
            api = api.layer(axum::middleware::from_fn_with_state(
                Arc::clone(guard),
                require_token,
            ));
        }
        // The mutating control calls (ADR 0023) judge the write credential themselves, so the
        // dashboard token's guard does not apply to them.
        let controls = axum::Router::new()
            .route(&format!("{PROCESSOR}/pause"), post(api_pause))
            .route(&format!("{PROCESSOR}/resume"), post(api_resume))
            .route(&format!("{PROCESSOR}/reset"), post(api_reset))
            .route(
                &format!("{PROCESSOR}/dead-letters/retry-all"),
                post(api_retry_all),
            )
            .route(
                &format!("{PROCESSOR}/dead-letters/{{letter}}/retry"),
                post(api_retry),
            )
            .route(
                &format!("{PROCESSOR}/dead-letters/{{letter}}"),
                delete(api_discard),
            );
        let api = api
            .merge(controls)
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

/// The tokens that read: the dashboard token and, when set, the write token (ADR 0023).
pub struct ReadGuard {
    read: Secret,
    write: Option<Secret>,
}

/// Who made a read, as the audit line of a dead-letter list names it.
#[derive(Debug, Clone, Copy)]
struct Reader(&'static str);

/// The bare 401 of every refused credential: no detail in the body.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer")),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
        ],
    )
        .into_response()
}

fn presented(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(auth::bearer)
}

/// The dashboard token check (ADR 0018). A missing, malformed or wrong token is the same bare
/// 401: no detail in the body, and the same work done (both sides are hashed and compared in
/// constant time), so the answer does not tell which. The write token reads too.
async fn require_token(
    State(guard): State<Arc<ReadGuard>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let presented = presented(request.headers());
    let read = guard.read.matches(presented);
    let write = guard.write.as_ref().is_some_and(|w| w.matches(presented));
    if !(read || write) {
        return unauthorized();
    }
    let reader = if write {
        "write-token"
    } else {
        "dashboard-token"
    };
    request.extensions_mut().insert(Reader(reader));
    next.run(request).await
}

/// CORS for `/api/v1/*` (ADR 0016): a request whose `Origin` is exactly one of the peer origins
/// gets it back in `Access-Control-Allow-Origin` (never a wildcard, never credentials), so a
/// peer hub's dashboard can merge this hub in the browser. A preflight (`OPTIONS` with
/// `Access-Control-Request-Method`) is answered 204 for a peer asking for `GET`, else 403. Other
/// origins get no CORS header, so the browser keeps the answer from their scripts. `POST` and
/// `DELETE` (the processor controls, ADR 0023) are allowed like `GET`: the page sends its control
/// calls to the hub that served it, but a peer's page or a tool in a peer's origin may too.
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
        let allowed = request
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .is_some_and(|m| m == "GET" || m == "POST" || m == "DELETE");
        if origin.is_none() || !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
        let mut r = StatusCode::NO_CONTENT.into_response();
        let h = r.headers_mut();
        h.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, DELETE"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            // `authorization`: the page forwards the dashboard token to its peers (ADR 0018);
            // `content-type`: a reset's JSON body (ADR 0023).
            HeaderValue::from_static("accept, authorization, content-type"),
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

/// The event processors of every service, aggregated over its instances (ADR 0022).
async fn api_processors(State(s): State<HttpState>) -> Response {
    fresh_json(processors::view(&s.topology()))
}

// ---------------------------------------------------------------------------------------------
// Event browser (ADR 0025)

/// The answer to a browser call that has no data, counted under `kind`.
fn refused(s: &HttpState, kind: &'static str, refusal: Refusal) -> Response {
    s.browser.count(kind, refusal.outcome());
    match refusal {
        Refusal::Disabled => error_json(StatusCode::NOT_FOUND, crate::browser::DISABLED),
        Refusal::BadRequest(why) => error_json(StatusCode::BAD_REQUEST, &why),
        Refusal::NotFound(why) => error_json(StatusCode::NOT_FOUND, &why),
        Refusal::Unavailable(why) => error_json(StatusCode::SERVICE_UNAVAILABLE, &why),
        Refusal::Forbidden(hidden) => answer_json(
            StatusCode::FORBIDDEN,
            &serde_json::json!({ "error": hidden.reason(), "hidden": hidden.code() }),
        ),
        Refusal::TooMany(why) => {
            let mut r = error_json(StatusCode::TOO_MANY_REQUESTS, &why);
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            r
        }
    }
}

/// The topic, the filters and, for a payload filter (`q`), the caller who may use it.
fn browse(
    s: &HttpState,
    query: &events::SearchQuery,
    headers: &HeaderMap,
    tail: bool,
) -> Result<(String, events::Filters), Refusal> {
    let topic = s.browser.topic(query.topic.as_deref())?;
    let filters = events::Filters::parse(query, tail).map_err(Refusal::BadRequest)?;
    if filters.q.is_some() {
        let caller =
            events::payload_access(&s.config, presented(headers)).map_err(Refusal::Forbidden)?;
        tracing::info!(caller, topic = %topic, tail, "event payload search");
    }
    Ok((topic, filters))
}

/// `GET /api/v1/events`: one page of a search, newest first (see [`crate::browser`]).
async fn api_events(
    State(s): State<HttpState>,
    Query(query): Query<events::SearchQuery>,
    headers: HeaderMap,
) -> Response {
    const KIND: &str = "search";
    let page = async {
        let (topic, filters) = browse(&s, &query, &headers, false)?;
        let limit = events::parse_limit(query.limit.as_deref(), s.config.event_search_max_results)
            .map_err(Refusal::BadRequest)?;
        let cursor = query
            .cursor
            .as_deref()
            .filter(|c| !c.is_empty())
            .map(|c| events::Cursor::decode(c, filters.digest(&topic)))
            .transpose()
            .map_err(Refusal::BadRequest)?;
        s.browser.search(topic, filters, limit, cursor).await
    };
    match page.await {
        Ok(view) => {
            s.browser.count(KIND, "ok");
            fresh_json(view)
        }
        Err(refusal) => refused(&s, KIND, refusal),
    }
}

/// `GET /api/v1/events/{topic}/{partition}/{offset}`: one record, with every header and, under
/// the payload rules (ADR 0025), its payload, each served payload audited.
async fn api_event(
    State(s): State<HttpState>,
    Path((topic, partition, offset)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    const KIND: &str = "record";
    let access = events::payload_access(&s.config, presented(&headers));
    let detail = async {
        let topic = s.browser.topic(Some(&topic))?;
        let partition = partition.parse::<i32>().ok().filter(|p| *p >= 0);
        let offset = offset.parse::<i64>().ok().filter(|o| *o >= 0);
        let (Some(partition), Some(offset)) = (partition, offset) else {
            return Err(Refusal::BadRequest(
                "the partition and the offset are non-negative numbers".to_string(),
            ));
        };
        s.browser
            .record(topic, partition, offset, access.map(|_| ()))
            .await
    };
    match detail.await {
        Ok(detail) => {
            if let Ok(caller) = access
                && (detail.payload_base64.is_some() || matches!(detail.payload, Some(Some(_))))
            {
                let r = &detail.summary;
                s.browser
                    .payload_served(caller, &r.topic, r.partition, r.offset, r.size);
            }
            s.browser.count(KIND, "ok");
            fresh_json(detail)
        }
        Err(refusal) => refused(&s, KIND, refusal),
    }
}

/// How often a tail sends `event: heartbeat`.
pub const TAIL_HEARTBEAT: Duration = Duration::from_secs(15);

/// `GET /api/v1/events/tail`: server-sent events from the end of every partition: `start`, then a
/// `record` (a summary, never a payload) per matching record, `heartbeat` every
/// [`TAIL_HEARTBEAT`], and `end` with the reason (`maxDuration`, `slowReader`, `shutdown`).
async fn api_events_tail(
    State(s): State<HttpState>,
    Query(query): Query<events::SearchQuery>,
    headers: HeaderMap,
) -> Response {
    const KIND: &str = "tail";
    let tail = async {
        let (topic, filters) = browse(&s, &query, &headers, true)?;
        let tail = s
            .browser
            .tail(topic.clone(), filters, s.stopping.clone())
            .await?;
        Ok::<_, Refusal>((topic, tail))
    };
    let (topic, mut tail) = match tail.await {
        Ok(started) => started,
        Err(refusal) => return refused(&s, KIND, refusal),
    };
    s.browser.count(KIND, "ok");
    let (out, events) = tokio::sync::mpsc::channel::<Event>(16);
    let start = serde_json::json!({
        "topic": topic,
        "partitions": tail.partitions,
        "endsAtMs": tail.ends_at_ms,
    });
    tokio::spawn(async move {
        if out
            .send(Event::default().event("start").data(start.to_string()))
            .await
            .is_err()
        {
            return;
        }
        let mut heartbeat = tokio::time::interval(TAIL_HEARTBEAT);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        loop {
            let event = tokio::select! {
                biased;
                _ = out.closed() => return,
                reason = &mut tail.end => {
                    // Without a reason the tail stopped because this reader left.
                    let Ok(reason) = reason else { return };
                    let end = serde_json::json!({ "reason": reason });
                    let _ = out.send(Event::default().event("end").data(end.to_string())).await;
                    return;
                }
                record = tail.records.recv() => match record {
                    Some(summary) => match Event::default().event("record").json_data(&summary) {
                        Ok(event) => event,
                        Err(_) => continue,
                    },
                    // The Kafka side ended; its reason follows on `end`.
                    None => continue,
                },
                _ = heartbeat.tick() => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis());
                    Event::default().event("heartbeat").data(format!("{{\"nowMs\":{now}}}"))
                }
            };
            if out.send(event).await.is_err() {
                return;
            }
        }
    });
    let stream = tokio_stream::wrappers::ReceiverStream::new(events).map(Ok::<_, Infallible>);
    let mut response = Sse::new(stream).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

#[derive(Deserialize)]
struct GroupsQuery {
    topic: Option<String>,
}

/// `GET /api/v1/consumer-groups?topic=`: every consumer group with committed offsets on the
/// topic and its lag, members or not.
async fn api_consumer_groups(
    State(s): State<HttpState>,
    Query(query): Query<GroupsQuery>,
) -> Response {
    const KIND: &str = "groups";
    let groups = async {
        let topic = s.browser.topic(query.topic.as_deref())?;
        s.browser.groups(topic).await
    };
    match groups.await {
        Ok(view) => {
            s.browser.count(KIND, "ok");
            fresh_json(view)
        }
        Err(refusal) => refused(&s, KIND, refusal),
    }
}

// ---------------------------------------------------------------------------------------------
// Processor control (ADR 0023)

/// Every control route starts here.
const PROCESSOR: &str = "/api/v1/services/{service}/processors/{processor}";
/// The largest body a control call but a reset takes.
pub const MAX_CONTROL_BODY: usize = 4096;
/// The largest body a reset takes: room for [`crate::control::MAX_POSITIONS`] positions with
/// decimal segments and positions (a 1024-segment source with 20-digit positions is about 54 KiB).
pub const MAX_RESET_BODY: usize = 64 * 1024;

/// The status code of a verdict.
fn status_of(verdict: Verdict) -> StatusCode {
    match verdict {
        Verdict::Ok => StatusCode::OK,
        Verdict::BadRequest => StatusCode::BAD_REQUEST,
        Verdict::NotFound => StatusCode::NOT_FOUND,
        Verdict::Conflict => StatusCode::CONFLICT,
        Verdict::Failed => StatusCode::BAD_GATEWAY,
        Verdict::Timeout => StatusCode::GATEWAY_TIMEOUT,
    }
}

/// The body of a call that failed: why, and the answer of the instance when one decided it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody<'a> {
    error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_id: Option<&'a str>,
    #[serde(flatten)]
    answer: Option<AnswerView>,
}

fn answer_json<T: Serialize>(status: StatusCode, value: &T) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(value),
    )
        .into_response()
}

fn error_json(status: StatusCode, error: &str) -> Response {
    answer_json(
        status,
        &ErrorBody {
            error,
            instance_id: None,
            answer: None,
        },
    )
}

/// The answer to a call the hub refused before asking any instance: why, and the refusal code
/// when it has one (`UNSUPPORTED`), shaped like an instance's refusal.
fn refusal_json(refusal: &crate::control::Refusal, service: &str, processor: &str) -> Response {
    let message = refusal.message(service, processor);
    answer_json(
        status_of(refusal.verdict()),
        &ErrorBody {
            error: &message,
            instance_id: None,
            answer: refusal.code().map(|code| AnswerView {
                result: "refused",
                code: Some(code),
                message: message.clone(),
            }),
        },
    )
}

/// Runs a control call to the end even when the caller hangs up (a reset half done would hold
/// every instance until the lease ran out), then answers.
async fn detached(call: impl Future<Output = Response> + Send + 'static) -> Response {
    tokio::spawn(call).await.unwrap_or_else(|_| {
        error_json(StatusCode::INTERNAL_SERVER_ERROR, "the control call failed")
    })
}

/// Judges the write credential of a mutating control call and reads its body (at most
/// [`MAX_RESET_BODY`] bytes for a reset, else [`MAX_CONTROL_BODY`]). `Ok` carries the caller as the audit line names it: `write-token`
/// or the service of the ACL entry. A refusal is counted, never logged.
async fn admit_control(
    s: &HttpState,
    action: Action,
    headers: &HeaderMap,
    body: Body,
) -> Result<(String, Bytes), Box<Response>> {
    let control = s.router.control();
    let write = s.config.dashboard_write_token.as_ref();
    if write.is_none() && !s.auth.controls_configured() {
        control.refused(action, "disabled");
        return Err(Box::new(error_json(
            StatusCode::FORBIDDEN,
            "controls are disabled",
        )));
    }
    let presented = presented(headers);
    let write_ok = write.is_some_and(|w| w.matches(presented));
    let caller = if write_ok {
        "write-token".to_string()
    } else {
        let Some(token) = presented else {
            control.refused(action, "unauthenticated");
            return Err(Box::new(unauthorized()));
        };
        // A service's JWT is judged like at Register; its rotated key is fetched first.
        s.auth.prepare_bearer(Some(token)).await;
        match s.auth.control_right(token) {
            ControlRight::Granted(service) => service,
            ControlRight::Denied(_) => {
                control.refused(action, "forbidden");
                return Err(Box::new(error_json(
                    StatusCode::FORBIDDEN,
                    "this credential may not control processors (its ACL entry has no control = true)",
                )));
            }
            ControlRight::Unknown => {
                let read = s.config.dashboard_token.as_ref();
                if read.is_some_and(|r| r.matches(Some(token))) {
                    control.refused(action, "forbidden");
                    return Err(Box::new(error_json(
                        StatusCode::FORBIDDEN,
                        "the dashboard token is read-only; controls need KRAKEN_HUB_DASHBOARD_WRITE_TOKEN or an ACL credential with control = true",
                    )));
                }
                control.refused(action, "unauthenticated");
                return Err(Box::new(unauthorized()));
            }
        }
    };
    let limit = if action == Action::Reset {
        MAX_RESET_BODY
    } else {
        MAX_CONTROL_BODY
    };
    match axum::body::to_bytes(body, limit).await {
        Ok(bytes) => Ok((caller, bytes)),
        Err(_) => {
            control.refused(action, Verdict::BadRequest.as_str());
            Err(Box::new(error_json(
                StatusCode::BAD_REQUEST,
                &format!("the body is larger than {limit} bytes, or unreadable"),
            )))
        }
    }
}

async fn api_pause(
    State(s): State<HttpState>,
    Path((service, processor)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    fan_out(s, Action::Pause, service, processor, headers, body).await
}

async fn api_resume(
    State(s): State<HttpState>,
    Path((service, processor)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    fan_out(s, Action::Resume, service, processor, headers, body).await
}

/// Pause or resume on every instance that runs the processor: 200 with the counts when
/// at least one persisted it.
async fn fan_out(
    s: HttpState,
    action: Action,
    service: String,
    processor: String,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let started = Instant::now();
    let (caller, _) = match admit_control(&s, action, &headers, body).await {
        Ok(admitted) => admitted,
        Err(refused) => return *refused,
    };
    detached(async move {
        let control = Arc::clone(s.router.control());
        let paused = action == Action::Pause;
        let (verdict, response) = match control
            .pause(&ContextId::default(), &service, &processor, paused)
            .await
        {
            Ok(report) => (
                report.verdict,
                answer_json(status_of(report.verdict), &report),
            ),
            Err(refusal) => (
                refusal.verdict(),
                refusal_json(&refusal, &service, &processor),
            ),
        };
        control.audit(
            &caller,
            &service,
            &processor,
            action,
            verdict.as_str(),
            started,
            None,
        );
        response
    })
    .await
}

/// A reset to the body's target, as a leased operation (see [`crate::control::Control::reset`]).
async fn api_reset(
    State(s): State<HttpState>,
    Path((service, processor)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let started = Instant::now();
    let (caller, body) = match admit_control(&s, Action::Reset, &headers, body).await {
        Ok(admitted) => admitted,
        Err(refused) => return *refused,
    };
    let control = Arc::clone(s.router.control());
    let target = match Target::parse(&body) {
        Ok(target) => target,
        Err(why) => {
            let outcome = Verdict::BadRequest.as_str();
            control.audit(
                &caller,
                &service,
                &processor,
                Action::Reset,
                outcome,
                started,
                None,
            );
            return error_json(StatusCode::BAD_REQUEST, &why);
        }
    };
    detached(async move {
        let (verdict, operation, response) = match control
            .reset(&ContextId::default(), &service, &processor, target)
            .await
        {
            Ok(report) => (
                report.verdict,
                Some(report.operation_id.clone()),
                answer_json(status_of(report.verdict), &report),
            ),
            Err(refusal) => (
                refusal.verdict(),
                None,
                refusal_json(&refusal, &service, &processor),
            ),
        };
        let outcome = verdict.as_str();
        control.audit(
            &caller,
            &service,
            &processor,
            Action::Reset,
            outcome,
            started,
            operation.as_deref(),
        );
        response
    })
    .await
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<String>,
}

/// The answer of a dead-letter call that an instance answered.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LetterReport<'a> {
    service: &'a str,
    processor: &'a str,
    instance_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    letter_id: Option<&'a str>,
    #[serde(flatten)]
    body: LetterBody,
}

#[derive(Serialize)]
#[serde(untagged)]
enum LetterBody {
    Page(LetterPage),
    Retry(RetryView),
    RetryAll(RetryAllView),
    Discarded { discarded: bool },
}

/// Why an instance's answer is a failure, for the caller.
fn describe(answer: &Answer) -> String {
    match answer {
        Answer::Refused { message, .. } => format!("the instance refused: {message}"),
        Answer::Timeout => "the instance did not answer in time".to_string(),
        Answer::Left => "the instance left before it answered".to_string(),
        Answer::Unsent(why) => format!("the request could not be sent: {why}"),
        _ => "the instance sent an answer of another kind".to_string(),
    }
}

/// A dead-letter call on the instance with the freshest status.
async fn letters(
    s: HttpState,
    caller: String,
    service: String,
    processor: String,
    call: LetterCall,
    started: Instant,
) -> Response {
    detached(async move {
        let control = Arc::clone(s.router.control());
        let action = call.action();
        let (verdict, response) = match control
            .dead_letters(&ContextId::default(), &service, &processor, &call)
            .await
        {
            Err(refusal) => (
                refusal.verdict(),
                refusal_json(&refusal, &service, &processor),
            ),
            Ok(outcome) => {
                let verdict = outcome.answer.verdict();
                let letter_id = match &call {
                    LetterCall::Retry(id) | LetterCall::Discard(id) => Some(id.as_str()),
                    _ => None,
                };
                let body = match (&outcome.answer, &call) {
                    (Answer::Letters(page), _) => Some(LetterBody::Page(page.clone())),
                    (Answer::Retry(r), _) => Some(LetterBody::Retry(r.clone())),
                    (Answer::RetryAll(r), _) => Some(LetterBody::RetryAll(r.clone())),
                    (Answer::Ok(_), LetterCall::Discard(_)) => {
                        Some(LetterBody::Discarded { discarded: true })
                    }
                    _ => None,
                };
                let response = match body {
                    Some(body) => answer_json(
                        status_of(verdict),
                        &LetterReport {
                            service: &service,
                            processor: &processor,
                            instance_id: &outcome.instance_id,
                            letter_id,
                            body,
                        },
                    ),
                    None => answer_json(
                        status_of(verdict),
                        &ErrorBody {
                            error: &describe(&outcome.answer),
                            instance_id: Some(&outcome.instance_id),
                            answer: Some(outcome.answer.view()),
                        },
                    ),
                };
                (verdict, response)
            }
        };
        control.audit(
            &caller,
            &service,
            &processor,
            action,
            verdict.as_str(),
            started,
            None,
        );
        response
    })
    .await
}

/// True for a letter id an instance could hold: 1 to 128 bytes, no control characters.
fn valid_letter_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= crate::control::MAX_ID_BYTES && !id.chars().any(char::is_control)
}

/// `GET .../dead-letters?limit=n`: a read (the dashboard token, or the write token).
async fn api_dead_letters(
    State(s): State<HttpState>,
    Path((service, processor)): Path<(String, String)>,
    Query(query): Query<LimitQuery>,
    reader: Option<axum::Extension<Reader>>,
) -> Response {
    let started = Instant::now();
    let caller = reader.map_or("anonymous", |r| r.0.0).to_string();
    let limit = match query.limit.as_deref().map(str::parse::<u32>) {
        None => crate::control::DEFAULT_LETTERS,
        Some(Ok(n)) => n.clamp(1, crate::control::MAX_LETTERS),
        Some(Err(_)) => {
            let outcome = Verdict::BadRequest.as_str();
            s.router.control().audit(
                &caller,
                &service,
                &processor,
                Action::DeadLetterList,
                outcome,
                started,
                None,
            );
            return error_json(
                StatusCode::BAD_REQUEST,
                "limit must be a number from 1 to 100",
            );
        }
    };
    letters(
        s,
        caller,
        service,
        processor,
        LetterCall::List(limit),
        started,
    )
    .await
}

async fn api_retry(
    State(s): State<HttpState>,
    Path((service, processor, letter)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    letter_call(
        s,
        service,
        processor,
        LetterCall::Retry(letter),
        headers,
        body,
    )
    .await
}

async fn api_retry_all(
    State(s): State<HttpState>,
    Path((service, processor)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    letter_call(s, service, processor, LetterCall::RetryAll, headers, body).await
}

async fn api_discard(
    State(s): State<HttpState>,
    Path((service, processor, letter)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    letter_call(
        s,
        service,
        processor,
        LetterCall::Discard(letter),
        headers,
        body,
    )
    .await
}

/// A mutating dead-letter call: the write credential, the letter id, then the instance.
async fn letter_call(
    s: HttpState,
    service: String,
    processor: String,
    call: LetterCall,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let started = Instant::now();
    let action = call.action();
    let (caller, _) = match admit_control(&s, action, &headers, body).await {
        Ok(admitted) => admitted,
        Err(refused) => return *refused,
    };
    if let LetterCall::Retry(id) | LetterCall::Discard(id) = &call
        && !valid_letter_id(id)
    {
        let outcome = Verdict::BadRequest.as_str();
        s.router.control().audit(
            &caller, &service, &processor, action, outcome, started, None,
        );
        return error_json(
            StatusCode::BAD_REQUEST,
            "a letter id is 1 to 128 bytes without control characters",
        );
    }
    letters(s, caller, service, processor, call, started).await
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
        let browser = Browser::new(Arc::clone(&config), Metrics::noop());
        HttpState {
            registry,
            sessions,
            router,
            config,
            auth: Authorizer::off(Metrics::new(opentelemetry::global::meter("test"))),
            anchor,
            started: anchor.instant,
            build_id: "test".into(),
            stopping: CancellationToken::new(),
            streams: Arc::default(),
            summaries: Arc::default(),
            browser,
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

    /// The token check goes through the constant-time comparison (a missing, a wrong and a right
    /// token each cost exactly one), not a `==` on strings.
    #[tokio::test]
    async fn the_dashboard_token_check_uses_the_constant_time_compare() {
        use tower::ServiceExt;
        let mut s = state();
        s.config = Arc::new(Config {
            otlp_endpoint: None,
            dashboard_token: Some(Secret::new("a-dashboard-token-0123456789")),
            ..Config::default()
        });
        let app = app(s);
        let get = |auth: Option<&str>| {
            let mut r = axum::http::Request::builder().uri("/api/v1/topology");
            if let Some(a) = auth {
                r = r.header(header::AUTHORIZATION, a);
            }
            r.body(axum::body::Body::empty()).unwrap()
        };
        let before = auth::ct::COMPARISONS.with(|c| c.get());
        let status = |r: Response| r.status();
        assert_eq!(
            status(app.clone().oneshot(get(None)).await.unwrap()),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(
                app.clone()
                    .oneshot(get(Some("Bearer wrong")))
                    .await
                    .unwrap()
            ),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(
                app.clone()
                    .oneshot(get(Some("Bearer a-dashboard-token-0123456789")))
                    .await
                    .unwrap()
            ),
            StatusCode::OK
        );
        assert_eq!(auth::ct::COMPARISONS.with(|c| c.get()) - before, 3);
    }

    /// One audit line per control call that got past the credential check, naming the caller,
    /// the call and its outcome; none for a refused credential (it is only counted).
    #[tokio::test]
    async fn a_control_call_writes_one_audit_line() {
        use crate::proto::{
            ControlOk, ControlReply, InstanceStatus, ProcessorStatus, control_reply, server_message,
        };
        use crate::session::SessionMeta;
        use tower::ServiceExt;
        crate::telemetry::test_support::capture_logs();
        let mut s = state();
        s.config = Arc::new(Config {
            otlp_endpoint: None,
            dashboard_write_token: Some(Secret::new("write-0123456789abcdef")),
            ..Config::default()
        });
        let ctx = ContextId::default();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let meta = SessionMeta::default();
        let token = s
            .sessions
            .insert(&ctx, "b-1", "svc", meta, tx, CancellationToken::new());
        let report = InstanceStatus {
            connected_hub_ids: vec![],
            handlers_in_flight: 0,
            processors: vec![ProcessorStatus {
                name: "audit".into(),
                ..Default::default()
            }],
        };
        assert!(
            s.sessions
                .set_status(&ctx, "b-1", token, &report, Instant::now())
        );
        let control = Arc::clone(s.router.control());
        tokio::spawn(async move {
            while let Some(Ok(msg)) = rx.recv().await {
                if let Some(server_message::Message::Control(req)) = msg.message {
                    let result = control_reply::Result::Ok(ControlOk::default());
                    let reply = ControlReply {
                        control_id: req.control_id,
                        result: Some(result),
                    };
                    control.on_reply(&ContextId::default(), "b-1", token, reply);
                }
            }
        });
        let app = app(s);
        let pause = |auth: &str| {
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/v1/services/svc/processors/audit/pause")
                .header(header::AUTHORIZATION, auth)
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let r = app
            .clone()
            .oneshot(pause("Bearer nope-nope-nope"))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        let r = app
            .clone()
            .oneshot(pause("Bearer write-0123456789abcdef"))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let lines: Vec<_> = crate::telemetry::test_support::logged()
            .into_iter()
            .filter(|l| l.message == "control")
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        for (field, value) in [
            ("caller", "write-token"),
            ("service", "svc"),
            ("processor", "audit"),
            ("action", "pause"),
            ("outcome", "ok"),
        ] {
            assert_eq!(lines[0].field(field), Some(value), "{field}");
        }
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

    /// The event browser's instruments: every call by kind and outcome, the records a search
    /// read, the open tails, and one payload view (with its audit line) per payload served.
    #[tokio::test]
    async fn the_event_browser_counts_calls_scans_tails_and_payload_views() {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use testcontainers::runners::AsyncRunner;
        use tower::ServiceExt;
        crate::telemetry::test_support::capture_logs();
        let kafka = testcontainers_modules::kafka::apache::Kafka::default()
            .start()
            .await
            .expect("kafka container");
        let port = kafka
            .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
            .await
            .unwrap();
        let bootstrap = format!("127.0.0.1:{port}");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .unwrap();
        for i in 0..3 {
            producer
                .send(
                    FutureRecord::<str, str>::to("kraken.events")
                        .key("k")
                        .payload(&format!("{{\"n\":{i}}}")),
                    Duration::from_secs(20),
                )
                .await
                .unwrap();
        }
        let metrics = crate::telemetry::test_support::test_metrics();
        let mut s = state();
        s.config = Arc::new(Config {
            otlp_endpoint: None,
            kafka_bootstrap: Some(bootstrap),
            event_payloads: true,
            dashboard_token: Some(Secret::new("a-dashboard-token-0123456789")),
            ..Config::default()
        });
        s.browser = Browser::new(Arc::clone(&s.config), Arc::clone(&metrics.metrics));
        let app = app(s);
        let get = |uri: &str| {
            axum::http::Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer a-dashboard-token-0123456789")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let r = app.clone().oneshot(get("/api/v1/events")).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let r = app
            .clone()
            .oneshot(get("/api/v1/events?topic=other"))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        let r = app
            .clone()
            .oneshot(get("/api/v1/events/kraken.events/0/1"))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("{\\\"n\\\":1}"));

        let tail = app
            .clone()
            .oneshot(get("/api/v1/events/tail"))
            .await
            .unwrap();
        assert_eq!(tail.status(), StatusCode::OK);
        assert_eq!(
            metrics.up_down_total("kraken_hub_event_tails"),
            Some(1),
            "a tail reading Kafka"
        );
        drop(tail);
        // The tail notices its reader left at its next poll.
        let deadline = Instant::now() + Duration::from_secs(10);
        while metrics.up_down_total("kraken_hub_event_tails") != Some(0) {
            assert!(Instant::now() < deadline, "the tail did not end");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let count = |kind: &str, outcome: &str| {
            let total = metrics
                .counter("kraken_hub_event_browser_requests_total", "kind", kind)
                .unwrap_or(0);
            let by_outcome = metrics
                .counter(
                    "kraken_hub_event_browser_requests_total",
                    "outcome",
                    outcome,
                )
                .unwrap_or(0);
            (total, by_outcome)
        };
        assert_eq!(count("search", "bad_request"), (2, 1));
        assert_eq!(count("record", "ok").0, 1);
        assert_eq!(count("tail", "ok").0, 1);
        assert_eq!(
            metrics.histogram_sum("kraken_hub_event_browser_scanned_records"),
            Some((1, 3.0)),
            "one search that read the three records"
        );
        assert_eq!(
            metrics.counter_total("kraken_hub_event_payload_views_total"),
            Some(1)
        );
        let audits: Vec<_> = crate::telemetry::test_support::logged()
            .into_iter()
            .filter(|l| l.message == "event payload served")
            .collect();
        assert_eq!(audits.len(), 1, "{audits:?}");
        for (field, value) in [
            ("caller", "dashboard-token"),
            ("topic", "kraken.events"),
            ("partition", "0"),
            ("offset", "1"),
            ("size", "7"),
        ] {
            assert_eq!(audits[0].field(field), Some(value), "{field}");
        }
        drop(kafka);
    }
}
