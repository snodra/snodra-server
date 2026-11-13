//! Processor control, hub side (ADR 0023): the HTTP API asks the instances that report a processor
//! to pause, resume, reset (prepare all, execute on one, finish all) and to work on their dead
//! letters, over real gRPC to fake instances; the write credential guards every mutation.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::control::{
    ControlClient, Reaction, action_of, actions, default_result, ok, ok_to_everything, refused,
};
use common::{HttpResponse, TempDir, http_send, start_hub_with_http};
use kraken_hub::auth::{AuthMode, Secret, hash_token};
use kraken_hub::config::Config;
use kraken_hub::proto::{ControlRefusal, ControlRequest, control_reply, control_request};
use serde_json::{Value, json};

const WRITE: &str = "write-token-0123456789abcdef";
const READ: &str = "read-token-0123456789abcdef";
const PAUSE: &str = "/api/v1/services/svc/processors/audit/pause";
const RESUME: &str = "/api/v1/services/svc/processors/audit/resume";
const RESET: &str = "/api/v1/services/svc/processors/audit/reset";
const LETTERS: &str = "/api/v1/services/svc/processors/audit/dead-letters";
const LONG: Duration = Duration::from_secs(20);

fn config() -> Config {
    Config {
        hub_id: "hub-a".to_string(),
        otlp_endpoint: None,
        dashboard_write_token: Some(Secret::new(WRITE)),
        ..Config::default()
    }
}

async fn hub(config: Config) -> (SocketAddr, SocketAddr) {
    let (grpc, http) = start_hub_with_http(config).await;
    (grpc, http.unwrap())
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// `method path` with `token` as the bearer and `body`.
async fn call(
    http: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> HttpResponse {
    let auth = token.map(bearer);
    let mut headers: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
    if let Some(auth) = &auth {
        headers.push(("Authorization", auth));
    }
    http_send(http, method, path, &headers, body.as_bytes(), LONG).await
}

async fn post(http: SocketAddr, path: &str, body: &str) -> HttpResponse {
    call(http, "POST", path, Some(WRITE), body).await
}

fn json_of(r: &HttpResponse) -> Value {
    serde_json::from_str(&r.body).unwrap_or_else(|e| panic!("{e}: {} {}", r.status, r.body))
}

/// A behaviour that answers `ok_to_everything`, except what `f` decides.
fn answering(
    f: impl Fn(&ControlRequest) -> Option<Reaction> + Send + Sync + 'static,
) -> common::control::Behaviour {
    Arc::new(move |req: &ControlRequest| {
        f(req).unwrap_or_else(|| Reaction::Reply(default_result(req)))
    })
}

fn is(req: &ControlRequest, action: &str) -> bool {
    action_of(req) == action
}

fn operation_of(req: &ControlRequest) -> String {
    use control_request::Action as A;
    match &req.action {
        Some(A::ResetPrepare(p)) => p.operation_id.clone(),
        Some(A::ResetExecute(e)) => e.operation_id.clone(),
        Some(A::ResetFinish(f)) => f.operation_id.clone(),
        other => panic!("not a reset step: {other:?}"),
    }
}

#[tokio::test]
async fn a_pause_reaches_the_sessions_that_report_the_processor() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(
        grpc,
        "svc",
        "b-1",
        &["audit", "default"],
        ok_to_everything(),
    )
    .await;
    let b2 = ControlClient::new(grpc, "svc", "b-2", &["default"], ok_to_everything()).await;
    let c1 = ControlClient::new(grpc, "other", "c-1", &["audit"], ok_to_everything()).await;

    let r = post(http, PAUSE, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        json_of(&r),
        json!({
            "service": "svc", "processor": "audit", "action": "pause",
            "acknowledged": 1, "failed": 0,
            "instances": [{ "instanceId": "b-1", "result": "ok", "code": null, "message": "" }],
        })
    );
    let seen = b1.seen();
    assert_eq!(actions(&seen), ["pause"]);
    assert_eq!(seen[0].processor, "audit");
    assert!(!seen[0].control_id.is_empty());
    for other in [&b2, &c1] {
        assert!(other.seen().is_empty(), "{} was asked", other.instance_id);
    }

    let r = post(http, RESUME, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(actions(&b1.seen()), ["pause", "resume"]);
    assert_ne!(
        b1.seen()[0].control_id,
        b1.seen()[1].control_id,
        "one id per request"
    );
}

#[tokio::test]
async fn a_pause_is_200_when_one_instance_persisted_it_and_502_or_504_when_none_did() {
    let (grpc, http) = hub(Config {
        control_timeout: Duration::from_millis(400),
        ..config()
    })
    .await;
    let _b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    let _b2 = ControlClient::new(
        grpc,
        "svc",
        "b-2",
        &["audit"],
        answering(|req| {
            (is(req, "pause") || is(req, "resume"))
                .then(|| Reaction::Reply(refused(ControlRefusal::Failed, "mongo is down")))
        }),
    )
    .await;
    let r = post(http, PAUSE, "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(
        (body["acknowledged"].clone(), body["failed"].clone()),
        (json!(1), json!(1))
    );
    assert_eq!(
        body["instances"][1],
        json!({ "instanceId": "b-2", "result": "refused", "code": "FAILED", "message": "mongo is down" })
    );
    drop(_b1);

    let (grpc, http) = hub(Config {
        control_timeout: Duration::from_millis(400),
        ..config()
    })
    .await;
    let _f = ControlClient::new(
        grpc,
        "svc",
        "f-1",
        &["audit"],
        answering(|_| Some(Reaction::Reply(refused(ControlRefusal::Failed, "no")))),
    )
    .await;
    let _s = ControlClient::new(
        grpc,
        "svc",
        "s-1",
        &["audit"],
        answering(|_| Some(Reaction::Silent)),
    )
    .await;
    let r = post(http, PAUSE, "").await;
    assert_eq!(r.status, 502, "nobody persisted it: {}", r.body);
    assert_eq!(json_of(&r)["instances"][1]["result"], "timeout");
    drop(_f);
    // Every instance silent: a timeout.
    let started = std::time::Instant::now();
    let r = post(http, PAUSE, "").await;
    assert_eq!(r.status, 504, "{}", r.body);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "bounded by the control timeout"
    );
}

#[tokio::test]
async fn unknown_services_and_processors_are_404() {
    let (grpc, http) = hub(config()).await;
    let _b = ControlClient::new(grpc, "svc", "b-1", &["default"], ok_to_everything()).await;
    for path in [
        "/api/v1/services/ghost/processors/audit/pause",
        "/api/v1/services/svc/processors/audit/pause",
        "/api/v1/services/svc/processors/audit/dead-letters/retry-all",
    ] {
        let r = post(http, path, "").await;
        assert_eq!(r.status, 404, "{path}: {}", r.body);
        assert!(
            json_of(&r)["error"]
                .as_str()
                .unwrap()
                .contains("no instance"),
            "{}",
            r.body
        );
    }
    let r = call(
        http,
        "GET",
        "/api/v1/services/svc/processors/audit/dead-letters",
        Some(WRITE),
        "",
    )
    .await;
    assert_eq!(r.status, 404);
}

#[tokio::test]
async fn a_reset_prepares_every_instance_executes_on_the_freshest_and_finishes_all() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    let b2 = ControlClient::new(grpc, "svc", "b-2", &["audit"], ok_to_everything()).await;
    // Reports it, but does not run it (any more) when asked: not involved.
    let b3 = ControlClient::new(
        grpc,
        "svc",
        "b-3",
        &["audit"],
        answering(|req| {
            is(req, "prepare").then(|| {
                Reaction::Reply(refused(ControlRefusal::UnknownProcessor, "no audit here"))
            })
        }),
    )
    .await;
    // b-2 reports again: its status is the freshest.
    tokio::time::sleep(Duration::from_millis(20)).await;
    b2.report(&["audit"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let body = json_of(&r);
    let op = body["operationId"].as_str().unwrap().to_string();
    assert_eq!(op.len(), 36, "a UUID");
    assert_eq!(body["outcome"], "ok");
    assert_eq!(body["executedBy"], "b-2");
    assert_eq!(body["target"], json!({ "to": "earliest" }));
    assert_eq!(body["replay"], true);
    assert_eq!(
        body["ranges"],
        json!([{ "segment": "0", "from": "0", "until": "42" }])
    );
    assert_eq!(
        (
            body["markersDeleted"].clone(),
            body["resetHandlers"].clone()
        ),
        (json!(3), json!(1))
    );
    assert_eq!(body["error"], Value::Null);
    assert_eq!(
        body["instances"],
        json!([
            { "instanceId": "b-1", "prepare": { "result": "ok", "code": null, "message": "" },
              "finish": { "result": "ok", "code": null, "message": "" } },
            { "instanceId": "b-2", "prepare": { "result": "ok", "code": null, "message": "" },
              "finish": { "result": "ok", "code": null, "message": "" } },
            { "instanceId": "b-3", "prepare": { "result": "refused", "code": "UNKNOWN_PROCESSOR", "message": "no audit here" },
              "finish": null },
        ])
    );

    assert_eq!(actions(&b1.seen()), ["prepare", "finish"]);
    assert_eq!(actions(&b2.seen()), ["prepare", "execute", "finish"]);
    assert_eq!(
        actions(&b3.seen()),
        ["prepare"],
        "not involved: never finished"
    );
    for req in b1.seen().iter().chain(&b2.seen()).chain(&b3.seen()) {
        assert_eq!(operation_of(req), op, "one operation id throughout");
        assert_eq!(req.processor, "audit");
    }
    let Some(control_request::Action::ResetPrepare(prepare)) = &b1.seen()[0].action else {
        panic!()
    };
    assert_eq!(prepare.lease_ms, 30_000, "the default lease");
    let Some(control_request::Action::ResetExecute(execute)) = &b2.seen()[1].action else {
        panic!()
    };
    assert_eq!(
        execute.target.as_ref().unwrap().to,
        Some(kraken_hub::proto::reset_target::To::Earliest(true))
    );

    // The other targets reach the instance as given.
    for (body, want) in [
        (
            r#"{"to":"latest"}"#,
            kraken_hub::proto::reset_target::To::Latest(true),
        ),
        (
            r#"{"to":{"timestampMs":1762250000000}}"#,
            kraken_hub::proto::reset_target::To::TimestampMs(1_762_250_000_000),
        ),
        (
            r#"{"to":{"positions":[{"segment":"1","position":"7"},{"segment":"0","position":"12"}]}}"#,
            kraken_hub::proto::reset_target::To::Positions(kraken_hub::proto::SegmentPositions {
                positions: vec![
                    kraken_hub::proto::SegmentPosition {
                        segment: "0".into(),
                        position: "12".into(),
                    },
                    kraken_hub::proto::SegmentPosition {
                        segment: "1".into(),
                        position: "7".into(),
                    },
                ],
            }),
        ),
    ] {
        let r = post(http, RESET, body).await;
        assert_eq!(r.status, 200, "{body}: {}", r.body);
        let execute = b2
            .seen()
            .into_iter()
            .rev()
            .find(|r| is(r, "execute"))
            .unwrap();
        let Some(control_request::Action::ResetExecute(execute)) = execute.action else {
            panic!()
        };
        assert_eq!(execute.target.unwrap().to, Some(want), "{body}");
    }
}

#[tokio::test]
async fn a_locked_prepare_aborts_the_reset_on_every_instance() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    let b2 = ControlClient::new(
        grpc,
        "svc",
        "b-2",
        &["audit"],
        answering(|req| {
            is(req, "prepare")
                .then(|| Reaction::Reply(refused(ControlRefusal::Locked, "held by operation 7f00")))
        }),
    )
    .await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 409, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(body["outcome"], "aborted");
    assert_eq!(body["executedBy"], Value::Null);
    assert_eq!(
        body["error"],
        json!({ "stage": "prepare", "instanceId": "b-2", "result": "refused", "code": "LOCKED", "message": "held by operation 7f00" })
    );
    assert_eq!(actions(&b1.seen()), ["prepare", "finish-aborted"]);
    assert_eq!(
        actions(&b2.seen()),
        ["prepare", "finish-aborted"],
        "released wherever it may hold the lock"
    );
}

#[tokio::test]
async fn a_prepare_that_times_out_aborts_the_reset() {
    let (grpc, http) = hub(Config {
        control_timeout: Duration::from_millis(400),
        ..config()
    })
    .await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    let b2 = ControlClient::new(
        grpc,
        "svc",
        "b-2",
        &["audit"],
        answering(|req| is(req, "prepare").then_some(Reaction::Silent)),
    )
    .await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 504, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(body["outcome"], "aborted");
    assert_eq!(body["error"]["result"], "timeout");
    assert_eq!(body["error"]["instanceId"], "b-2");
    assert_eq!(actions(&b1.seen()), ["prepare", "finish-aborted"]);
    // A late prepare may still take the lock: the silent instance is released too.
    assert_eq!(actions(&b2.seen()), ["prepare", "finish-aborted"]);
}

#[tokio::test]
async fn an_execute_failure_finishes_every_instance_as_aborted() {
    let (grpc, http) = hub(config()).await;
    let refusing = || {
        answering(|req| {
            is(req, "execute").then(|| {
                Reaction::Reply(refused(
                    ControlRefusal::SubscribersActive,
                    "subscribers: consumer-7",
                ))
            })
        })
    };
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], refusing()).await;
    let b2 = ControlClient::new(grpc, "svc", "b-2", &["audit"], refusing()).await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 409, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["error"]["stage"], "execute");
    assert_eq!(body["error"]["code"], "SUBSCRIBERS_ACTIVE");
    assert_eq!(body["replay"], false);
    let executor = body["executedBy"].as_str().unwrap();
    for b in [&b1, &b2] {
        let want: &[&str] = if b.instance_id == executor {
            &["prepare", "execute", "finish-aborted"]
        } else {
            &["prepare", "finish-aborted"]
        };
        assert_eq!(actions(&b.seen()), want, "{}", b.instance_id);
    }

    // A reset handler that throws is a conflict too; an INVALID target a bad request.
    for (code, status) in [
        (ControlRefusal::ResetHandlerFailed, 409),
        (ControlRefusal::Invalid, 400),
        (ControlRefusal::Failed, 502),
    ] {
        let (grpc, http) = hub(config()).await;
        let _b = ControlClient::new(
            grpc,
            "svc",
            "b-1",
            &["audit"],
            answering(move |req| is(req, "execute").then(|| Reaction::Reply(refused(code, "x")))),
        )
        .await;
        let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
        assert_eq!(r.status, status, "{code:?}: {}", r.body);
    }
}

#[tokio::test]
async fn the_lease_is_renewed_while_the_reset_runs_and_never_after_the_finish() {
    let (grpc, http) = hub(Config {
        control_lease: Duration::from_millis(1000),
        control_lease_renew: Duration::from_millis(200),
        ..config()
    })
    .await;
    let slow = || {
        answering(|req| {
            is(req, "execute")
                .then(|| Reaction::After(Duration::from_millis(1100), default_result(req)))
        })
    };
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], slow()).await;
    let b2 = ControlClient::new(grpc, "svc", "b-2", &["audit"], slow()).await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(600)).await;
    for b in [&b1, &b2] {
        let seen = b.seen();
        let prepares: Vec<&ControlRequest> = seen.iter().filter(|r| is(r, "prepare")).collect();
        // The first prepare and a renewal every 200 ms while the 1.1 s execute runs.
        assert!(
            (5..=7).contains(&prepares.len()),
            "{}: {:?}",
            b.instance_id,
            actions(&seen)
        );
        for p in &prepares {
            let Some(control_request::Action::ResetPrepare(p)) = &p.action else {
                panic!()
            };
            assert_eq!(p.lease_ms, 1000);
        }
        let op = operation_of(prepares[0]);
        assert!(
            prepares.iter().all(|p| operation_of(p) == op),
            "renewed with the same id"
        );
        assert_eq!(
            action_of(seen.last().unwrap()),
            "finish",
            "nothing after the finish"
        );
    }
}

/// An execute that times out may still run on the executor, or may already have run reset
/// handlers and deleted markers: the 504 says so rather than "nothing changed".
#[tokio::test]
async fn an_execute_timeout_says_the_executor_may_still_finish_or_have_changed_things() {
    let (grpc, http) = hub(Config {
        reset_timeout: Duration::from_millis(400),
        ..config()
    })
    .await;
    let b1 = ControlClient::new(
        grpc,
        "svc",
        "b-1",
        &["audit"],
        answering(|req| is(req, "execute").then_some(Reaction::Silent)),
    )
    .await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 504, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["error"]["stage"], "execute");
    assert_eq!(body["error"]["result"], "timeout");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("may still finish")
            && message.contains("reset handlers")
            && message.contains("markers"),
        "{message}"
    );
    assert_eq!(
        actions(&b1.seen()),
        ["prepare", "execute", "finish-aborted"]
    );
}

/// A renewal sent just before the execute's reply is answered before any finish is sent: a
/// renewal that reached an instance after its finish would find the lock free.
#[tokio::test]
async fn the_finish_waits_for_the_renewals_in_flight() {
    let (grpc, http) = hub(Config {
        control_lease: Duration::from_millis(2000),
        control_lease_renew: Duration::from_millis(200),
        ..config()
    })
    .await;
    // (what, when): a renewal is answered 500 ms after it arrives, the execute 300 ms after.
    let log: Arc<std::sync::Mutex<Vec<(&'static str, std::time::Instant)>>> = Arc::default();
    let behaviour = {
        let log = Arc::clone(&log);
        answering(move |req| {
            let mut log = log.lock().unwrap();
            let now = std::time::Instant::now();
            if is(req, "prepare") {
                let renewal = log.iter().any(|(what, _)| *what == "prepare");
                log.push((if renewal { "renewal" } else { "prepare" }, now));
                return renewal
                    .then(|| Reaction::After(Duration::from_millis(500), default_result(req)));
            }
            log.push((action_of(req), now));
            is(req, "execute")
                .then(|| Reaction::After(Duration::from_millis(300), default_result(req)))
        })
    };
    let _b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], behaviour).await;
    let r = post(http, RESET, r#"{"to":"earliest"}"#).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let log = log.lock().unwrap().clone();
    let at = |what: &str| log.iter().find(|(w, _)| *w == what).map(|(_, t)| *t);
    let (renewal, finish) = (at("renewal").expect("a renewal"), at("finish").unwrap());
    assert!(
        finish >= renewal + Duration::from_millis(450),
        "the finish went {:?} after the renewal, before its reply",
        finish - renewal
    );
}

#[tokio::test]
async fn a_second_reset_of_a_processor_is_refused_while_one_runs() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(
        grpc,
        "svc",
        "b-1",
        &["audit", "default"],
        answering(|req| {
            (is(req, "execute") && req.processor == "audit")
                .then(|| Reaction::After(Duration::from_millis(800), default_result(req)))
        }),
    )
    .await;
    let first = tokio::spawn(async move { post(http, RESET, r#"{"to":"earliest"}"#).await });
    b1.wait_for(2, Duration::from_secs(3)).await;
    let r = post(http, RESET, r#"{"to":"latest"}"#).await;
    assert_eq!(r.status, 409, "{}", r.body);
    assert!(
        json_of(&r)["error"]
            .as_str()
            .unwrap()
            .contains("is running on this hub")
    );
    // Another processor of the service is not held.
    let r = post(
        http,
        "/api/v1/services/svc/processors/default/reset",
        r#"{"to":"latest"}"#,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(first.await.unwrap().status, 200);
    // Released once it ended.
    let r = post(http, RESET, r#"{"to":"latest"}"#).await;
    assert_eq!(r.status, 200, "{}", r.body);
}

#[tokio::test]
async fn an_instance_that_leaves_mid_call_fails_it_at_once() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(
        grpc,
        "svc",
        "b-1",
        &["audit"],
        answering(|_| Some(Reaction::Silent)),
    )
    .await;
    let call = tokio::spawn(async move { post(http, PAUSE, "").await });
    b1.wait_for(1, Duration::from_secs(3)).await;
    let started = std::time::Instant::now();
    drop(b1);
    let r = call.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "not at the 10 s timeout"
    );
    assert_eq!(r.status, 502, "{}", r.body);
    assert_eq!(
        json_of(&r)["instances"][0],
        json!({ "instanceId": "b-1", "result": "left", "code": null, "message": "instance left" })
    );
}

/// A letter with every field set, `n`-th of its page.
fn letter(n: usize, long: bool) -> kraken_hub::proto::DeadLetterSummary {
    let text = |s: &str, len: usize| if long { s.repeat(len) } else { s.to_string() };
    kraken_hub::proto::DeadLetterSummary {
        id: if long {
            "i".repeat(300)
        } else {
            format!("letter-{n}")
        },
        processor: "audit".into(),
        source: "kraken.events".into(),
        segment: "1".into(),
        position: (100 + n).to_string(),
        key: "order-7".into(),
        event_id: format!("evt-{n}"),
        event_type: "examples.OrderPlaced".into(),
        reason: "HANDLER_FAILED".into(),
        error_type: "IllegalStateException".into(),
        error_message: text("boom ", 400),
        blocked_by: String::new(),
        attempts: 3,
        parked_at_ms: 1_762_250_000_000,
        last_attempt_at_ms: 1_762_250_001_000,
    }
}

#[tokio::test]
async fn dead_letters_are_listed_bounded_and_retried_or_discarded_by_one_instance() {
    let (grpc, http) = hub(Config {
        dashboard_token: Some(Secret::new(READ)),
        ..config()
    })
    .await;
    let b1 = ControlClient::new(
        grpc,
        "svc",
        "b-1",
        &["audit"],
        answering(|req| {
            use control_reply::Result as R;
            use control_request::Action as A;
            Some(Reaction::Reply(match &req.action {
                Some(A::DeadLetterList(l)) => R::DeadLetters(kraken_hub::proto::DeadLetterPage {
                    letters: (0..150).map(|n| letter(n, l.limit == 100)).collect(),
                    total: 150,
                }),
                Some(A::DeadLetterRetry(r)) if r.letter_id == "blocked" => {
                    R::Retry(kraken_hub::proto::DeadLetterRetryOutcome {
                        result: "Blocked".into(),
                        detail: "letter-0".into(),
                    })
                }
                Some(A::DeadLetterRetry(_)) => {
                    R::Retry(kraken_hub::proto::DeadLetterRetryOutcome {
                        result: "Retried".into(),
                        detail: String::new(),
                    })
                }
                Some(A::DeadLetterRetryAll(_)) => {
                    R::RetryAll(kraken_hub::proto::DeadLetterRetryAllOutcome {
                        retried: 2,
                        failed: 1,
                        blocked: 4,
                        skipped: 0,
                    })
                }
                Some(A::DeadLetterDiscard(d)) if d.letter_id == "gone" => {
                    refused(ControlRefusal::NotFound, "no letter gone")
                }
                _ => ok(),
            }))
        }),
    )
    .await;

    // The read token lists them (a read); the default page is 50 letters.
    let r = call(http, "GET", LETTERS, Some(READ), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let body = json_of(&r);
    assert_eq!(
        (body["service"].clone(), body["processor"].clone()),
        (json!("svc"), json!("audit"))
    );
    assert_eq!(body["instanceId"], "b-1");
    assert_eq!(body["total"], 150);
    assert_eq!(
        body["letters"].as_array().unwrap().len(),
        100,
        "at most 100 kept"
    );
    assert_eq!(
        body["letters"][0],
        json!({
            "id": "letter-0", "processor": "audit", "source": "kraken.events", "segment": "1",
            "position": "100", "key": "order-7", "eventId": "evt-0", "eventType": "examples.OrderPlaced",
            "reason": "HANDLER_FAILED", "errorType": "IllegalStateException", "errorMessage": "boom ",
            "blockedBy": "", "attempts": 3, "parkedAtMs": 1762250000000i64, "lastAttemptAtMs": 1762250001000i64,
        })
    );
    let Some(control_request::Action::DeadLetterList(list)) = &b1.seen()[0].action else {
        panic!()
    };
    assert_eq!(list.limit, 50);
    // A limit beyond 100 is clamped (this fake then sends long fields: they are cut).
    let r = call(
        http,
        "GET",
        &format!("{LETTERS}?limit=500"),
        Some(WRITE),
        "",
    )
    .await;
    assert_eq!(r.status, 200, "the write token reads too: {}", r.body);
    let body = json_of(&r);
    assert_eq!(body["letters"][0]["id"].as_str().unwrap().len(), 128);
    assert_eq!(
        body["letters"][0]["errorMessage"].as_str().unwrap().len(),
        512
    );
    for bad in ["?limit=abc", "?limit=-1"] {
        let r = call(http, "GET", &format!("{LETTERS}{bad}"), Some(READ), "").await;
        assert_eq!(r.status, 400, "{bad}");
    }

    let r = post(http, &format!("{LETTERS}/letter-3/retry"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        json_of(&r),
        json!({ "service": "svc", "processor": "audit", "instanceId": "b-1", "letterId": "letter-3", "result": "Retried", "detail": "" })
    );
    let r = post(http, &format!("{LETTERS}/blocked/retry"), "").await;
    assert_eq!(r.status, 409, "{}", r.body);
    assert_eq!(json_of(&r)["detail"], "letter-0");

    let r = post(http, &format!("{LETTERS}/retry-all"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        json_of(&r),
        json!({ "service": "svc", "processor": "audit", "instanceId": "b-1", "retried": 2, "failed": 1, "blocked": 4, "skipped": 0 })
    );

    let r = call(
        http,
        "DELETE",
        &format!("{LETTERS}/letter-3"),
        Some(WRITE),
        "",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        json_of(&r),
        json!({ "service": "svc", "processor": "audit", "instanceId": "b-1", "letterId": "letter-3", "discarded": true })
    );
    let r = call(http, "DELETE", &format!("{LETTERS}/gone"), Some(WRITE), "").await;
    assert_eq!(r.status, 404, "{}", r.body);
    assert_eq!(
        json_of(&r),
        json!({ "error": "the instance refused: no letter gone", "instanceId": "b-1", "result": "refused", "code": "NOT_FOUND", "message": "no letter gone" })
    );
    let ids: Vec<String> = b1
        .seen()
        .iter()
        .filter_map(|r| match &r.action {
            Some(control_request::Action::DeadLetterRetry(x)) => Some(x.letter_id.clone()),
            Some(control_request::Action::DeadLetterDiscard(x)) => Some(x.letter_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, ["letter-3", "blocked", "letter-3", "gone"]);
    // A letter id the instance could not hold is refused before anyone is asked.
    let asked = b1.seen().len();
    let r = post(http, &format!("{LETTERS}/{}/retry", "x".repeat(129)), "").await;
    assert_eq!(r.status, 400);
    assert_eq!(b1.seen().len(), asked);
}

#[tokio::test]
async fn bodies_are_bounded_and_checked_before_anyone_is_asked() {
    let (grpc, http) = hub(config()).await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    let many: Vec<String> = (0..1025)
        .map(|p| format!("{{\"segment\":\"{p}\",\"position\":\"0\"}}"))
        .collect();
    for (body, why) in [
        ("".to_string(), "no target"),
        ("{\"to\":\"yesterday\"}".to_string(), "an unknown target"),
        (
            "{\"to\":{\"timestampMs\":-5}}".to_string(),
            "a negative time",
        ),
        ("not json".to_string(), "not JSON"),
        (
            format!("{{\"to\":{{\"positions\":[{}]}}}}", many.join(",")),
            "1025 positions",
        ),
        (
            format!("{{\"to\":\"earliest\",\"pad\":\"{}\"}}", "x".repeat(5000)),
            "over 4 KiB",
        ),
    ] {
        let r = post(http, RESET, &body).await;
        assert_eq!(r.status, 400, "{why}: {}", r.body);
        assert!(json_of(&r)["error"].is_string(), "{why}");
    }
    let r = post(http, PAUSE, &"x".repeat(5000)).await;
    assert_eq!(r.status, 400, "every control call bounds its body");
    assert!(b1.seen().is_empty(), "nobody was asked");
}

// ---- access control ----

async fn status(
    http: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (u16, String) {
    let r = call(http, method, path, token, body).await;
    (r.status, r.body)
}

#[tokio::test]
async fn without_a_write_token_or_an_acl_control_entry_controls_are_disabled() {
    let (grpc, http) = hub(Config {
        dashboard_write_token: None,
        ..config()
    })
    .await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    for (method, path, body) in [
        ("POST", PAUSE, ""),
        ("POST", RESUME, ""),
        ("POST", RESET, r#"{"to":"earliest"}"#),
        (
            "POST",
            "/api/v1/services/svc/processors/audit/dead-letters/l-1/retry",
            "",
        ),
        (
            "POST",
            "/api/v1/services/svc/processors/audit/dead-letters/retry-all",
            "",
        ),
        (
            "DELETE",
            "/api/v1/services/svc/processors/audit/dead-letters/l-1",
            "",
        ),
    ] {
        for token in [None, Some("anything-at-all-0123456789")] {
            let (code, body) = status(http, method, path, token, body).await;
            assert_eq!(code, 403, "{method} {path}");
            assert_eq!(body, r#"{"error":"controls are disabled"}"#);
        }
    }
    // Reads stay as they were (no dashboard token: open).
    let (code, _) = status(http, "GET", LETTERS, None, "").await;
    assert_eq!(code, 200);
    assert_eq!(actions(&b1.seen()), ["list"]);
}

#[tokio::test]
async fn the_write_token_writes_the_read_token_never_does() {
    let (grpc, http) = hub(Config {
        dashboard_token: Some(Secret::new(READ)),
        ..config()
    })
    .await;
    let b1 = ControlClient::new(grpc, "svc", "b-1", &["audit"], ok_to_everything()).await;
    assert_eq!(status(http, "POST", PAUSE, None, "").await.0, 401);
    assert_eq!(
        status(http, "POST", PAUSE, Some("not-the-token-0123456789"), "")
            .await
            .0,
        401
    );
    let (code, body) = status(http, "POST", PAUSE, Some(READ), "").await;
    assert_eq!(code, 403);
    assert!(body.contains("read-only"), "{body}");
    assert!(b1.seen().is_empty());
    assert_eq!(status(http, "POST", PAUSE, Some(WRITE), "").await.0, 200);
    assert_eq!(actions(&b1.seen()), ["pause"]);
    // A 401 is bare, as the dashboard token's.
    let r = call(http, "POST", PAUSE, None, "").await;
    assert_eq!(
        (r.header("www-authenticate"), r.body.as_str()),
        (Some("Bearer"), "")
    );
    // The write token reads what the dashboard token reads; no token reads nothing.
    for path in ["/api/v1/processors", "/api/v1/topology", LETTERS] {
        assert_eq!(
            status(http, "GET", path, Some(WRITE), "").await.0,
            200,
            "{path}"
        );
        assert_eq!(
            status(http, "GET", path, Some(READ), "").await.0,
            200,
            "{path}"
        );
        assert_eq!(status(http, "GET", path, None, "").await.0, 401, "{path}");
    }
}

#[tokio::test]
async fn an_acl_credential_controls_only_with_control_set() {
    let dir = TempDir::new();
    let acl = dir.write(
        "acl.toml",
        &format!(
            "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"svc\"\ncontrol = true\n\n[[token]]\ntoken_sha256 = \"{}\"\nservice = \"ops\"\n",
            hash_token("tok-svc-0123456789"),
            hash_token("tok-ops-0123456789")
        ),
    );
    // A dashboard token guards the reads; the controls judge their own credential.
    let (grpc, http) = hub(Config {
        dashboard_write_token: None,
        dashboard_token: Some(Secret::new(READ)),
        auth_mode: AuthMode::Enforce,
        acl_file: Some(acl),
        ..config()
    })
    .await;
    let b1 = ControlClient::start(
        grpc,
        "svc",
        "b-1",
        &["audit"],
        Some("tok-svc-0123456789"),
        ok_to_everything(),
    )
    .await;
    assert_eq!(
        status(http, "POST", PAUSE, Some("tok-svc-0123456789"), "")
            .await
            .0,
        200
    );
    let (code, body) = status(http, "POST", PAUSE, Some("tok-ops-0123456789"), "").await;
    assert_eq!(code, 403, "an entry without control = true");
    assert!(body.contains("may not control"), "{body}");
    let (code, body) = status(http, "POST", PAUSE, Some(READ), "").await;
    assert_eq!(code, 403);
    assert!(body.contains("read-only"), "{body}");
    // An ACL credential does not read the HTTP API.
    assert_eq!(
        status(http, "GET", LETTERS, Some("tok-svc-0123456789"), "")
            .await
            .0,
        401
    );
    assert_eq!(
        status(http, "POST", PAUSE, Some("tok-unknown-0123456789"), "")
            .await
            .0,
        401
    );
    assert_eq!(status(http, "POST", PAUSE, None, "").await.0, 401);
    assert_eq!(actions(&b1.seen()), ["pause"]);
}

#[tokio::test]
async fn peers_may_send_control_calls_cross_origin() {
    let peer = "http://hub-b.example:7001";
    let (_grpc, http) = hub(Config {
        peers: vec![peer.to_string()],
        ..config()
    })
    .await;
    for method in ["POST", "DELETE"] {
        let r = common::http_request(
            http,
            "OPTIONS",
            RESET,
            &[
                ("Origin", peer),
                ("Access-Control-Request-Method", method),
                (
                    "Access-Control-Request-Headers",
                    "authorization, content-type",
                ),
            ],
        )
        .await;
        assert_eq!(r.status, 204, "{method}");
        assert_eq!(r.header("access-control-allow-origin"), Some(peer));
        let methods = r.header("access-control-allow-methods").unwrap();
        assert!(methods.contains(method), "{methods}");
        let headers = r.header("access-control-allow-headers").unwrap();
        assert!(
            headers.contains("authorization") && headers.contains("content-type"),
            "{headers}"
        );
        // Any other origin is refused.
        let r = common::http_request(
            http,
            "OPTIONS",
            RESET,
            &[
                ("Origin", "http://evil.example"),
                ("Access-Control-Request-Method", method),
            ],
        )
        .await;
        assert_eq!(r.status, 403);
    }
    // A refused call carries the CORS header, so the peer's page can read why.
    let r = http_send(
        http,
        "POST",
        PAUSE,
        &[
            ("Origin", peer),
            ("Authorization", &bearer("wrong-wrong-wrong-0123")),
        ],
        b"",
        LONG,
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.header("access-control-allow-origin"), Some(peer));
}
