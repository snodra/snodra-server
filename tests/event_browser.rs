//! The event browser (ADR 0025) against a real Kafka: search by every filter, newest first over
//! the partitions, with a cursor; the budgets and the concurrency limits; the record detail and
//! the payload rules; the tail; consumer groups and their lag; and what happens without Kafka.
//! Every test keeps its broker in a local, dropped at its end.

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::kafka::{self, Member, Rec};
use common::{FakeClient, http_send};
use kraken_hub::auth::Secret;
use kraken_hub::config::Config;
use kraken_hub::proto::{RequestKind, server_message};
use serde_json::Value;

const T: i64 = 1_700_000_000_000;
const READ: &str = "a-dashboard-token-0123456789";
const WRITE: &str = "a-write-token-0123456789";

fn config(bootstrap: &str, topics: &[&str]) -> Config {
    Config {
        otlp_endpoint: None,
        kafka_bootstrap: Some(bootstrap.to_string()),
        event_topics: topics.iter().map(|t| t.to_string()).collect(),
        ..Config::default()
    }
}

async fn hub(config: Config) -> (SocketAddr, SocketAddr) {
    let (grpc, http) = common::start_hub_with_http(config).await;
    (grpc, http.unwrap())
}

/// `GET path` (30 s at most): the status and the JSON body (`Null` when there is none).
async fn get(addr: SocketAddr, path: &str, token: Option<&str>) -> (u16, Value) {
    let auth = token.map(|t| format!("Bearer {t}"));
    let headers: Vec<(&str, &str)> = auth
        .as_deref()
        .map(|a| vec![("Authorization", a)])
        .unwrap_or_default();
    let r = http_send(addr, "GET", path, &headers, b"", Duration::from_secs(30)).await;
    let body = serde_json::from_str(&r.body).unwrap_or(Value::Null);
    (r.status, body)
}

/// The `id` header of every result, in order.
fn ids(page: &Value) -> Vec<String> {
    page["results"]
        .as_array()
        .unwrap_or_else(|| panic!("no results in {page}"))
        .iter()
        .map(|r| r["headers"]["id"].as_str().unwrap_or("").to_string())
        .collect()
}

fn library(partition: i32, ts: i64, key: &str, ty: &str, id: &str) -> Rec {
    Rec::on(partition)
        .at(T + ts)
        .key(key)
        .header("type", ty)
        .header("id", id)
        .header("timestamp", (T + ts).to_string())
        .payload(format!("{{\"id\":\"{id}\"}}"))
}

#[tokio::test]
async fn search_matches_every_filter_newest_first_and_pages_with_a_cursor() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("events");
    kafka::create_topic(&broker.bootstrap, &topic, 3).await;
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[
            library(0, 10, "k-1", "orders.Placed", "e-1")
                .header("correlationId", "c-1")
                .header("source", "svc-a")
                .header("userId", "u-1"),
            library(0, 40, "k-2", "orders.Shipped", "e-2")
                .header("correlationId", "c-2")
                .header("source", "svc-b"),
            library(1, 20, "k-3", "users.Created", "e-3")
                .header("correlationId", "c-1")
                .header("userId", "u-2"),
            library(1, 50, "k-1", "orders.Placed", "e-4")
                .header("correlationId", "c-3")
                .header("source", "svc-a"),
            library(2, 30, "k-4", "orders.Cancelled", "e-5"),
            library(2, 60, "k-5", "users.Deleted", "e-6").header("correlationId", "c-1"),
        ],
    )
    .await;
    let (_grpc, http) = hub(config(&broker.bootstrap, &[&topic, "absent"])).await;

    let (status, all) = get(http, "/api/v1/events", None).await;
    assert_eq!(status, 200, "{all}");
    assert_eq!(
        all["topic"],
        topic.as_str(),
        "the first topic is the default"
    );
    assert_eq!(ids(&all), ["e-6", "e-4", "e-2", "e-5", "e-3", "e-1"]);
    assert_eq!(all["scanned"], 6);
    assert_eq!(all["exhausted"], true);
    assert!(
        all["stoppedBy"].is_null() && all["cursor"].is_null(),
        "{all}"
    );
    let first = &all["results"][0];
    assert_eq!(
        (&first["partition"], &first["offset"], &first["timestamp"]),
        (&Value::from(2), &Value::from(1), &Value::from(T + 60))
    );
    assert_eq!(first["key"], "k-5");
    assert_eq!(first["headers"]["type"], "users.Deleted");
    assert_eq!(first["size"], "{\"id\":\"e-6\"}".len());
    assert!(
        !all.to_string().contains("{\\\"id\\\""),
        "no payload in a result: {all}"
    );

    let search = |query: String| {
        let path = format!("/api/v1/events?topic={topic}&{query}");
        async move {
            let (status, page) = get(http, &path, None).await;
            assert_eq!(status, 200, "{path}: {page}");
            ids(&page)
        }
    };
    for (query, want) in [
        ("key=k-1", vec!["e-4", "e-1"]),
        ("type=orders.Placed", vec!["e-4", "e-1"]),
        ("type=orders.*", vec!["e-4", "e-2", "e-5", "e-1"]),
        ("type=orders", vec![]),
        ("source=svc-a", vec!["e-4", "e-1"]),
        ("correlationId=c-1", vec!["e-6", "e-3", "e-1"]),
        ("userId=u-2", vec!["e-3"]),
        ("eventId=e-5", vec!["e-5"]),
        ("type=orders.*&key=k-1&source=svc-a", vec!["e-4", "e-1"]),
        (
            &format!("from={}&to={}", T + 20, T + 50),
            vec!["e-4", "e-2", "e-5", "e-3"],
        ),
        (
            "from=2023-11-14T22:13:20.030Z",
            vec!["e-6", "e-4", "e-2", "e-5"],
        ),
        ("to=2023-11-14T23:13:20.020%2B01:00", vec!["e-3", "e-1"]),
    ] {
        assert_eq!(search(query.to_string()).await, want, "{query}");
    }

    // Pages of two: together they are the whole result, in the same order, nothing twice.
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let path = match &cursor {
            Some(c) => format!("/api/v1/events?topic={topic}&limit=2&cursor={c}"),
            None => format!("/api/v1/events?topic={topic}&limit=2"),
        };
        let (status, page) = get(http, &path, None).await;
        assert_eq!(status, 200, "{page}");
        pages.push(ids(&page));
        if page["exhausted"] == true {
            assert!(page["cursor"].is_null());
            break;
        }
        assert_eq!(page["stoppedBy"], "limit");
        cursor = Some(page["cursor"].as_str().unwrap().to_string());
        assert!(pages.len() < 10, "{pages:?}");
    }
    assert_eq!(
        pages,
        vec![vec!["e-6", "e-4"], vec!["e-2", "e-5"], vec!["e-3", "e-1"]]
    );

    // A cursor only continues its own search.
    let (_, page) = get(http, &format!("/api/v1/events?topic={topic}&limit=2"), None).await;
    let c = page["cursor"].as_str().unwrap();
    let (status, body) = get(
        http,
        &format!("/api/v1/events?topic={topic}&limit=2&type=x&cursor={c}"),
        None,
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body["error"].as_str().unwrap().contains("another search"));
    for bad in [
        format!("/api/v1/events?topic={topic}&cursor=%25%25"),
        "/api/v1/events?topic=not-configured".to_string(),
        format!("/api/v1/events?topic={topic}&limit=101"),
        format!("/api/v1/events?topic={topic}&from=tomorrow"),
        format!("/api/v1/events?topic={topic}&from=20&to=10"),
    ] {
        let (status, body) = get(http, &bad, None).await;
        assert_eq!(status, 400, "{bad}: {body}");
        assert!(body["error"].is_string(), "{body}");
    }

    // A configured topic that does not exist is missing, not an error (and is not created).
    let (status, body) = get(http, "/api/v1/events?topic=absent", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["missing"], true);
    assert_eq!(body["results"], Value::Array(vec![]));
    let (status, _) = get(http, "/api/v1/events?topic=absent", None).await;
    assert_eq!(status, 200);
    let (_, again) = get(http, "/api/v1/events?topic=absent", None).await;
    assert_eq!(again["missing"], true, "the browser created the topic");
    drop(broker);
}

/// Producers set `CreateTime`, so a partition's timestamps need not rise: `to` still finds a
/// record written after a later-stamped one (within [`kraken_hub::browser::TO_SKEW`]), and `from`
/// and `to` hold for every record.
#[tokio::test]
async fn from_and_to_find_records_with_out_of_order_timestamps() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("skew");
    kafka::create_topic(&broker.bootstrap, &topic, 1).await;
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[
            library(0, 10, "k", "s.A", "e-10"),
            library(0, 100, "k", "s.A", "e-100"),
            library(0, 20, "k", "s.A", "e-20"),
            library(0, 60_000, "k", "s.A", "e-60000"),
            library(0, 30, "k", "s.A", "e-30"),
            library(0, 5, "k", "s.A", "e-5"),
        ],
    )
    .await;
    let (_g, http) = hub(config(&broker.bootstrap, &[&topic])).await;
    for (query, want) in [
        (
            format!("to={}", T + 50),
            vec!["e-5", "e-30", "e-20", "e-10"],
        ),
        (
            format!("from={}&to={}", T + 15, T + 50),
            vec!["e-30", "e-20"],
        ),
        (format!("from={}", T + 50), vec!["e-60000", "e-100"]),
    ] {
        let (status, page) = get(http, &format!("/api/v1/events?{query}"), None).await;
        assert_eq!(status, 200, "{query}: {page}");
        assert_eq!(ids(&page), want, "{query}");
    }
    drop(broker);
}

/// Records compressed by their producer (any codec a Kafka client may use) are found and read.
#[tokio::test]
async fn compressed_records_are_found_and_read() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("codecs");
    kafka::create_topic(&broker.bootstrap, &topic, 1).await;
    for (i, codec) in ["gzip", "snappy", "lz4", "zstd"].into_iter().enumerate() {
        let record = library(0, i as i64, codec, "c.Packed", &format!("e-{codec}"));
        kafka::produce_compressed(&broker.bootstrap, &topic, &[record], codec).await;
    }
    let (_g, http) = hub(config(&broker.bootstrap, &[&topic])).await;
    let (status, page) = get(http, "/api/v1/events", None).await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(ids(&page), ["e-zstd", "e-lz4", "e-snappy", "e-gzip"]);
    let (status, detail) = get(http, &format!("/api/v1/events/{topic}/0/3"), None).await;
    assert_eq!(status, 200, "{detail}");
    assert_eq!(detail["key"], "zstd");
    assert_eq!(detail["headers"]["id"], "e-zstd");
    drop(broker);
}

/// Many partitions read at once: every page of a search with a cursor continues the last, and
/// together they are the whole list, newest first, nothing twice.
#[tokio::test]
async fn many_partitions_page_newest_first_without_skipping_or_repeating() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("wide");
    kafka::create_topic(&broker.bootstrap, &topic, 24).await;
    let records: Vec<Rec> = (0..24 * 30)
        .map(|i| {
            let ty = if i % 3 == 0 { "w.Hit" } else { "w.Miss" };
            // Rising within a partition, interleaved across them.
            let ts = (i / 24) * 100 + (i % 24) * 37 % 100;
            library(i % 24, ts as i64, "k", ty, &format!("e-{i}"))
        })
        .collect();
    kafka::produce(&broker.bootstrap, &topic, &records).await;
    let (_g, http) = hub(Config {
        event_search_max_results: 1000,
        ..config(&broker.bootstrap, &[&topic])
    })
    .await;

    let (status, all) = get(http, "/api/v1/events?type=w.Hit&limit=1000", None).await;
    assert_eq!(status, 200, "{all}");
    assert_eq!(all["exhausted"], true, "{all}");
    let whole = ids(&all);
    assert_eq!(whole.len(), 24 * 10);
    let times: Vec<i64> = all["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_i64().unwrap())
        .collect();
    assert!(times.windows(2).all(|w| w[0] >= w[1]), "{times:?}");

    let mut got = Vec::new();
    let mut cursor: Option<String> = None;
    for pages in 1.. {
        assert!(pages <= 40, "{} of {} found", got.len(), whole.len());
        let path = match &cursor {
            Some(c) => format!("/api/v1/events?type=w.Hit&limit=7&cursor={c}"),
            None => "/api/v1/events?type=w.Hit&limit=7".to_string(),
        };
        let (status, page) = get(http, &path, None).await;
        assert_eq!(status, 200, "{page}");
        got.extend(ids(&page));
        match page["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert_eq!(got, whole);
    drop(broker);
}

#[tokio::test]
async fn budgets_stop_a_search_and_concurrency_is_bounded() {
    let broker = kafka::kafka().await;
    let big = kafka::unique("big");
    kafka::create_topic(&broker.bootstrap, &big, 1).await;
    let bootstrap = broker.bootstrap.clone();
    let topic = big.clone();
    tokio::task::spawn_blocking(move || kafka::produce_many(&bootstrap, &topic, 0, 300_000, T))
        .await
        .unwrap();

    // The scan budget: 100 records read, nothing found, a cursor that moves on.
    let (_g, scan) = hub(Config {
        event_search_max_scanned: 100,
        ..config(&broker.bootstrap, &[&big])
    })
    .await;
    let (status, page) = get(scan, "/api/v1/events?key=nope", None).await;
    assert_eq!(status, 200, "{page}");
    assert_eq!(page["stoppedBy"], "scanBudget");
    assert_eq!(page["scanned"], 100);
    assert_eq!(page["exhausted"], false);
    assert_eq!(page["results"], Value::Array(vec![]));
    let c = page["cursor"].as_str().unwrap();
    let (_, next) = get(scan, &format!("/api/v1/events?key=nope&cursor={c}"), None).await;
    assert_eq!(next["stoppedBy"], "scanBudget");
    assert_ne!(next["cursor"], page["cursor"], "the next page moved on");
    // The newest records come first: the last ones written.
    let (_, newest) = get(scan, "/api/v1/events?limit=3", None).await;
    let keys: Vec<&str> = newest["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["k-299999", "k-299998", "k-299997"]);

    // The time budget, and one search at a time.
    let (_g, slow) = hub(Config {
        event_search_max_scanned: 10_000_000,
        event_search_timeout: Duration::from_millis(300),
        event_max_searches: 1,
        ..config(&broker.bootstrap, &[&big])
    })
    .await;
    let first = tokio::spawn(async move { get(slow, "/api/v1/events?key=nope", None).await });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let busy = http_send(
        slow,
        "GET",
        "/api/v1/events?key=nope",
        &[],
        b"",
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(busy.status, 429, "{}", busy.body);
    assert!(busy.header("retry-after").is_some());
    let (status, page) = first.await.unwrap();
    assert_eq!(status, 200, "{page}");
    assert_eq!(page["stoppedBy"], "timeBudget");
    let scanned = page["scanned"].as_u64().unwrap();
    assert!(scanned < 300_000, "{scanned}");
    assert!(page["cursor"].is_string());
    // The slot is given back.
    let (status, _) = get(slow, "/api/v1/events?limit=1", None).await;
    assert_eq!(status, 200);
    drop(broker);
}

#[tokio::test]
async fn the_record_detail_serves_the_payload_only_under_the_rules_and_audits_it() {
    kafka::capture_logs();
    let broker = kafka::kafka().await;
    let topic = kafka::unique("detail");
    kafka::create_topic(&broker.bootstrap, &topic, 1).await;
    let secret = format!("s3cr3t-{}", uuid::Uuid::now_v7().simple());
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[
            Rec::on(0)
                .at(T)
                .key(b"k\xff".to_vec())
                .header("type", "a.Placed")
                .header("id", "e-1")
                .header("bin", b"\x00\xff".to_vec())
                .payload(format!("{{\"secret\":\"{secret}\"}}")),
            Rec::on(0)
                .at(T + 1)
                .header("type", "a.Raw")
                .payload(b"\xff\xfe".to_vec()),
        ],
    )
    .await;
    let base = config(&broker.bootstrap, &[&topic]);
    let record = format!("/api/v1/events/{topic}/0/0");

    // The switch is off: hidden, whatever the token.
    let (_g, off) = hub(Config {
        dashboard_token: Some(Secret::new(READ)),
        ..base.clone()
    })
    .await;
    let (status, d) = get(off, &record, Some(READ)).await;
    assert_eq!(status, 200, "{d}");
    assert_eq!(d["payload"]["hidden"], "payloads-off");
    assert_eq!(d["key"], "k\\xff");
    assert_eq!(d["headers"]["type"], "a.Placed");
    let bin = d["allHeaders"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == "bin")
        .unwrap();
    assert_eq!(bin["valueHex"], "00ff");
    assert!(bin["value"].is_null());
    let (status, q) = get(off, &format!("/api/v1/events?q={secret}"), Some(READ)).await;
    assert_eq!(status, 403, "{q}");
    assert_eq!(q["hidden"], "payloads-off");

    // On, but the hub has no token to present.
    let (_g, open) = hub(Config {
        event_payloads: true,
        ..base.clone()
    })
    .await;
    let (_, d) = get(open, &record, None).await;
    assert_eq!(d["payload"]["hidden"], "no-token-configured");
    let (status, q) = get(open, &format!("/api/v1/events?q={secret}"), None).await;
    assert_eq!(
        (status, &q["hidden"]),
        (403, &Value::from("no-token-configured"))
    );

    // On, a write token configured (reads stay open), but not presented; then presented.
    let (_g, writer) = hub(Config {
        event_payloads: true,
        dashboard_write_token: Some(Secret::new(WRITE)),
        ..base.clone()
    })
    .await;
    let (_, d) = get(writer, &record, None).await;
    assert_eq!(d["payload"]["hidden"], "no-token-presented");
    assert!(!d.to_string().contains(&secret));
    let (_, d) = get(writer, &record, Some(WRITE)).await;
    assert_eq!(d["payload"], format!("{{\"secret\":\"{secret}\"}}"));

    // On, both tokens: the dashboard token sees the payloads and may search them.
    let (_g, secure) = hub(Config {
        event_payloads: true,
        dashboard_token: Some(Secret::new(READ)),
        dashboard_write_token: Some(Secret::new(WRITE)),
        ..base.clone()
    })
    .await;
    let (status, _) = get(secure, &record, None).await;
    assert_eq!(status, 401, "reads need the token");
    let (_, d) = get(secure, &record, Some(READ)).await;
    assert_eq!(d["payload"], format!("{{\"secret\":\"{secret}\"}}"));
    assert!(d.get("payloadTruncated").is_none());
    let (_, raw) = get(secure, &format!("/api/v1/events/{topic}/0/1"), Some(READ)).await;
    assert_eq!(raw["payloadBase64"], "//4=");
    assert!(raw.get("payload").is_none(), "{raw}");
    let (status, q) = get(secure, &format!("/api/v1/events?q={secret}"), Some(READ)).await;
    assert_eq!(status, 200, "{q}");
    assert_eq!(ids(&q), ["e-1"]);
    assert!(
        !q.to_string().contains(&secret),
        "a result never carries the payload"
    );

    // Out of range, unknown partition, unconfigured topic.
    for (path, want) in [
        (format!("/api/v1/events/{topic}/0/2"), 404),
        (format!("/api/v1/events/{topic}/0/-1"), 400),
        (format!("/api/v1/events/{topic}/7/0"), 404),
        (format!("/api/v1/events/{topic}/x/0"), 400),
        ("/api/v1/events/elsewhere/0/0".to_string(), 400),
    ] {
        let (status, body) = get(secure, &path, Some(READ)).await;
        assert_eq!(status, want, "{path}: {body}");
    }

    // One audit line per payload served (three: write token, dashboard token twice), with the
    // caller and the record; never the payload, in any line.
    let lines = kafka::logged();
    let audits: Vec<_> = lines
        .iter()
        .filter(|l| l.message == "event payload served" && l.field("topic") == Some(&topic))
        .collect();
    assert_eq!(audits.len(), 3, "{audits:#?}");
    assert_eq!(audits[0].field("caller"), Some("write-token"));
    assert_eq!(audits[1].field("caller"), Some("dashboard-token"));
    assert_eq!(
        (
            audits[1].field("partition"),
            audits[1].field("offset"),
            audits[1].field("size")
        ),
        (
            Some("0"),
            Some("0"),
            Some(
                format!("{{\"secret\":\"{secret}\"}}")
                    .len()
                    .to_string()
                    .as_str()
            )
        )
    );
    assert_eq!(audits[2].field("offset"), Some("1"));
    assert!(
        lines.iter().all(|l| !l.text().contains(&secret)),
        "a payload was logged"
    );
    drop(broker);
}

#[tokio::test]
async fn a_tail_delivers_new_records_ends_on_time_and_drops_a_slow_reader() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("tail");
    kafka::create_topic(&broker.bootstrap, &topic, 2).await;
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[library(0, 0, "old", "t.Hit", "before")],
    )
    .await;
    let (_g, short) = hub(Config {
        event_tail_duration: Duration::from_secs(2),
        event_max_tails: 1,
        ..config(&broker.bootstrap, &[&topic])
    })
    .await;

    let started = Instant::now();
    let mut tail = kafka::sse(short, "/api/v1/events/tail?type=t.Hit", None).await;
    assert_eq!(tail.status, 200);
    let (name, data) = tail.next(Duration::from_secs(10)).await.unwrap();
    assert_eq!(name, "start", "{data}");
    let busy = common::http_request(short, "GET", "/api/v1/events/tail", &[]).await;
    assert_eq!(busy.status, 429, "one tail at a time: {}", busy.body);
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[
            library(1, 1, "k", "t.Miss", "miss"),
            library(1, 2, "k", "t.Hit", "hit"),
        ],
    )
    .await;
    let (name, data) = tail.next(Duration::from_secs(10)).await.unwrap();
    assert_eq!(name, "record");
    let record: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(record["headers"]["id"], "hit", "{record}");
    assert!(record.get("payload").is_none(), "never a payload: {record}");
    let (name, data) = tail.next(Duration::from_secs(10)).await.unwrap();
    assert_eq!(name, "end");
    assert_eq!(
        serde_json::from_str::<Value>(&data).unwrap()["reason"],
        "maxDuration"
    );
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert!(tail.next(Duration::from_secs(5)).await.is_none(), "closed");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut again = kafka::sse(short, "/api/v1/events/tail", None).await;
    assert_eq!(again.status, 200, "the slot was given back");
    assert_eq!(
        again.next(Duration::from_secs(10)).await.unwrap().0,
        "start"
    );
    drop(again);

    // A reader that does not read: its tail ends, and the slot comes back.
    let (_g, long) = hub(Config {
        event_max_tails: 1,
        ..config(&broker.bootstrap, &[&topic])
    })
    .await;
    let mut slow = kafka::sse(long, "/api/v1/events/tail", None).await;
    assert_eq!(slow.next(Duration::from_secs(10)).await.unwrap().0, "start");
    let bootstrap = broker.bootstrap.clone();
    let flood = topic.clone();
    tokio::task::spawn_blocking(move || kafka::produce_many(&bootstrap, &flood, 0, 30_000, T))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut records = 0;
    let end = loop {
        match slow.next(Duration::from_secs(20)).await {
            Some((name, _)) if name == "record" => records += 1,
            Some((name, data)) if name == "end" => break data,
            Some((name, data)) => panic!("unexpected {name}: {data}"),
            None => panic!("closed without an end after {records} records"),
        }
    };
    assert_eq!(
        serde_json::from_str::<Value>(&end).unwrap()["reason"],
        "slowReader"
    );
    assert!(records < 30_000, "{records}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let next = kafka::sse(long, "/api/v1/events/tail", None).await;
    assert_eq!(next.status, 200);
    drop((tail, slow, next));
    drop(broker);
}

#[tokio::test]
async fn consumer_groups_show_lag_with_and_without_members_and_the_hub_creates_none() {
    let broker = kafka::kafka().await;
    let topic = kafka::unique("groups");
    let other = kafka::unique("other");
    kafka::create_topic(&broker.bootstrap, &topic, 2).await;
    kafka::create_topic(&broker.bootstrap, &other, 1).await;
    let mut records: Vec<Rec> = (0..10).map(|i| library(0, i, "k", "a.B", "e")).collect();
    records.extend((0..5).map(|i| library(1, i, "k", "a.B", "e")));
    kafka::produce(&broker.bootstrap, &topic, &records).await;
    let before = kafka::groups(&broker.bootstrap);

    let bootstrap = broker.bootstrap.clone();
    let (t, o) = (topic.clone(), other.clone());
    let live = tokio::task::spawn_blocking(move || {
        kafka::commit(&bootstrap, "g-empty", &t, &[(0, 4), (1, 5)]);
        kafka::commit(&bootstrap, "g-elsewhere", &o, &[(0, 0)]);
        // Named like the browser's group.id: listed like any other (the hub has none of its own).
        kafka::commit(&bootstrap, "kraken-hub-browser-planted", &o, &[(0, 0)]);
        Member::join(&bootstrap, "g-live", &t, &[(0, 10), (1, 2)])
    })
    .await
    .unwrap();

    let (grpc, http) = hub(config(&broker.bootstrap, &[&topic, &other])).await;
    let (status, view) = get(http, "/api/v1/consumer-groups", None).await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["topic"], topic.as_str());
    assert_eq!(view["truncated"], false);
    let groups = view["groups"].as_array().unwrap();
    let names: Vec<&str> = groups
        .iter()
        .map(|g| g["groupId"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["g-empty", "g-live"], "by lag, descending: {view}");
    let empty = &groups[0];
    assert_eq!(
        (&empty["members"], &empty["lag"]),
        (&Value::from(0), &Value::from(6))
    );
    assert_eq!(empty["state"], "Empty");
    assert_eq!(empty["topic"], topic.as_str());
    assert_eq!(
        empty["partitions"],
        serde_json::json!([
            {"partition": 0, "committed": 4, "highWatermark": 10, "lag": 6},
            {"partition": 1, "committed": 5, "highWatermark": 5, "lag": 0},
        ])
    );
    let live_view = &groups[1];
    assert_eq!(
        (&live_view["members"], &live_view["lag"]),
        (&Value::from(1), &Value::from(3))
    );
    assert_eq!(live_view["state"], "Stable");
    let (_, elsewhere) = get(
        http,
        &format!("/api/v1/consumer-groups?topic={other}"),
        None,
    )
    .await;
    let names: Vec<&str> = elsewhere["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["groupId"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["g-elsewhere", "kraken-hub-browser-planted"]);
    let (status, _) = get(http, "/api/v1/consumer-groups?topic=nope", None).await;
    assert_eq!(status, 400);

    // Everything the browser does, then the broker's groups: only the test's own are new.
    let _ = get(http, "/api/v1/events?limit=5", None).await;
    let _ = get(http, &format!("/api/v1/events/{topic}/0/0"), None).await;
    let mut tail = kafka::sse(http, "/api/v1/events/tail", None).await;
    assert_eq!(tail.next(Duration::from_secs(10)).await.unwrap().0, "start");
    kafka::produce(
        &broker.bootstrap,
        &topic,
        &[library(0, 99, "k", "a.B", "x")],
    )
    .await;
    assert_eq!(
        tail.next(Duration::from_secs(10)).await.unwrap().0,
        "record"
    );
    drop(tail);
    let _ = get(http, "/api/v1/consumer-groups", None).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = kafka::groups(&broker.bootstrap);
    let new: Vec<&String> = after.difference(&before).collect();
    assert_eq!(
        new,
        [
            "g-elsewhere",
            "g-empty",
            "g-live",
            "kraken-hub-browser-planted"
        ],
        "{after:?}"
    );
    assert!(
        after
            .iter()
            .all(|g| !g.starts_with("kraken-hub") || g == "kraken-hub-browser-planted"),
        "{after:?}"
    );
    drop((live, grpc));
    drop(broker);
}

/// A bootstrap address nothing listens on.
fn closed_port() -> String {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    format!("127.0.0.1:{port}")
}

#[tokio::test]
async fn an_unreachable_kafka_answers_503_and_routing_goes_on() {
    let (grpc, http) = hub(Config {
        event_search_timeout: Duration::from_secs(2),
        ..config(&closed_port(), &["kraken.events"])
    })
    .await;
    let searches: Vec<_> = [
        "/api/v1/events",
        "/api/v1/events/kraken.events/0/0",
        "/api/v1/consumer-groups",
        "/api/v1/events/tail",
    ]
    .into_iter()
    .map(|path| tokio::spawn(async move { (path, get(http, path, None).await) }))
    .collect();

    // While the browser waits for Kafka, a query is routed as always.
    let mut handler = FakeClient::register(grpc, "service-b", &[], &["b.Count"]).await;
    let mut sender = FakeClient::register(grpc, "service-a", &[], &[]).await;
    let routed = Instant::now();
    sender
        .tx
        .send(FakeClient::request(
            "r1",
            RequestKind::Query,
            "b.Count",
            "",
            0,
        ))
        .await
        .unwrap();
    loop {
        match handler.next().await {
            Some(Ok(m)) => match m.message {
                Some(server_message::Message::Request(_)) => break,
                _ => continue,
            },
            other => panic!("{other:?}"),
        }
    }
    handler
        .tx
        .send(FakeClient::reply_ok("r1", b"1"))
        .await
        .unwrap();
    loop {
        match sender.next().await {
            Some(Ok(m)) => match m.message {
                Some(server_message::Message::Reply(_)) => break,
                _ => continue,
            },
            other => panic!("{other:?}"),
        }
    }
    assert!(
        routed.elapsed() < Duration::from_secs(1),
        "{:?}",
        routed.elapsed()
    );
    assert!(
        kraken_hub::health_probe(grpc).await.unwrap(),
        "still SERVING"
    );
    let (status, _) = get(http, "/api/v1/topology", None).await;
    assert_eq!(status, 200);

    for search in searches {
        let (path, (status, body)) = search.await.unwrap();
        assert_eq!(status, 503, "{path}: {body}");
        assert!(body["error"].as_str().unwrap().contains("Kafka"), "{body}");
    }
}

/// A tail request that is dropped while its Kafka setup still runs keeps its slot until that
/// setup ends: opening and aborting tails never runs more setups than the tail limit.
#[tokio::test]
async fn an_aborted_tail_keeps_its_slot_until_its_kafka_setup_ends() {
    use tokio::io::AsyncWriteExt;
    let budget = Duration::from_secs(3);
    let (_g, http) = hub(Config {
        event_search_timeout: budget,
        event_max_tails: 4,
        ..config(&closed_port(), &["kraken.events"])
    })
    .await;
    let opened = Instant::now();
    for _ in 0..8 {
        let mut client = tokio::net::TcpStream::connect(http).await.unwrap();
        let request = format!("GET /api/v1/events/tail HTTP/1.1\r\nHost: {http}\r\n\r\n");
        client.write_all(request.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(client);
    }
    // Every setup still waits for Kafka: a fifth tail is refused.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let busy = http_send(
        http,
        "GET",
        "/api/v1/events/tail",
        &[],
        b"",
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(busy.status, 429, "{}", busy.body);
    assert!(opened.elapsed() < budget, "{:?}", opened.elapsed());
    // The setups end at their budget and give their slots back.
    tokio::time::sleep_until((opened + budget + Duration::from_secs(1)).into()).await;
    let (status, body) = get(http, "/api/v1/events/tail", None).await;
    assert_eq!(status, 503, "{body}");
}

/// A broker that accepts connections and never answers: every call is 503 at its budget, and its
/// consumer closes within the close grace, so the slot is free again by the budget plus the grace.
#[tokio::test]
async fn a_silent_broker_frees_every_slot_within_the_budget_and_the_close_grace() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bootstrap = silent.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in silent.incoming() {
            held.push(stream);
        }
    });
    let budget = Duration::from_secs(2);
    let (_g, http) = hub(Config {
        event_search_timeout: budget,
        event_max_searches: 1,
        event_max_tails: 1,
        ..config(&bootstrap, &["kraken.events"])
    })
    .await;
    for path in [
        "/api/v1/events",
        "/api/v1/events/kraken.events/0/0",
        "/api/v1/consumer-groups",
        "/api/v1/events/tail",
    ] {
        let started = Instant::now();
        let (status, body) = get(http, path, None).await;
        assert_eq!(status, 503, "{path}: {body}");
        let freed = started + budget + kraken_hub::browser::CLOSE_GRACE;
        tokio::time::sleep_until(freed.into()).await;
        let (status, body) = get(http, path, None).await;
        assert_eq!(status, 503, "{path}: the slot is free again: {body}");
    }
}

#[tokio::test]
async fn without_a_bootstrap_the_browser_is_off() {
    let (_g, http) = hub(Config {
        otlp_endpoint: None,
        ..Config::default()
    })
    .await;
    for path in [
        "/api/v1/events",
        "/api/v1/events/kraken.events/0/0",
        "/api/v1/events/tail",
        "/api/v1/consumer-groups",
    ] {
        let (status, body) = get(http, path, None).await;
        assert_eq!(status, 404, "{path}");
        assert_eq!(
            body["error"],
            "event browser disabled: set KRAKEN_HUB_KAFKA_BOOTSTRAP"
        );
    }
    let (_, version) = get(http, "/api/v1/version", None).await;
    assert_eq!(
        version["eventBrowser"],
        serde_json::json!({"enabled": false, "payloads": false, "topics": []})
    );
    let (_, settings) = get(http, "/api/v1/settings", None).await;
    let names: Vec<&str> = settings["settings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    for name in [
        "KRAKEN_HUB_KAFKA_BOOTSTRAP",
        "KRAKEN_HUB_EVENT_TOPICS",
        "KRAKEN_HUB_EVENT_BROWSER_PAYLOADS",
        "KRAKEN_HUB_EVENT_SEARCH_MAX_SCANNED",
        "KRAKEN_HUB_EVENT_SEARCH_MAX_RESULTS",
        "KRAKEN_HUB_EVENT_SEARCH_TIMEOUT",
        "KRAKEN_HUB_EVENT_MAX_SEARCHES",
        "KRAKEN_HUB_EVENT_MAX_TAILS",
        "KRAKEN_HUB_EVENT_TAIL_DURATION",
    ] {
        assert!(names.contains(&name), "{name} missing");
    }

    // On: the version says so; payloads are viewable only with the switch and a token.
    let (_g, on) = hub(Config {
        event_payloads: true,
        dashboard_token: Some(Secret::new(READ)),
        ..config("kafka:9092", &["kraken.events", "kraken.events.dlt"])
    })
    .await;
    let (_, version) = get(on, "/api/v1/version", Some(READ)).await;
    assert_eq!(
        version["eventBrowser"],
        serde_json::json!({
            "enabled": true,
            "payloads": true,
            "topics": ["kraken.events", "kraken.events.dlt"],
        })
    );
}
