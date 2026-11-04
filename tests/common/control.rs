//! A fake P6b instance for the processor control tests (ADR 0023): it registers over real gRPC,
//! declares "instance-status", "processors" and (unless told otherwise) "processor-control",
//! reports its processors, heartbeats, and answers every `ControlRequest` as its behaviour says:
//! at once, after a delay, or never. It records every request it was sent.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_hub::proto::{
    ClientMessage, ControlRefusal, ControlReply, ControlRequest, Credentials, Heartbeat,
    InstanceStatus, ProcessorState, ProcessorStatus, Register, ServerMessage, client_message,
    control_reply, control_request, credentials, server_message,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::FakeClient;

/// What the fake does with one request.
pub enum Reaction {
    Reply(control_reply::Result),
    After(Duration, control_reply::Result),
    /// Never answers.
    Silent,
}

pub type Behaviour = Arc<dyn Fn(&ControlRequest) -> Reaction + Send + Sync>;

/// Answers `ok` to everything but a reset execute, which it answers with a replay of partition 0
/// from 0 to 42 (3 markers deleted, 1 reset handler), and the dead-letter calls, which it answers
/// with an empty page, `Retried` and zero counts.
pub fn ok_to_everything() -> Behaviour {
    Arc::new(|req: &ControlRequest| Reaction::Reply(default_result(req)))
}

/// The answer [`ok_to_everything`] gives to `req`.
pub fn default_result(req: &ControlRequest) -> control_reply::Result {
    use control_reply::Result as R;
    use control_request::Action as A;
    match &req.action {
        Some(A::ResetExecute(_)) => R::Reset(kraken_hub::proto::ResetOutcome {
            ranges: vec![kraken_hub::proto::ReplayRange {
                partition: 0,
                from: 0,
                until: 42,
            }],
            markers_deleted: 3,
            reset_handlers: 1,
        }),
        Some(A::DeadLetterList(_)) => R::DeadLetters(kraken_hub::proto::DeadLetterPage {
            letters: vec![],
            total: 0,
        }),
        Some(A::DeadLetterRetry(_)) => R::Retry(kraken_hub::proto::DeadLetterRetryOutcome {
            result: "Retried".into(),
            detail: String::new(),
        }),
        Some(A::DeadLetterRetryAll(_)) => {
            R::RetryAll(kraken_hub::proto::DeadLetterRetryAllOutcome::default())
        }
        _ => ok(),
    }
}

pub fn ok() -> control_reply::Result {
    control_reply::Result::Ok(kraken_hub::proto::ControlOk {
        detail: String::new(),
    })
}

pub fn refused(code: ControlRefusal, message: &str) -> control_reply::Result {
    control_reply::Result::Refused(kraken_hub::proto::ControlRefused {
        code: code as i32,
        message: message.to_string(),
    })
}

/// A processor as a P6b instance reports it.
pub fn processor(name: &str) -> ProcessorStatus {
    ProcessorStatus {
        name: name.to_string(),
        group_id: format!("svc.{name}"),
        topic: "kraken.events".to_string(),
        state: ProcessorState::ProcessorRunning as i32,
        dead_letter_mode: "collection".to_string(),
        ..Default::default()
    }
}

pub struct ControlClient {
    pub instance_id: String,
    tx: mpsc::Sender<ClientMessage>,
    seen: Arc<Mutex<Vec<ControlRequest>>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for ControlClient {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl ControlClient {
    /// Registers `instance_id` of `service` (with the ACL `token`, if any), reports `processors`
    /// (names) and returns once the hub holds the report.
    pub async fn start(
        addr: SocketAddr,
        service: &str,
        instance_id: &str,
        processors: &[&str],
        token: Option<&str>,
        behaviour: Behaviour,
    ) -> Self {
        let mut client = FakeClient::connect(addr).await;
        client.instance_id = instance_id.to_string();
        client
            .send_raw_register(Register {
                credentials: token.map(|t| Credentials {
                    kind: Some(credentials::Kind::Token(t.to_string())),
                }),
                ..client.register_message(service, &[], &[])
            })
            .await;
        let ack = client.expect_ack().await;
        client
            .status_with_processors(
                &[&ack.hub_id],
                processors.iter().map(|n| processor(n)).collect(),
            )
            .await;
        // The echo of a heartbeat sent after the report: the hub holds the report.
        client.heartbeat(7).await;
        loop {
            match client.next().await {
                Some(Ok(ServerMessage {
                    message: Some(server_message::Message::Heartbeat(h)),
                })) if h.sent_at_ms == 7 => break,
                Some(Ok(_)) => {}
                other => panic!("{instance_id}: {other:?}"),
            }
        }
        let FakeClient { tx, mut rx, .. } = client;
        let seen: Arc<Mutex<Vec<ControlRequest>>> = Arc::default();
        let reader = {
            let (tx, seen) = (tx.clone(), Arc::clone(&seen));
            tokio::spawn(async move {
                while let Ok(Some(msg)) = rx.message().await {
                    let Some(server_message::Message::Control(req)) = msg.message else {
                        continue;
                    };
                    seen.lock().unwrap().push(req.clone());
                    let (delay, result) = match behaviour(&req) {
                        Reaction::Reply(r) => (Duration::ZERO, r),
                        Reaction::After(d, r) => (d, r),
                        Reaction::Silent => continue,
                    };
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let reply = ClientMessage {
                            message: Some(client_message::Message::ControlReply(ControlReply {
                                control_id: req.control_id,
                                result: Some(result),
                            })),
                        };
                        let _ = tx.send(reply).await;
                    });
                }
            })
        };
        let heartbeat = {
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let beat = ClientMessage {
                        message: Some(client_message::Message::Heartbeat(Heartbeat {
                            sent_at_ms: 0,
                        })),
                    };
                    if tx.send(beat).await.is_err() {
                        return;
                    }
                }
            })
        };
        Self {
            instance_id: instance_id.to_string(),
            tx,
            seen,
            tasks: vec![reader, heartbeat],
        }
    }

    /// An instance of `service` reporting `processors`, answering with `behaviour`.
    pub async fn new(
        addr: SocketAddr,
        service: &str,
        instance_id: &str,
        processors: &[&str],
        behaviour: Behaviour,
    ) -> Self {
        Self::start(addr, service, instance_id, processors, None, behaviour).await
    }

    /// Every control request this instance was sent, in order.
    pub fn seen(&self) -> Vec<ControlRequest> {
        self.seen.lock().unwrap().clone()
    }

    /// Reports `processors` again (a fresher status).
    pub async fn report(&self, processors: &[&str]) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Status(InstanceStatus {
                connected_hub_ids: vec![],
                handlers_in_flight: 0,
                processors: processors.iter().map(|n| processor(n)).collect(),
            })),
        };
        self.tx.send(msg).await.unwrap();
    }

    /// Waits until this instance was sent `n` requests (at most `within`).
    pub async fn wait_for(&self, n: usize, within: Duration) -> Vec<ControlRequest> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let seen = self.seen();
            if seen.len() >= n {
                return seen;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{}: {} requests, wanted {n}: {seen:?}",
                self.instance_id,
                seen.len()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The action name of a request, for compact assertions.
pub fn action_of(req: &ControlRequest) -> &'static str {
    use control_request::Action as A;
    match &req.action {
        Some(A::Pause(_)) => "pause",
        Some(A::Resume(_)) => "resume",
        Some(A::ResetPrepare(_)) => "prepare",
        Some(A::ResetExecute(_)) => "execute",
        Some(A::ResetFinish(f)) if f.aborted => "finish-aborted",
        Some(A::ResetFinish(_)) => "finish",
        Some(A::DeadLetterList(_)) => "list",
        Some(A::DeadLetterRetry(_)) => "retry",
        Some(A::DeadLetterRetryAll(_)) => "retry-all",
        Some(A::DeadLetterDiscard(_)) => "discard",
        None => "none",
    }
}

/// The actions of `requests`, in order.
pub fn actions(requests: &[ControlRequest]) -> Vec<&'static str> {
    requests.iter().map(action_of).collect()
}
