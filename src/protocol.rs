//! Protocol version negotiation (ADR 0013).
//!
//! `Register.protocol_version` 0 is protocol v1 (every client before P0 sends nothing). Version 2
//! is the one this hub speaks: the hub answers each accepted `Register` with a `RegisterAck` and
//! applies the short heartbeat timeout. Version 1 was never defined and reads as v1; versions
//! above 2 are answered as 2.

use std::fmt;
use std::time::Duration;

use crate::config::Config;
use crate::context::ContextId;
use crate::proto::{RegisterAck, ServerMessage, server_message};

/// Never prints the token or the JWT: the kind and `"***"`. The generated `Debug` is skipped for
/// `Credentials` and its oneof in `build.rs`, because `Register` and `ClientMessage` print it too.
impl fmt::Debug for crate::proto::Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Debug for crate::proto::credentials::Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::proto::credentials::Kind;
        match self {
            Kind::Token(_) => f.debug_tuple("Token").field(&"***").finish(),
            Kind::Jwt(_) => f.debug_tuple("Jwt").field(&"***").finish(),
        }
    }
}

/// The protocol version this hub implements.
pub const HUB_PROTOCOL_VERSION: u32 = 2;

/// Capability of flow control (ADR 0014): permits, per-instance queues, RESOURCE_EXHAUSTED.
pub const FLOW_CONTROL: &str = "flow-control";

/// Capability of multi-hub HA (ADR 0016): the client reports `InstanceStatus` (the hubs it is
/// connected to) on this stream, and the hub shows the latest report in the topology.
pub const INSTANCE_STATUS: &str = "instance-status";

/// Capability of scatter-gather queries (ADR 0017): the client may send `Request.mode`
/// SCATTER_GATHER and receives `ReplyPart` / `ReplyEnd` for it.
pub const SCATTER_GATHER: &str = "scatter-gather";

/// Capability of streaming queries (ADR 0017, P4b): a sender may send `Request.mode` STREAMING
/// and receives `ReplyPart` / `ReplyEnd` for it; a handler receives such requests and answers
/// with parts and an end under the sender's credits (`Flow`), and honours `Cancel`.
pub const STREAMING: &str = "streaming";

/// Capability of subscription queries (ADR 0017, P4c): a sender may send `Request.mode`
/// SUBSCRIPTION and receives the initial result and the pushed updates as `ReplyPart` /
/// `ReplyEnd`; a handler receives such requests (`<id>#n`, mode SUBSCRIPTION) and pushes its
/// updates as parts, ends them with `ReplyEnd`, and honours `Cancel`.
pub const SUBSCRIPTION: &str = "subscription";

/// Optional protocol features this hub supports. A later phase adds `processors`.
pub const HUB_CAPABILITIES: &[&str] = &[
    FLOW_CONTROL,
    INSTANCE_STATUS,
    SCATTER_GATHER,
    STREAMING,
    SUBSCRIPTION,
];

/// True when the client declared `capability` and this hub supports it.
pub fn shares(client: &[String], capability: &str) -> bool {
    HUB_CAPABILITIES.contains(&capability) && client.iter().any(|c| c == capability)
}

/// True when a session that declared `protocol_version` speaks v2 (gets acks, short timeout).
pub fn is_v2(protocol_version: u32) -> bool {
    protocol_version >= 2
}

/// The version both sides speak: v1 below 2, else the lower of the two.
pub fn negotiate(protocol_version: u32) -> u32 {
    if is_v2(protocol_version) {
        protocol_version.min(HUB_PROTOCOL_VERSION)
    } else {
        0
    }
}

/// Capabilities both sides declared, in the hub's order.
pub fn shared_capabilities(client: &[String]) -> Vec<String> {
    HUB_CAPABILITIES
        .iter()
        .filter(|c| client.iter().any(|x| x == *c))
        .map(|c| c.to_string())
        .collect()
}

fn ms(d: Duration) -> u32 {
    u32::try_from(d.as_millis()).unwrap_or(u32::MAX)
}

/// The `RegisterAck` for a v2 session that declared `protocol_version` and `capabilities`.
pub fn register_ack(
    config: &Config,
    ctx: &ContextId,
    protocol_version: u32,
    capabilities: &[String],
) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::RegisterAck(RegisterAck {
            hub_id: config.hub_id.clone(),
            protocol_version: negotiate(protocol_version),
            capabilities: shared_capabilities(capabilities),
            heartbeat_interval_ms: ms(config.heartbeat_interval),
            heartbeat_timeout_ms: ms(config.heartbeat_timeout_for(protocol_version)),
            default_timeout_ms: ms(config.default_timeout),
            max_timeout_ms: ms(config.max_timeout),
            context: ctx.to_string(),
            type_timeouts_ms: config
                .timeout_overrides
                .iter()
                .map(|(t, d)| (t.clone(), ms(*d)))
                .collect(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_carries_the_hub_settings() {
        let config = Config {
            hub_id: "h1".into(),
            timeout_overrides: [("t.X".to_string(), Duration::from_millis(1234))].into(),
            ..Config::default()
        };
        let Some(server_message::Message::RegisterAck(ack)) =
            register_ack(&config, &ContextId::default(), 5, &[]).message
        else {
            panic!("not an ack");
        };
        assert_eq!(ack.hub_id, "h1");
        assert_eq!(ack.protocol_version, 2);
        assert!(ack.capabilities.is_empty());
        assert_eq!(ack.heartbeat_interval_ms, 3000);
        assert_eq!(ack.heartbeat_timeout_ms, 10_000);
        assert_eq!(ack.default_timeout_ms, 5000);
        assert_eq!(ack.max_timeout_ms, 60_000);
        assert_eq!(ack.context, "default");
        assert_eq!(ack.type_timeouts_ms, [("t.X".to_string(), 1234)].into());
    }

    #[test]
    fn versions_below_two_are_v1_and_higher_ones_negotiate_down() {
        assert!(!is_v2(0));
        assert!(!is_v2(1));
        assert!(is_v2(2));
        assert_eq!(negotiate(0), 0);
        assert_eq!(negotiate(1), 0);
        assert_eq!(negotiate(2), 2);
        assert_eq!(negotiate(9), 2);
    }

    #[test]
    fn instance_status_is_shared_when_declared() {
        let client = ["instance-status".to_string(), "flow-control".to_string()];
        assert_eq!(
            shared_capabilities(&client),
            ["flow-control", "instance-status"]
        );
        assert!(shares(&client, INSTANCE_STATUS));
        let Some(server_message::Message::RegisterAck(ack)) =
            register_ack(&Config::default(), &ContextId::default(), 2, &client).message
        else {
            panic!("not an ack");
        };
        assert!(ack.capabilities.iter().any(|c| c == "instance-status"));
    }

    #[test]
    fn scatter_gather_is_shared_when_declared() {
        let client = ["scatter-gather".to_string()];
        assert_eq!(shared_capabilities(&client), ["scatter-gather"]);
        assert!(shares(&client, SCATTER_GATHER));
        assert!(!shares(&[], SCATTER_GATHER));
    }

    #[test]
    fn streaming_is_shared_when_declared() {
        let client = ["streaming".to_string(), "scatter-gather".to_string()];
        assert_eq!(
            shared_capabilities(&client),
            ["scatter-gather", "streaming"]
        );
        assert!(shares(&client, STREAMING));
        assert!(!shares(&["scatter-gather".to_string()], STREAMING));
    }

    #[test]
    fn subscription_is_shared_when_declared() {
        let client = ["subscription".to_string(), "streaming".to_string()];
        assert_eq!(shared_capabilities(&client), ["streaming", "subscription"]);
        assert!(shares(&client, SUBSCRIPTION));
        assert!(!shares(&["streaming".to_string()], SUBSCRIPTION));
    }

    #[test]
    fn only_declared_capabilities_the_hub_knows_are_shared() {
        let client = ["made-up".to_string(), "flow-control".to_string()];
        assert_eq!(shared_capabilities(&client), ["flow-control"]);
        assert!(shares(&client, FLOW_CONTROL));
        assert!(!shares(&["made-up".to_string()], FLOW_CONTROL));
        assert!(!shares(&client, "made-up"));
    }

    #[test]
    fn credentials_and_register_never_print_the_token_in_debug() {
        use crate::proto::{ClientMessage, Credentials, Register, client_message, credentials};
        for (kind, secret) in [
            (
                credentials::Kind::Token("s3cret-static-token".into()),
                "s3cret-static-token",
            ),
            (
                credentials::Kind::Jwt("eyJhbGciOi.payload.sig".into()),
                "eyJhbGciOi.payload.sig",
            ),
        ] {
            let credentials = Credentials { kind: Some(kind) };
            let register = Register {
                service_name: "orders".into(),
                instance_id: "i1".into(),
                credentials: Some(credentials.clone()),
                ..Register::default()
            };
            let message = ClientMessage {
                message: Some(client_message::Message::Register(register.clone())),
            };
            for text in [
                format!("{credentials:?}"),
                format!("{register:?}"),
                format!("{message:?}"),
                format!("{register:#?}"),
            ] {
                assert!(!text.contains(secret), "{text}");
                assert!(text.contains("***"), "{text}");
            }
            // The kind stays visible, and the rest of the Register still prints.
            let text = format!("{register:?}");
            assert!(text.contains("orders") && text.contains("i1"), "{text}");
        }
        let text = format!(
            "{:?}",
            Credentials {
                kind: Some(credentials::Kind::Token("x".into()))
            }
        );
        assert!(text.contains("Token"), "{text}");
        let text = format!(
            "{:?}",
            Credentials {
                kind: Some(credentials::Kind::Jwt("x".into()))
            }
        );
        assert!(text.contains("Jwt"), "{text}");
    }
}
