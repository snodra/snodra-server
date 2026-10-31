//! Protocol version negotiation (ADR 0013).
//!
//! `Register.protocol_version` 0 is protocol v1 (every client before P0 sends nothing). Version 2
//! is the one this hub speaks: the hub answers each accepted `Register` with a `RegisterAck` and
//! applies the short heartbeat timeout. Version 1 was never defined and reads as v1; versions
//! above 2 are answered as 2.

use std::time::Duration;

use crate::config::Config;
use crate::context::ContextId;
use crate::proto::{RegisterAck, ServerMessage, server_message};

/// The protocol version this hub implements.
pub const HUB_PROTOCOL_VERSION: u32 = 2;

/// Optional protocol features this hub supports. None are defined in P0; later phases add
/// `flow-control`, `scatter-gather`, `streaming`, `subscription` and `processors`.
pub const HUB_CAPABILITIES: &[&str] = &[];

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
    fn no_capability_is_shared_yet() {
        assert!(shared_capabilities(&["flow-control".to_string()]).is_empty());
    }
}
