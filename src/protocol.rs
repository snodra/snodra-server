//! Protocol version negotiation (ADR 0013).
//!
//! `Register.protocol_version` 0 is protocol v1 (every client before P0 sends nothing). Version 2
//! is the one this hub speaks: the hub answers each accepted `Register` with a `RegisterAck` and
//! applies the short heartbeat timeout. Version 1 was never defined and reads as v1; versions
//! above 2 are answered as 2.

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

#[cfg(test)]
mod tests {
    use super::*;

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
