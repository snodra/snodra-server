//! The hub's side of the handshake: the `RegisterAck` that answers every accepted `Register`, and
//! a `Debug` for credentials that never prints them.

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

fn ms(d: Duration) -> u32 {
    u32::try_from(d.as_millis()).unwrap_or(u32::MAX)
}

/// The `RegisterAck` for every accepted `Register`.
pub fn register_ack(config: &Config, ctx: &ContextId) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::RegisterAck(RegisterAck {
            hub_id: config.hub_id.clone(),
            heartbeat_interval_ms: ms(config.heartbeat_interval),
            heartbeat_timeout_ms: ms(config.heartbeat_timeout),
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
            register_ack(&config, &ContextId::default()).message
        else {
            panic!("not an ack");
        };
        assert_eq!(ack.hub_id, "h1");
        assert_eq!(ack.heartbeat_interval_ms, 3000);
        assert_eq!(ack.heartbeat_timeout_ms, 10_000);
        assert_eq!(ack.default_timeout_ms, 5000);
        assert_eq!(ack.max_timeout_ms, 60_000);
        assert_eq!(ack.context, "default");
        assert_eq!(ack.type_timeouts_ms, [("t.X".to_string(), 1234)].into());
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
            // A refresh carries the credential too.
            let refresh = ClientMessage {
                message: Some(client_message::Message::Credentials(credentials.clone())),
            };
            for text in [
                format!("{credentials:?}"),
                format!("{register:?}"),
                format!("{message:?}"),
                format!("{register:#?}"),
                format!("{refresh:?}"),
                format!("{refresh:#?}"),
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
