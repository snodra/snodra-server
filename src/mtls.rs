//! The identity a client certificate carries: exactly one URI SAN of the
//! form `snodra://service/<name>` names the service the certificate authenticates.
//!
//! The TLS handshake has already verified the chain against the client CA and the validity
//! period when this runs; this module only reads what the certificate says. Parsing is pure Rust
//! (`x509-parser`), so the image still needs no C toolchain.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest as _, Sha256};

/// The prefix of the URI SAN that names a service.
pub const SERVICE_SAN_PREFIX: &str = "snodra://service/";

/// The longest service name a certificate may carry (the hub clips names in logs at the same
/// length).
pub const MAX_SERVICE_NAME: usize = 128;

/// Why a certificate names no service; the `result` label of
/// `snodra_server_client_certificates_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertError {
    /// No URI SAN of the form, a name outside the allowed characters, or a certificate that does
    /// not parse.
    NoService,
    /// More than one URI SAN of the form: the certificate would name several services.
    MultipleServices,
}

impl CertError {
    pub fn result(self) -> &'static str {
        match self {
            Self::NoService => "no_service",
            Self::MultipleServices => "multiple_services",
        }
    }

    /// The `reason` label of `snodra_server_auth_violations_total`.
    pub fn reason(self) -> &'static str {
        match self {
            Self::NoService => "cert_no_service",
            Self::MultipleServices => "cert_multiple_services",
        }
    }

    /// What a refused `Register` tells the client.
    pub fn message(self) -> &'static str {
        match self {
            Self::NoService => "certificate names no service",
            Self::MultipleServices => "certificate names more than one service",
        }
    }
}

/// The certificate a connection's TLS peer presented: the DER of its leaf, or none. It travels as
/// the connection info of the gRPC server, so every request on the connection can read it from
/// its extensions.
#[derive(Debug, Clone, Default)]
pub struct PeerCertificate(pub Option<Arc<[u8]>>);

impl PeerCertificate {
    /// The leaf DER, when the peer presented a certificate.
    pub fn der(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }
}

/// What the hub keeps of a client certificate: never the certificate itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCert {
    /// The service the certificate names.
    pub service: String,
    /// The end of its validity (`notAfter`).
    pub not_after: SystemTime,
    /// SHA-256 of the DER encoding.
    pub fingerprint: [u8; 32],
}

impl ClientCert {
    /// The first 8 hex characters of the fingerprint: the only form of the certificate's identity
    /// that is logged or shown.
    pub fn short(&self) -> String {
        short_fingerprint(&self.fingerprint)
    }
}

pub fn short_fingerprint(fingerprint: &[u8; 32]) -> String {
    fingerprint[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A service name a certificate may carry: 1 to [`MAX_SERVICE_NAME`] characters of
/// `[A-Za-z0-9._-]`.
pub fn valid_service_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_SERVICE_NAME
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Reads the identity of the DER certificate `der`.
///
/// Every URI SAN that starts with [`SERVICE_SAN_PREFIX`] counts as a claim to a service (the
/// scheme and the path segment are matched exactly, case included): more than one is
/// [`CertError::MultipleServices`] even when they agree, one with a name outside
/// [`valid_service_name`] is [`CertError::NoService`]. Every other SAN is ignored.
pub fn identify(der: &[u8]) -> Result<ClientCert, CertError> {
    use x509_parser::extensions::GeneralName;
    use x509_parser::parse_x509_certificate;

    let (rest, cert) = parse_x509_certificate(der).map_err(|_| CertError::NoService)?;
    if !rest.is_empty() {
        return Err(CertError::NoService);
    }
    let sans = cert
        .subject_alternative_name()
        .map_err(|_| CertError::NoService)?;
    let mut claims = sans
        .iter()
        .flat_map(|san| &san.value.general_names)
        .filter_map(|name| match name {
            GeneralName::URI(uri) => uri.strip_prefix(SERVICE_SAN_PREFIX),
            _ => None,
        });
    let service = claims.next().ok_or(CertError::NoService)?;
    if claims.next().is_some() {
        return Err(CertError::MultipleServices);
    }
    if !valid_service_name(service) {
        return Err(CertError::NoService);
    }
    let not_after = u64::try_from(cert.validity().not_after.timestamp())
        .map(|secs| UNIX_EPOCH + Duration::from_secs(secs))
        .unwrap_or(UNIX_EPOCH);
    Ok(ClientCert {
        service: service.to_string(),
        not_after,
        fingerprint: Sha256::digest(der).into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pki_test_support::{ClientSpec, Pki};

    fn der(spec: &ClientSpec) -> Vec<u8> {
        Pki::new("test CA").client_der(spec).0
    }

    #[test]
    fn one_snodra_service_uri_names_the_service() {
        let cert = identify(&der(&ClientSpec::service("billing-service"))).unwrap();
        assert_eq!(cert.service, "billing-service");
    }

    #[test]
    fn no_uri_san_names_no_service() {
        let spec = ClientSpec::default();
        assert_eq!(identify(&der(&spec)), Err(CertError::NoService));
    }

    #[test]
    fn dns_sans_alone_name_no_service_and_are_ignored_next_to_one_uri() {
        let dns_only = ClientSpec {
            dns: vec!["billing-service".into()],
            ..ClientSpec::default()
        };
        assert_eq!(identify(&der(&dns_only)), Err(CertError::NoService));
        let both = ClientSpec {
            dns: vec!["billing.example".into()],
            ..ClientSpec::service("billing")
        };
        assert_eq!(identify(&der(&both)).unwrap().service, "billing");
    }

    #[test]
    fn a_uri_that_is_not_a_snodra_service_is_ignored() {
        for uri in [
            "spiffe://example.org/ns/prod/sa/billing",
            "https://billing.example/",
            "snodra://other/billing",
            "snodra://service",
            "SNODRA://service/billing",
        ] {
            let spec = ClientSpec {
                uris: vec![uri.into()],
                ..ClientSpec::default()
            };
            assert_eq!(identify(&der(&spec)), Err(CertError::NoService), "{uri}");
        }
        let one_among_others = ClientSpec {
            uris: vec![
                "spiffe://example.org/billing".into(),
                "snodra://service/billing".into(),
            ],
            ..ClientSpec::default()
        };
        assert_eq!(
            identify(&der(&one_among_others)).unwrap().service,
            "billing"
        );
    }

    #[test]
    fn two_service_uris_are_ambiguous_even_when_they_agree() {
        for uris in [
            ["snodra://service/a", "snodra://service/b"],
            ["snodra://service/a", "snodra://service/a"],
        ] {
            let spec = ClientSpec {
                uris: uris.iter().map(|u| u.to_string()).collect(),
                ..ClientSpec::default()
            };
            assert_eq!(identify(&der(&spec)), Err(CertError::MultipleServices));
        }
    }

    #[test]
    fn a_name_outside_the_allowed_characters_names_no_service() {
        for name in ["", "a b", "a/b", "a?x=1", "a#f", "a%41", &"x".repeat(129)] {
            let spec = ClientSpec {
                uris: vec![format!("snodra://service/{name}")],
                ..ClientSpec::default()
            };
            assert_eq!(identify(&der(&spec)), Err(CertError::NoService), "{name:?}");
        }
        assert!(identify(&der(&ClientSpec::service(&"x".repeat(128)))).is_ok());
        assert!(identify(&der(&ClientSpec::service("a.b_c-D9"))).is_ok());
    }

    #[test]
    fn a_malformed_certificate_names_no_service() {
        assert_eq!(identify(b"not a certificate"), Err(CertError::NoService));
        assert_eq!(identify(&[]), Err(CertError::NoService));
        let mut cut = der(&ClientSpec::service("a"));
        cut.truncate(cut.len() / 2);
        assert_eq!(identify(&cut), Err(CertError::NoService));
    }

    #[test]
    fn the_certificate_reports_not_after_and_a_fingerprint_of_its_der() {
        let spec = ClientSpec::service("a").valid_for(Duration::from_secs(3600));
        let bytes = der(&spec);
        let cert = identify(&bytes).unwrap();
        let left = cert
            .not_after
            .duration_since(SystemTime::now())
            .expect("not after is in the future");
        assert!(
            left > Duration::from_secs(3590) && left <= Duration::from_secs(3600),
            "{left:?}"
        );
        let expected: [u8; 32] = Sha256::digest(&bytes).into();
        assert_eq!(cert.fingerprint, expected);
        assert_eq!(cert.short().len(), 8);
        assert!(cert.short().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn identifying_ignores_the_subject_and_validity_start() {
        // Two certificates for one service differ in fingerprint, not in identity.
        let a = identify(&der(&ClientSpec::service("a"))).unwrap();
        let b = identify(&der(&ClientSpec::service("a"))).unwrap();
        assert_eq!(a.service, b.service);
        assert_ne!(a.fingerprint, b.fingerprint);
    }
}
