//! Test-only support for the TLS tests (ADR 0018, 0020): a throwaway CA, server certificates and
//! client certificates generated at test time (nothing is checked in).
//!
//! This file is compiled twice, by `#[path]`: as `pki_test_support` into the library's own unit
//! tests, and into the integration tests' `common`. It therefore depends on nothing but external
//! crates.
#![allow(dead_code)]

use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    SanType,
};

/// A throwaway CA that signs server and client certificates.
pub struct Pki {
    ca_params: CertificateParams,
    ca_key: KeyPair,
    pub ca_pem: String,
}

/// What a test client certificate carries.
#[derive(Clone, Default)]
pub struct ClientSpec {
    /// URI SANs, verbatim.
    pub uris: Vec<String>,
    /// DNS SANs.
    pub dns: Vec<String>,
    /// How long from now the certificate stays valid; a year when unset.
    pub valid_for: Option<Duration>,
    /// `serverAuth` only: a certificate that may not be used by a client.
    pub server_auth_only: bool,
}

impl ClientSpec {
    /// A certificate whose only SAN is `kraken://service/<name>`.
    pub fn service(name: &str) -> Self {
        Self {
            uris: vec![format!("kraken://service/{name}")],
            ..Self::default()
        }
    }

    pub fn valid_for(mut self, d: Duration) -> Self {
        self.valid_for = Some(d);
        self
    }
}

impl Pki {
    pub fn new(name: &str) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(DnType::CommonName, name);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca_params,
            ca_key,
        }
    }

    fn issuer(&self) -> Issuer<'_, &KeyPair> {
        Issuer::from_params(&self.ca_params, &self.ca_key)
    }

    /// `(certificate PEM, private key PEM)` for a server reachable as `localhost` and `127.0.0.1`.
    pub fn server(&self) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params
            .subject_alt_names
            .push(SanType::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()));
        let cert = params.signed_by(&key, &self.issuer()).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// `(certificate PEM, private key PEM)` for a client described by `spec`.
    pub fn client(&self, spec: &ClientSpec) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "test client");
        for uri in &spec.uris {
            params
                .subject_alt_names
                .push(SanType::URI(uri.as_str().try_into().unwrap()));
        }
        for name in &spec.dns {
            params
                .subject_alt_names
                .push(SanType::DnsName(name.as_str().try_into().unwrap()));
        }
        params.extended_key_usages = vec![if spec.server_auth_only {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        if let Some(valid_for) = spec.valid_for {
            let now = time::OffsetDateTime::now_utc();
            params.not_before = now - time::Duration::seconds(60);
            params.not_after = now + time::Duration::try_from(valid_for).unwrap();
        }
        let cert = params.signed_by(&key, &self.issuer()).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// The client certificate DER of `spec` (what a peer presents), with its private key PEM.
    pub fn client_der(&self, spec: &ClientSpec) -> (Vec<u8>, String) {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let (pem, key) = self.client(spec);
        let der = CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
        (der.as_ref().to_vec(), key)
    }
}
