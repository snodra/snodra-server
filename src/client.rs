//! The gRPC client side of the binary's own modes (`--health-check`, `--topology`): where to
//! connect and how to trust the hub (roadmap P5a, ADR 0018).
//!
//! A [`Target`] speaks TLS when its address is `https://...`, when `--tls-ca <pem>` is given, or
//! when it is the local hub and the hub's environment configures TLS. It then verifies the
//! server against the given CA, against the hub's own certificate (the local default, pinned,
//! because `127.0.0.1` is rarely in a certificate), or against the system roots.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring::default_provider;
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint};

/// Whom to trust when the connection uses TLS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trust {
    /// The certificates in this PEM file are the roots.
    Ca(PathBuf),
    /// Only a server that presents the first certificate of this PEM file (the hub's own, for
    /// the probes of the hub's own container).
    Pinned(PathBuf),
    /// The operating system's roots.
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// `host:port`.
    pub address: String,
    /// `None`: plain text.
    pub tls: Option<Trust>,
}

impl Target {
    /// Plain text to `addr`.
    pub fn plain(addr: impl std::fmt::Display) -> Self {
        Self {
            address: addr.to_string(),
            tls: None,
        }
    }

    /// Resolves the CLI's `--address` and `--tls-ca`. `local_port` and `local_cert` describe the
    /// hub's own environment: with no `--address` the target is `127.0.0.1:<local_port>`, and
    /// when `local_cert` is set (the hub serves TLS) it is pinned unless `--tls-ca` says
    /// otherwise.
    pub fn resolve(
        address: Option<&str>,
        ca: Option<&Path>,
        local_port: u16,
        local_cert: Option<&Path>,
    ) -> Self {
        let (address, scheme_tls, local) = match address {
            Some(a) => match a.strip_prefix("https://") {
                Some(rest) => (rest, true, false),
                None => (a.strip_prefix("http://").unwrap_or(a), false, false),
            },
            None => ("", false, true),
        };
        let address = if local {
            format!("127.0.0.1:{local_port}")
        } else {
            address.trim_end_matches('/').to_string()
        };
        let tls = match (ca, local, local_cert, scheme_tls) {
            (Some(ca), ..) => Some(Trust::Ca(ca.to_path_buf())),
            (None, true, Some(cert), _) => Some(Trust::Pinned(cert.to_path_buf())),
            (None, _, _, true) => Some(Trust::System),
            _ => None,
        };
        Self { address, tls }
    }

    fn host(&self) -> &str {
        match self.address.rsplit_once(':') {
            Some((host, _)) => host.trim_start_matches('[').trim_end_matches(']'),
            None => &self.address,
        }
    }

    /// Connects (the caller bounds the time).
    pub async fn connect(&self) -> anyhow::Result<Channel> {
        let Some(trust) = &self.tls else {
            return Ok(Endpoint::from_shared(format!("http://{}", self.address))?
                .connect()
                .await?);
        };
        let connector = TlsConnector::from(client_config(trust)?);
        let name = ServerName::try_from(self.host().to_string())
            .with_context(|| format!("{} is not a valid server name", self.host()))?;
        let address = self.address.clone();
        let channel = Endpoint::from_shared(format!("https://{}", self.address))?
            .connect_with_connector(tower::service_fn(move |_uri: tonic::codegen::http::Uri| {
                let (connector, name, address) = (connector.clone(), name.clone(), address.clone());
                async move {
                    let tcp = TcpStream::connect(&address).await?;
                    let tls = connector.connect(name, tcp).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(tls))
                }
            }))
            .await?;
        Ok(channel)
    }
}

/// The rustls client configuration for `trust`; HTTP/2 only (this is for gRPC).
pub fn client_config(trust: &Trust) -> anyhow::Result<Arc<ClientConfig>> {
    let provider = Arc::new(default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let mut config = match trust {
        Trust::Ca(path) => builder
            .with_root_certificates(roots_from_pem(path)?)
            .with_no_client_auth(),
        Trust::System => {
            let mut roots = RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            let (added, _ignored) = roots.add_parsable_certificates(native.certs);
            if added == 0 {
                anyhow::bail!("no system root certificates found; pass --tls-ca <pem>");
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        Trust::Pinned(path) => {
            let leaf = CertificateDer::pem_file_iter(path)
                .with_context(|| format!("read {}", path.display()))?
                .next()
                .with_context(|| format!("no certificate in {}", path.display()))?
                .with_context(|| format!("parse {}", path.display()))?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Pinned {
                    leaf,
                    algs: provider.signature_verification_algorithms,
                }))
                .with_no_client_auth()
        }
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

fn roots_from_pem(path: &Path) -> anyhow::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let mut n = 0;
    for cert in CertificateDer::pem_file_iter(path)
        .with_context(|| format!("read CA {}", path.display()))?
    {
        roots.add(cert.with_context(|| format!("parse CA {}", path.display()))?)?;
        n += 1;
    }
    if n == 0 {
        anyhow::bail!("no certificate in {}", path.display());
    }
    Ok(roots)
}

/// Accepts exactly one certificate (the hub's own), whatever name it was reached by, and checks
/// the handshake signatures with it.
#[derive(Debug)]
struct Pinned {
    leaf: CertificateDer<'static>,
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.leaf.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_resolve_from_the_cli_arguments() {
        let ca = Path::new("/ca.pem");
        let cert = Path::new("/hub.pem");
        // Local default, plain.
        assert_eq!(
            Target::resolve(None, None, 7000, None),
            Target {
                address: "127.0.0.1:7000".into(),
                tls: None
            }
        );
        // Local default with TLS configured: pinned to the hub's own certificate.
        assert_eq!(
            Target::resolve(None, None, 7000, Some(cert)).tls,
            Some(Trust::Pinned(cert.into()))
        );
        // --tls-ca wins, and turns TLS on for a plain address.
        assert_eq!(
            Target::resolve(Some("hub:7000"), Some(ca), 7000, None),
            Target {
                address: "hub:7000".into(),
                tls: Some(Trust::Ca(ca.into()))
            }
        );
        // https:// alone: the system roots.
        assert_eq!(
            Target::resolve(Some("https://hub.example:7000/"), None, 7000, None),
            Target {
                address: "hub.example:7000".into(),
                tls: Some(Trust::System)
            }
        );
        // http:// and a bare address stay plain, whatever the local environment says.
        assert_eq!(
            Target::resolve(Some("http://hub:7000"), None, 7000, Some(cert)).tls,
            None
        );
        assert_eq!(
            Target::resolve(Some("hub:7000"), None, 7000, Some(cert)).tls,
            None
        );
        assert_eq!(
            Target::resolve(Some("[::1]:7000"), None, 7000, None).host(),
            "::1"
        );
    }
}
