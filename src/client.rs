//! The gRPC client side of the binary's own modes (`--health-check`, `--topology`): where to
//! connect and how to trust the hub.
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

/// The client certificate and key (PEM files) a probe presents to a hub that asks for one
///.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl ClientIdentity {
    pub fn new(cert: PathBuf, key: PathBuf) -> Self {
        Self { cert, key }
    }

    /// What a probe presents: the `--tls-client-cert/--tls-client-key` pair when given;
    /// otherwise, for the local hub (no `--address`) that requires client certificates, the hub's
    /// own certificate and key (`hub_pair`), which works when that certificate is issued by the
    /// client CA and may be used by a client; otherwise nothing. A hub that only allows
    /// certificates (`optional`) is probed without one: a certificate that is not from its client
    /// CA would fail the handshake.
    pub fn resolve(
        flags: Option<(&Path, &Path)>,
        local: bool,
        hub_pair: Option<(&Path, &Path)>,
        required: bool,
    ) -> Option<Self> {
        flags
            .or(hub_pair.filter(|_| local && required))
            .map(|(cert, key)| Self::new(cert.to_path_buf(), key.to_path_buf()))
    }

    fn load(
        &self,
    ) -> anyhow::Result<(
        Vec<CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )> {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&self.cert)
            .with_context(|| format!("read client certificate {}", self.cert.display()))?
            .collect::<Result<_, _>>()
            .with_context(|| format!("parse client certificate {}", self.cert.display()))?;
        if chain.is_empty() {
            anyhow::bail!("no certificate in {}", self.cert.display());
        }
        let key = rustls::pki_types::PrivateKeyDer::from_pem_file(&self.key)
            .with_context(|| format!("read client key {}", self.key.display()))?;
        Ok((chain, key))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// `host:port`.
    pub address: String,
    /// `None`: plain text.
    pub tls: Option<Trust>,
    /// The client certificate to present; `None`: none. Needs `tls`: connecting to a plain-text
    /// target with one is an error, not a silently dropped credential.
    pub identity: Option<ClientIdentity>,
}

impl Target {
    /// Plain text to `addr`.
    pub fn plain(addr: impl std::fmt::Display) -> Self {
        Self {
            address: addr.to_string(),
            tls: None,
            identity: None,
        }
    }

    /// This target presenting `identity` (when there is one) to the server.
    pub fn with_identity(mut self, identity: Option<ClientIdentity>) -> Self {
        self.identity = identity;
        self
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
        Self {
            address,
            tls,
            identity: None,
        }
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
            // A client certificate for a plain-text target is a mistake the operator must hear
            // about: the probe would otherwise run unauthenticated and look as if it did not.
            if self.identity.is_some() {
                anyhow::bail!(
                    "a client certificate needs TLS, but {} is plain text \
                     (use an https:// address or --tls-ca, or drop --tls-client-cert and \
                     --tls-client-key)",
                    self.address
                );
            }
            return Ok(Endpoint::from_shared(format!("http://{}", self.address))?
                .connect()
                .await?);
        };
        let connector = TlsConnector::from(client_config(trust, self.identity.as_ref())?);
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

/// The rustls client configuration for `trust`, presenting `identity` when there is one; HTTP/2
/// only (this is for gRPC).
pub fn client_config(
    trust: &Trust,
    identity: Option<&ClientIdentity>,
) -> anyhow::Result<Arc<ClientConfig>> {
    let provider = Arc::new(default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let identity = identity.map(ClientIdentity::load).transpose()?;
    // Finishes a builder that already knows whom to trust.
    macro_rules! finish {
        ($builder:expr) => {
            match identity {
                Some((chain, key)) => $builder
                    .with_client_auth_cert(chain, key)
                    .context("client certificate and key")?,
                None => $builder.with_no_client_auth(),
            }
        };
    }
    let mut config = match trust {
        Trust::Ca(path) => finish!(builder.with_root_certificates(roots_from_pem(path)?)),
        Trust::System => {
            let mut roots = RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            let (added, _ignored) = roots.add_parsable_certificates(native.certs);
            if added == 0 {
                anyhow::bail!("no system root certificates found; pass --tls-ca <pem>");
            }
            finish!(builder.with_root_certificates(roots))
        }
        Trust::Pinned(path) => {
            let leaf = CertificateDer::pem_file_iter(path)
                .with_context(|| format!("read {}", path.display()))?
                .next()
                .with_context(|| format!("no certificate in {}", path.display()))?
                .with_context(|| format!("parse {}", path.display()))?;
            finish!(
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(Pinned {
                        leaf,
                        algs: provider.signature_verification_algorithms,
                    }))
            )
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
                tls: None,
                identity: None
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
                tls: Some(Trust::Ca(ca.into())),
                identity: None
            }
        );
        // https:// alone: the system roots.
        assert_eq!(
            Target::resolve(Some("https://hub.example:7000/"), None, 7000, None),
            Target {
                address: "hub.example:7000".into(),
                tls: Some(Trust::System),
                identity: None
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

    #[test]
    fn the_probe_presents_a_client_certificate_only_when_it_has_one_to_present() {
        let pair =
            || ClientIdentity::new(Path::new("/hub.pem").into(), Path::new("/hub.key").into());
        let own = pair();
        let flags = ClientIdentity::new("/c.pem".into(), "/k.pem".into());
        let given = Some(("/c.pem".as_ref(), "/k.pem".as_ref()));
        let hub = Some((Path::new("/hub.pem"), Path::new("/hub.key")));
        // Flags always win.
        assert_eq!(
            ClientIdentity::resolve(given, true, hub, true),
            Some(flags.clone())
        );
        assert_eq!(
            ClientIdentity::resolve(given, false, None, false),
            Some(flags)
        );
        // The local hub that requires certificates: the probe presents the hub's own pair.
        assert_eq!(ClientIdentity::resolve(None, true, hub, true), Some(own));
        // Not required (optional presents nothing, a certificate that is not from the client CA
        // would fail the handshake), not the local hub, or no pair: nothing.
        assert_eq!(ClientIdentity::resolve(None, true, hub, false), None);
        assert_eq!(ClientIdentity::resolve(None, false, hub, true), None);
        assert_eq!(ClientIdentity::resolve(None, true, None, true), None);
    }
}
