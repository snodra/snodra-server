//! TLS for both listeners (roadmap P5a, ADR 0018), with rustls and the `ring` provider.
//!
//! * [`CertStore`] holds the current certificate and key and hands them to every handshake
//!   (`ResolvesServerCert`), so a reloaded certificate applies to new connections while the
//!   existing ones keep running on the one they started with.
//! * [`server_config`] builds the rustls server configuration. Client certificates are P5c:
//!   [`ClientAuth`] is the hook, with a single variant until then.
//! * [`accept`] turns a stream of accepted TCP connections into completed TLS handshakes, one
//!   task per connection (a slow or plain-text client never holds up the accept loop), for the
//!   gRPC server ([`TlsConn`]) and for axum ([`TlsListener`]).

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Context as _;
use rustls::ServerConfig;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// A TLS handshake that takes longer than this is dropped.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Handshakes in progress at once, per listener; further connections wait in the accept queue.
const MAX_HANDSHAKES: usize = 512;

/// Reads the PEM certificate chain and private key into a `CertifiedKey`, checking that the key
/// belongs to the leaf.
pub fn load_certified(cert: &Path, key: &Path) -> anyhow::Result<CertifiedKey> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .with_context(|| format!("read certificate {}", cert.display()))?
        .collect::<Result<_, _>>()
        .with_context(|| format!("parse certificate {}", cert.display()))?;
    if chain.is_empty() {
        anyhow::bail!("no certificate in {}", cert.display());
    }
    let key = PrivateKeyDer::from_pem_file(key)
        .with_context(|| format!("read private key {}", key.display()))?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key)
        .context("unsupported private key type")?;
    let certified = CertifiedKey::new(chain, signing);
    certified
        .keys_match()
        .context("the private key does not match the certificate")?;
    Ok(certified)
}

/// The certificate a listener presents, replaced by [`CertStore::reload`].
pub struct CertStore {
    current: RwLock<Arc<CertifiedKey>>,
    cert: PathBuf,
    key: PathBuf,
    name: &'static str,
}

impl fmt::Debug for CertStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertStore")
            .field("name", &self.name)
            .field("cert", &self.cert)
            .finish_non_exhaustive()
    }
}

impl CertStore {
    /// Loads the pair; a bad pair is fatal at startup. `name` (`grpc` or `http`) labels logs.
    pub fn load(cert: &Path, key: &Path, name: &'static str) -> anyhow::Result<Arc<Self>> {
        let certified = load_certified(cert, key)?;
        info!(listener = name, cert = %cert.display(), "tls certificate loaded");
        Ok(Arc::new(Self {
            current: RwLock::new(Arc::new(certified)),
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            name,
        }))
    }

    /// The files to watch for changes.
    pub fn paths(&self) -> Vec<PathBuf> {
        vec![self.cert.clone(), self.key.clone()]
    }

    /// Re-reads the pair. A pair that cannot be loaded (a half-written file, a key that does not
    /// match yet) keeps the previous certificate and logs an error; the next change retries.
    pub fn reload(&self) -> bool {
        match load_certified(&self.cert, &self.key) {
            Ok(certified) => {
                *self.current.write().unwrap() = Arc::new(certified);
                info!(listener = self.name, cert = %self.cert.display(), "tls certificate reloaded");
                true
            }
            Err(e) => {
                error!(listener = self.name, error = %format!("{e:#}"), "tls certificate reload failed; keeping the previous one");
                false
            }
        }
    }

    /// The leaf certificate currently presented (DER).
    pub fn leaf(&self) -> Vec<u8> {
        self.current.read().unwrap().cert[0].as_ref().to_vec()
    }
}

impl ResolvesServerCert for CertStore {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.read().unwrap()))
    }
}

/// Whether and how clients must present certificates. Roadmap P5c adds a variant that holds the
/// client CA and maps a certificate's SAN `kraken://service/<name>` to a service; until then
/// the listeners authenticate the server only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuth {
    #[default]
    None,
}

/// The server configuration for a listener. `alpn` lists the protocols to offer in order
/// (`h2` for gRPC; `h2` and `http/1.1` for the HTTP port).
pub fn server_config(
    store: Arc<CertStore>,
    alpn: &[&[u8]],
    client_auth: ClientAuth,
) -> Arc<ServerConfig> {
    let builder = ServerConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions");
    // P5c: `ClientAuth::Required(ca)` becomes `with_client_cert_verifier(..)` here.
    let builder = match client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_cert_resolver(store);
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// Completed TLS handshakes from a stream of accepted TCP connections. Each connection
/// handshakes in its own task, bounded by [`HANDSHAKE_TIMEOUT`]; a client that does not speak
/// TLS (plain text, a port scan) is dropped and logged at debug level. The accept task ends,
/// and with it the listening socket, when the returned receiver is dropped or `stopping` fires.
pub fn accept<S>(
    mut incoming: S,
    config: Arc<ServerConfig>,
    stopping: CancellationToken,
) -> mpsc::Receiver<TlsStream<TcpStream>>
where
    S: Stream<Item = io::Result<TcpStream>> + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(64);
    let acceptor = TlsAcceptor::from(config);
    let slots = Arc::new(Semaphore::new(MAX_HANDSHAKES));
    tokio::spawn(async move {
        loop {
            let tcp = tokio::select! {
                biased;
                _ = stopping.cancelled() => break,
                _ = tx.closed() => break,
                next = incoming.next() => next,
            };
            let tcp = match tcp {
                None => break,
                Some(Err(e)) => {
                    warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Some(Ok(tcp)) => tcp,
            };
            let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
                break;
            };
            let (acceptor, tx) = (acceptor.clone(), tx.clone());
            tokio::spawn(async move {
                let peer = tcp.peer_addr().ok();
                match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                    Ok(Ok(stream)) => {
                        drop(permit);
                        let _ = tx.send(stream).await;
                    }
                    Ok(Err(e)) => debug!(?peer, error = %e, "tls handshake failed"),
                    Err(_) => debug!(?peer, "tls handshake timed out"),
                }
            });
        }
    });
    rx
}

/// A TLS connection as tonic's server wants it.
pub struct TlsConn(pub TlsStream<TcpStream>);

impl tonic::transport::server::Connected for TlsConn {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// The HTTP port's listener when TLS is on: axum serves what [`accept`] completed.
pub struct TlsListener {
    rx: mpsc::Receiver<TlsStream<TcpStream>>,
    local: SocketAddr,
}

impl TlsListener {
    pub fn new(rx: mpsc::Receiver<TlsStream<TcpStream>>, local: SocketAddr) -> Self {
        Self { rx, local }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(stream) => {
                let peer = stream
                    .get_ref()
                    .0
                    .peer_addr()
                    .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                (stream, peer)
            }
            // The accept task ended: the server is shutting down.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}
