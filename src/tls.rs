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

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use rustls::DigitallySignedStruct;
use rustls::ServerConfig;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::telemetry::Metrics;

/// Handshakes in progress at once, per listener.
pub const MAX_HANDSHAKES: usize = 512;

/// What bounds the TLS handshakes of one listener: a connection that cannot get a slot is shed
/// (dropped at once), so a flood of half-open connections can neither stall the accept loop nor
/// keep honest clients out for longer than one handshake timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandshakeLimits {
    /// A handshake that takes longer is dropped. `KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT`, default 5 s.
    pub timeout: Duration,
    /// Handshakes in progress at once, over all peers.
    pub slots: usize,
    /// Handshakes in progress at once from one peer IP; 0 means no cap.
    /// `KRAKEN_HUB_TLS_HANDSHAKES_PER_IP`, default 32.
    pub per_ip: usize,
}

impl Default for HandshakeLimits {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            slots: MAX_HANDSHAKES,
            per_ip: 32,
        }
    }
}

/// Handshakes in progress per peer IP; a guard counts one handshake and releases it on drop.
#[derive(Default)]
struct PerIp(std::sync::Mutex<HashMap<IpAddr, usize>>);

struct IpGuard {
    ips: Arc<PerIp>,
    ip: IpAddr,
}

impl PerIp {
    /// A guard for `ip`, or `None` when it already has `cap` handshakes in progress.
    fn acquire(self: &Arc<Self>, ip: IpAddr, cap: usize) -> Option<IpGuard> {
        let mut map = self.0.lock().unwrap();
        let n = map.entry(ip).or_insert(0);
        if cap > 0 && *n >= cap {
            return None;
        }
        *n += 1;
        Some(IpGuard {
            ips: Arc::clone(self),
            ip,
        })
    }
}

impl Drop for IpGuard {
    fn drop(&mut self) {
        let mut map = self.ips.0.lock().unwrap();
        if let Some(n) = map.get_mut(&self.ip) {
            *n -= 1;
            if *n == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

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

/// A warning when the private key file can be read by its group or by others (Unix only; the
/// mode is the one `stat` shows after following symlinks). `None` for a tight key, and for a key
/// whose metadata cannot be read (loading it reports that). A key that others can read is a key
/// that others may already have.
pub fn key_permission_warning(key: &Path) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(key).ok()?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Some(format!(
                "tls private key {} is readable by its group or others (mode {mode:o}); run `chmod 600` on it (the hub's own user must still be able to read it)",
                key.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = key;
    None
}

fn warn_if_key_is_loose(listener: &str, key: &Path) {
    if let Some(warning) = key_permission_warning(key) {
        warn!(listener, "{warning}");
    }
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
        warn_if_key_is_loose(name, key);
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
                warn_if_key_is_loose(self.name, &self.key);
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

/// `KRAKEN_HUB_TLS_CLIENT_AUTH`: whether a client must present a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuthMode {
    /// A client may present one (it must then verify) or none (token or JWT, as before).
    #[default]
    Optional,
    /// A client without a valid certificate fails the handshake.
    Required,
}

impl ClientAuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }
}

impl std::str::FromStr for ClientAuthMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "optional" => Ok(Self::Optional),
            "required" => Ok(Self::Required),
            other => {
                anyhow::bail!("unknown client auth mode {other:?} (expected optional or required)")
            }
        }
    }
}

/// The client CAs a listener trusts, replaced by [`ClientCaStore::reload`]. The verifier is built
/// from the PEM bundle; a bundle that does not load keeps the previous verifier.
pub struct ClientCaStore {
    current: RwLock<Arc<dyn ClientCertVerifier>>,
    path: PathBuf,
    mode: ClientAuthMode,
    metrics: Arc<Metrics>,
}

impl fmt::Debug for ClientCaStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCaStore")
            .field("path", &self.path)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// A verifier for the client CAs in the PEM bundle `path`.
fn load_client_verifier(
    path: &Path,
    mode: ClientAuthMode,
) -> anyhow::Result<Arc<dyn ClientCertVerifier>> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path)
        .with_context(|| format!("read client CA {}", path.display()))?
    {
        roots
            .add(cert.with_context(|| format!("parse client CA {}", path.display()))?)
            .with_context(|| format!("client CA {}", path.display()))?;
    }
    if roots.is_empty() {
        anyhow::bail!("no certificate in client CA {}", path.display());
    }
    let builder =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::new(default_provider()));
    let builder = match mode {
        ClientAuthMode::Required => builder,
        ClientAuthMode::Optional => builder.allow_unauthenticated(),
    };
    builder.build().context("client CA verifier")
}

impl ClientCaStore {
    /// Loads the bundle; a bad bundle is fatal at startup.
    pub fn load(
        path: &Path,
        mode: ClientAuthMode,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Arc<Self>> {
        let verifier = load_client_verifier(path, mode)?;
        info!(client_ca = %path.display(), mode = mode.as_str(), "tls client CA loaded");
        Ok(Arc::new(Self {
            current: RwLock::new(verifier),
            path: path.to_path_buf(),
            mode,
            metrics,
        }))
    }

    /// The file to watch for changes.
    pub fn paths(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    pub fn mode(&self) -> ClientAuthMode {
        self.mode
    }

    /// Re-reads the bundle. One that cannot be loaded keeps the previous CAs, logs an error and is
    /// counted (`kraken_hub_tls_client_ca_reload_errors_total`). New handshakes use the new CAs;
    /// sessions that are live are not judged again.
    pub fn reload(&self) -> bool {
        match load_client_verifier(&self.path, self.mode) {
            Ok(verifier) => {
                *self.current.write().unwrap() = verifier;
                info!(client_ca = %self.path.display(), "tls client CA reloaded");
                true
            }
            Err(e) => {
                self.metrics.client_ca_reload_failed();
                error!(client_ca = %self.path.display(), error = %format!("{e:#}"), "tls client CA reload failed; keeping the previous one");
                false
            }
        }
    }

    fn verifier(&self) -> Arc<dyn ClientCertVerifier> {
        Arc::clone(&self.current.read().unwrap())
    }
}

/// What rustls asks at every handshake: it forwards to the store's current verifier, so a
/// reloaded CA bundle applies to the next handshake without rebuilding the server configuration.
struct CurrentVerifier(Arc<ClientCaStore>);

impl fmt::Debug for CurrentVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurrentVerifier").finish_non_exhaustive()
    }
}

impl ClientCertVerifier for CurrentVerifier {
    fn offer_client_auth(&self) -> bool {
        self.0.verifier().offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.0.verifier().client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        // Hints are advisory, and the trait lends a slice of the verifier current at the call, which
        // a reload may replace. No hints tell a client to present the certificate it holds; the
        // verifier decides.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.0
            .verifier()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verifier().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verifier().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.verifier().supported_verify_schemes()
    }
}

/// Whether and how clients must present certificates (ADR 0020): `None` authenticates the server
/// only (the HTTP listener always, and a gRPC listener without a client CA); otherwise the client
/// CA store the listener verifies against, in the mode it was loaded with.
#[derive(Debug, Clone, Default)]
pub enum ClientAuth {
    #[default]
    None,
    /// A client may present a certificate (it must then verify) or none.
    Optional(Arc<ClientCaStore>),
    /// A client without a valid certificate fails the handshake.
    Required(Arc<ClientCaStore>),
}

impl ClientAuth {
    /// The variant for a loaded store: its mode decides.
    pub fn from_store(store: Arc<ClientCaStore>) -> Self {
        match store.mode() {
            ClientAuthMode::Optional => Self::Optional(store),
            ClientAuthMode::Required => Self::Required(store),
        }
    }
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
    let builder = match client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Optional(cas) | ClientAuth::Required(cas) => {
            builder.with_client_cert_verifier(Arc::new(CurrentVerifier(cas)))
        }
    };
    let mut config = builder.with_cert_resolver(store);
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(config)
}

/// Completed TLS handshakes from a stream of accepted TCP connections. Each connection
/// handshakes in its own task, bounded by `limits.timeout`; a client that does not speak TLS
/// (plain text, a port scan) is dropped and logged at debug level. The accept loop never waits
/// for a slot: a connection that finds every slot busy (or its peer IP at its cap) is dropped at
/// once and counted in `kraken_hub_tls_handshakes_shed_total`. The accept task ends, and with it
/// the listening socket, when the returned receiver is dropped or `stopping` fires; pending
/// handshakes end with it. `listener` (`grpc` or `http`) labels the counter.
pub fn accept<S>(
    mut incoming: S,
    config: Arc<ServerConfig>,
    stopping: CancellationToken,
    limits: HandshakeLimits,
    listener: &'static str,
    metrics: Arc<Metrics>,
) -> mpsc::Receiver<TlsStream<TcpStream>>
where
    S: Stream<Item = io::Result<TcpStream>> + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(64);
    let acceptor = TlsAcceptor::from(config);
    let slots = Arc::new(Semaphore::new(limits.slots));
    let per_ip = Arc::new(PerIp::default());
    tokio::spawn(async move {
        // Shed connections are logged once per window, not once per connection: during a flood
        // they arrive at line rate.
        let mut shed_logged: Option<Instant> = None;
        let mut shed_since_log = 0u64;
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
                    tokio::select! {
                        biased;
                        _ = stopping.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                    continue;
                }
                Some(Ok(tcp)) => tcp,
            };
            let peer = tcp.peer_addr().ok();
            let shed = |reason: &'static str| {
                metrics.tls_handshake_shed(listener, reason);
            };
            let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
                shed("slots");
                shed_since_log += 1;
                log_shed(&mut shed_logged, &mut shed_since_log, listener);
                continue;
            };
            let guard = match peer {
                Some(peer) => match per_ip.acquire(peer.ip(), limits.per_ip) {
                    Some(guard) => Some(guard),
                    None => {
                        shed("per_ip");
                        shed_since_log += 1;
                        log_shed(&mut shed_logged, &mut shed_since_log, listener);
                        continue;
                    }
                },
                None => None,
            };
            let (acceptor, tx, stopping) = (acceptor.clone(), tx.clone(), stopping.clone());
            let metrics = Arc::clone(&metrics);
            tokio::spawn(async move {
                let handshake = tokio::time::timeout(limits.timeout, acceptor.accept(tcp));
                let result = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    result = handshake => result,
                };
                match result {
                    Ok(Ok(stream)) => {
                        drop((permit, guard));
                        tokio::select! {
                            biased;
                            _ = stopping.cancelled() => {}
                            _ = tx.send(stream) => {}
                        }
                    }
                    Ok(Err(e)) => {
                        metrics.tls_handshake_failure(listener, failure_reason(&e));
                        debug!(?peer, error = %e, "tls handshake failed");
                    }
                    Err(_) => {
                        metrics.tls_handshake_failure(listener, "timeout");
                        debug!(?peer, "tls handshake timed out");
                    }
                }
            });
        }
    });
    rx
}

/// Why a handshake failed, as a fixed label: a client certificate that was required and missing,
/// one that did not verify, or anything else (plain text, scans, protocol errors).
fn failure_reason(e: &io::Error) -> &'static str {
    match e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(rustls::Error::NoCertificatesPresented) => "client_cert_missing",
        Some(rustls::Error::InvalidCertificate(_)) => "client_cert_invalid",
        _ => "other",
    }
}

/// At most one "shedding" line per this long.
const SHED_LOG_EVERY: Duration = Duration::from_secs(10);

fn log_shed(last: &mut Option<Instant>, count: &mut u64, listener: &'static str) {
    if last.is_none_or(|at| at.elapsed() >= SHED_LOG_EVERY) {
        warn!(
            listener,
            shed = *count,
            "TLS handshake limit reached: connections dropped without a handshake"
        );
        *last = Some(Instant::now());
        *count = 0;
    }
}

/// A TLS connection as tonic's server wants it.
pub struct TlsConn(pub TlsStream<TcpStream>);

impl tonic::transport::server::Connected for TlsConn {
    /// The client certificate the peer presented, for the requests on this connection (ADR 0020).
    type ConnectInfo = crate::mtls::PeerCertificate;
    fn connect_info(&self) -> Self::ConnectInfo {
        crate::mtls::PeerCertificate(
            self.0
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .map(|leaf| Arc::from(leaf.as_ref())),
        )
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::test_support::{TestMetrics, test_metrics};
    use rustls::pki_types::ServerName;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::ReceiverStream;

    struct Fx {
        addr: SocketAddr,
        rx: mpsc::Receiver<TlsStream<TcpStream>>,
        stopping: CancellationToken,
        connector: tokio_rustls::TlsConnector,
        m: TestMetrics,
    }

    /// An `accept` loop over a loopback listener, with a throwaway self-signed certificate.
    async fn fixture(limits: HandshakeLimits) -> Fx {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = std::env::temp_dir().join(format!("kraken-hub-tls-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert_path, key_path) = (dir.join("cert.pem"), dir.join("key.pem"));
        std::fs::write(&cert_path, key.cert.pem()).unwrap();
        std::fs::write(&key_path, key.signing_key.serialize_pem()).unwrap();
        let store = CertStore::load(&cert_path, &key_path, "test").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let config = server_config(store, &[b"h2"], ClientAuth::None);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tcp_tx, tcp_rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                if tcp_tx.send(Ok(tcp)).await.is_err() {
                    break;
                }
            }
        });
        let stopping = CancellationToken::new();
        let m = test_metrics();
        let rx = accept(
            ReceiverStream::new(tcp_rx),
            config,
            stopping.clone(),
            limits,
            "test",
            Arc::clone(&m.metrics),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(key.cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Fx {
            addr,
            rx,
            stopping,
            connector: tokio_rustls::TlsConnector::from(Arc::new(client)),
            m,
        }
    }

    fn limits(slots: usize, per_ip: usize) -> HandshakeLimits {
        HandshakeLimits {
            timeout: Duration::from_secs(30),
            slots,
            per_ip,
        }
    }

    /// True when the server closes `tcp` within `within` (a shed connection is dropped at once).
    async fn closed_by_server(tcp: &mut TcpStream, within: Duration) -> bool {
        let mut buf = [0u8; 16];
        matches!(
            tokio::time::timeout(within, tcp.read(&mut buf)).await,
            Ok(Ok(0)) | Ok(Err(_))
        )
    }

    fn scratch_pair() -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = std::env::temp_dir().join(format!("kraken-hub-key-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key_path) = (dir.join("cert.pem"), dir.join("key.pem"));
        std::fs::write(&cert, key.cert.pem()).unwrap();
        std::fs::write(&key_path, key.signing_key.serialize_pem()).unwrap();
        (dir, cert, key_path)
    }

    #[cfg(unix)]
    fn chmod(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_key_readable_by_group_or_others_is_a_warning() {
        let (dir, _, key) = scratch_pair();
        chmod(&key, 0o600);
        assert_eq!(key_permission_warning(&key), None);
        chmod(&key, 0o400);
        assert_eq!(key_permission_warning(&key), None);
        for mode in [0o640, 0o604, 0o644, 0o660, 0o666] {
            chmod(&key, mode);
            let warning = key_permission_warning(&key).unwrap_or_else(|| panic!("{mode:o}"));
            assert!(warning.contains(&format!("{mode:o}")), "{warning}");
            assert!(warning.contains("chmod 600"), "{warning}");
        }
        // A key that cannot be read says nothing here (loading reports that).
        assert_eq!(key_permission_warning(&dir.join("missing.pem")), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn loading_and_reloading_warn_about_a_loose_key() {
        use crate::telemetry::test_support::{capture_logs, logged};
        let (dir, cert, key) = scratch_pair();
        chmod(&key, 0o644);
        capture_logs();
        let store = CertStore::load(&cert, &key, "test").unwrap();
        let warned = |lines: &[crate::telemetry::test_support::LogLine]| {
            lines
                .iter()
                .filter(|l| l.message.contains("private key") && l.message.contains("readable"))
                .count()
        };
        assert_eq!(warned(&logged()), 1, "at startup");
        assert!(store.reload());
        assert_eq!(warned(&logged()), 2, "and again on a reload");
        chmod(&key, 0o600);
        capture_logs();
        assert!(store.reload());
        assert_eq!(warned(&logged()), 0, "a tight key is not warned about");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn filling_the_slots_sheds_new_connections_and_a_later_client_still_connects() {
        let mut f = fixture(limits(2, 0)).await;
        // Two clients that connect and say nothing hold both slots.
        let silent = [
            TcpStream::connect(f.addr).await.unwrap(),
            TcpStream::connect(f.addr).await.unwrap(),
        ];
        tokio::time::sleep(Duration::from_millis(100)).await;
        // A third is dropped at once; the accept loop never waits for a slot.
        let mut third = TcpStream::connect(f.addr).await.unwrap();
        assert!(
            closed_by_server(&mut third, Duration::from_secs(2)).await,
            "the connection must be shed, not left waiting for a slot"
        );
        assert_eq!(
            f.m.counter("kraken_hub_tls_handshakes_shed_total", "reason", "slots"),
            Some(1)
        );
        // The silent clients leave: slots free up and a real client completes its handshake.
        drop(silent);
        let connector = f.connector.clone();
        let addr = f.addr;
        let connected = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                if let Ok(tcp) = TcpStream::connect(addr).await
                    && let Ok(tls) = connector
                        .connect(ServerName::try_from("localhost").unwrap(), tcp)
                        .await
                {
                    return tls;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("a client connects once slots are free");
        let accepted = tokio::time::timeout(Duration::from_secs(2), f.rx.recv()).await;
        assert!(matches!(accepted, Ok(Some(_))));
        drop(connected);
    }

    #[tokio::test]
    async fn a_peer_ip_cannot_hold_more_than_its_share_of_the_handshakes() {
        let f = fixture(limits(100, 2)).await;
        let held = [
            TcpStream::connect(f.addr).await.unwrap(),
            TcpStream::connect(f.addr).await.unwrap(),
        ];
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut third = TcpStream::connect(f.addr).await.unwrap();
        assert!(closed_by_server(&mut third, Duration::from_secs(2)).await);
        assert_eq!(
            f.m.counter("kraken_hub_tls_handshakes_shed_total", "reason", "per_ip"),
            Some(1)
        );
        assert_eq!(
            f.m.counter("kraken_hub_tls_handshakes_shed_total", "reason", "slots"),
            None
        );
        drop(held);
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_pending_handshakes() {
        // A 30 s handshake timeout and every slot taken: only `stopping` can end things.
        let mut f = fixture(limits(2, 0)).await;
        let _silent = [
            TcpStream::connect(f.addr).await.unwrap(),
            TcpStream::connect(f.addr).await.unwrap(),
        ];
        let _shed = TcpStream::connect(f.addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        f.stopping.cancel();
        let ended = tokio::time::timeout(Duration::from_secs(1), f.rx.recv()).await;
        assert!(
            matches!(ended, Ok(None)),
            "the accept loop and the pending handshakes must end on shutdown"
        );
    }

    #[tokio::test]
    async fn a_handshake_that_never_finishes_is_dropped_at_the_timeout() {
        let mut f = fixture(HandshakeLimits {
            timeout: Duration::from_millis(100),
            slots: 1,
            per_ip: 0,
        })
        .await;
        let mut silent = TcpStream::connect(f.addr).await.unwrap();
        assert!(closed_by_server(&mut silent, Duration::from_secs(2)).await);
        let _ = &mut f;
    }

    // ---- client certificates (ADR 0020) ----

    use crate::pki_test_support::{ClientSpec, Pki};

    /// An `accept` loop that verifies client certificates against a client CA file.
    struct Mx {
        addr: SocketAddr,
        rx: mpsc::Receiver<TlsStream<TcpStream>>,
        m: TestMetrics,
        dir: PathBuf,
        server_pki: Pki,
        /// Signs the clients' certificates; its certificate is the client CA file.
        clients: Pki,
        store: Arc<ClientCaStore>,
    }

    impl Drop for Mx {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    impl Mx {
        fn ca_file(&self) -> PathBuf {
            self.dir.join("client-ca.pem")
        }

        /// A TLS client trusting the server's CA, presenting `identity` (certificate and key PEM).
        fn connector(&self, identity: Option<(String, String)>) -> tokio_rustls::TlsConnector {
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(self.server_pki.ca_pem.as_bytes()).unwrap())
                .unwrap();
            let builder = rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots);
            let config = match identity {
                None => builder.with_no_client_auth(),
                Some((cert, key)) => builder
                    .with_client_auth_cert(
                        CertificateDer::pem_slice_iter(cert.as_bytes())
                            .collect::<Result<_, _>>()
                            .unwrap(),
                        PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
                    )
                    .unwrap(),
            };
            tokio_rustls::TlsConnector::from(Arc::new(config))
        }

        /// Connects and reads once: with TLS 1.3 a client learns that the server refused its
        /// certificate only when it reads. `Ok` when the connection is usable.
        async fn try_connect(&self, identity: Option<(String, String)>) -> Result<(), String> {
            let tcp = TcpStream::connect(self.addr).await.unwrap();
            let mut tls = self
                .connector(identity)
                .connect(ServerName::try_from("localhost").unwrap(), tcp)
                .await
                .map_err(|e| e.to_string())?;
            tokio::io::AsyncWriteExt::write_all(&mut tls, b"x")
                .await
                .map_err(|e| e.to_string())?;
            let mut buf = [0u8; 1];
            match tokio::time::timeout(Duration::from_millis(500), tls.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => Err("closed by the server".to_string()),
                Ok(Ok(_)) | Err(_) => Ok(()),
            }
        }

        /// The next connection the server completed, with the certificate it presented.
        async fn accepted(&mut self) -> Option<Option<Vec<u8>>> {
            let stream = tokio::time::timeout(Duration::from_millis(700), self.rx.recv())
                .await
                .ok()??;
            Some(
                stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|certs| certs.first())
                    .map(|c| c.as_ref().to_vec()),
            )
        }

        /// `(certificate PEM, key PEM)` of a client of the client CA.
        fn identity(&self, spec: &ClientSpec) -> (String, String) {
            self.clients.client(spec)
        }

        fn failures(&self, reason: &str) -> Option<u64> {
            self.m
                .counter("kraken_hub_tls_handshake_failures_total", "reason", reason)
        }
    }

    async fn mx(mode: ClientAuthMode) -> Mx {
        let server_pki = Pki::new("server CA");
        let (cert, key) = server_pki.server();
        let dir = std::env::temp_dir().join(format!("kraken-hub-mtls-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert_path, key_path) = (dir.join("cert.pem"), dir.join("key.pem"));
        std::fs::write(&cert_path, cert).unwrap();
        std::fs::write(&key_path, key).unwrap();
        let clients = Pki::new("client CA");
        let ca_path = dir.join("client-ca.pem");
        std::fs::write(&ca_path, &clients.ca_pem).unwrap();
        let m = test_metrics();
        let store = CertStore::load(&cert_path, &key_path, "test").unwrap();
        let cas = ClientCaStore::load(&ca_path, mode, Arc::clone(&m.metrics)).unwrap();
        let config = server_config(store, &[b"h2"], ClientAuth::from_store(Arc::clone(&cas)));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tcp_tx, tcp_rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                if tcp_tx.send(Ok(tcp)).await.is_err() {
                    break;
                }
            }
        });
        let rx = accept(
            ReceiverStream::new(tcp_rx),
            config,
            CancellationToken::new(),
            HandshakeLimits::default(),
            "test",
            Arc::clone(&m.metrics),
        );
        Mx {
            addr,
            rx,
            m,
            dir,
            server_pki,
            clients,
            store: cas,
        }
    }

    #[tokio::test]
    async fn required_refuses_a_client_without_a_certificate_at_the_handshake() {
        let mut mx = mx(ClientAuthMode::Required).await;
        assert!(mx.try_connect(None).await.is_err());
        assert!(mx.accepted().await.is_none(), "no connection is handed on");
        assert_eq!(mx.failures("client_cert_missing"), Some(1));
    }

    #[tokio::test]
    async fn required_accepts_a_certificate_from_the_client_ca_and_hands_it_on() {
        let mut mx = mx(ClientAuthMode::Required).await;
        let (cert, key) = mx.identity(&ClientSpec::service("orders"));
        mx.try_connect(Some((cert.clone(), key))).await.unwrap();
        let presented = mx
            .accepted()
            .await
            .expect("accepted")
            .expect("a certificate");
        let sent = CertificateDer::from_pem_slice(cert.as_bytes()).unwrap();
        assert_eq!(presented, sent.as_ref());
    }

    #[tokio::test]
    async fn optional_accepts_a_client_without_a_certificate() {
        let mut mx = mx(ClientAuthMode::Optional).await;
        mx.try_connect(None).await.unwrap();
        assert_eq!(mx.accepted().await, Some(None));
        assert_eq!(mx.failures("client_cert_missing"), None);
    }

    #[tokio::test]
    async fn optional_accepts_a_valid_certificate() {
        let mut mx = mx(ClientAuthMode::Optional).await;
        mx.try_connect(Some(mx.identity(&ClientSpec::service("orders"))))
            .await
            .unwrap();
        assert!(mx.accepted().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_certificate_from_another_ca_is_refused_in_both_modes() {
        for mode in [ClientAuthMode::Optional, ClientAuthMode::Required] {
            let mut mx = mx(mode).await;
            let other = Pki::new("some other CA");
            let identity = other.client(&ClientSpec::service("orders"));
            assert!(mx.try_connect(Some(identity)).await.is_err(), "{mode:?}");
            assert!(mx.accepted().await.is_none());
            assert_eq!(mx.failures("client_cert_invalid"), Some(1), "{mode:?}");
        }
    }

    #[tokio::test]
    async fn an_expired_certificate_is_refused_at_the_handshake() {
        let mx = mx(ClientAuthMode::Optional).await;
        let (cert, key) =
            mx.identity(&ClientSpec::service("orders").valid_for(Duration::from_secs(1)));
        tokio::time::sleep(Duration::from_millis(2100)).await;
        // It ended one second after its creation, a second ago.
        assert!(mx.try_connect(Some((cert, key))).await.is_err());
        assert_eq!(mx.failures("client_cert_invalid"), Some(1));
    }

    #[tokio::test]
    async fn the_client_ca_reloads_for_new_handshakes() {
        let mut mx = mx(ClientAuthMode::Required).await;
        let old = mx.identity(&ClientSpec::service("orders"));
        mx.try_connect(Some(old.clone())).await.unwrap();
        mx.accepted().await.expect("accepted");

        // The bundle is replaced by another CA: its certificates work, the old CA's do not.
        let next = Pki::new("next client CA");
        std::fs::write(mx.ca_file(), &next.ca_pem).unwrap();
        assert!(mx.store.reload());
        mx.try_connect(Some(next.client(&ClientSpec::service("orders"))))
            .await
            .unwrap();
        mx.accepted().await.expect("accepted");
        assert!(mx.try_connect(Some(old)).await.is_err());
    }

    #[tokio::test]
    async fn a_bad_client_ca_file_keeps_the_previous_verifier_and_is_counted() {
        let mut mx = mx(ClientAuthMode::Required).await;
        let identity = mx.identity(&ClientSpec::service("orders"));
        for junk in [
            "",
            "not pem at all",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        ] {
            std::fs::write(mx.ca_file(), junk).unwrap();
            assert!(!mx.store.reload(), "{junk:?}");
        }
        std::fs::remove_file(mx.ca_file()).unwrap();
        assert!(!mx.store.reload());
        assert_eq!(
            mx.m.counter_total("kraken_hub_tls_client_ca_reload_errors_total"),
            Some(4)
        );
        mx.try_connect(Some(identity)).await.unwrap();
        mx.accepted().await.expect("accepted");
    }

    #[test]
    fn a_client_ca_that_does_not_load_is_a_startup_error() {
        let m = test_metrics();
        let dir = std::env::temp_dir().join(format!("kraken-hub-ca-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("nope.pem");
        assert!(
            ClientCaStore::load(&missing, ClientAuthMode::Optional, Arc::clone(&m.metrics))
                .is_err()
        );
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let e = ClientCaStore::load(&empty, ClientAuthMode::Optional, Arc::clone(&m.metrics))
            .unwrap_err();
        assert!(format!("{e:#}").contains("no certificate"), "{e:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn client_auth_modes_parse() {
        assert_eq!(
            "required".parse::<ClientAuthMode>().unwrap(),
            ClientAuthMode::Required
        );
        assert_eq!(
            " Optional ".parse::<ClientAuthMode>().unwrap(),
            ClientAuthMode::Optional
        );
        assert!("none".parse::<ClientAuthMode>().is_err());
    }
}
