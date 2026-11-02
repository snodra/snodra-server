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
                    Ok(Err(e)) => debug!(?peer, error = %e, "tls handshake failed"),
                    Err(_) => debug!(?peer, "tls handshake timed out"),
                }
            });
        }
    });
    rx
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
}
