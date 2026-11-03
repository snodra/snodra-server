//! Test-only support for the JWT tests (ADR 0019): signing keys generated at test time (nothing
//! is checked in), token minting, forged tokens, and an in-process JWKS server.
//!
//! This file is compiled twice, by `#[path]`: as `jwt::test_support` into the library's own unit
//! tests, and as `common::jwt_keys` into the integration tests. It therefore depends on nothing
//! but external crates.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// Seconds since the epoch.
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// The claims of a Keycloak client-credentials access token: `iss`, `azp` (the service), `typ`
/// `Bearer`, and an `exp` `exp_in` seconds from now (negative: already expired).
pub fn claims(iss: &str, azp: &str, exp_in: i64) -> Value {
    json!({
        "iss": iss,
        "azp": azp,
        "typ": "Bearer",
        "iat": now(),
        "exp": now() + exp_in,
        "sub": "service-account-x",
    })
}

/// One signing key: signs tokens and describes itself as a JWK.
pub struct Signer {
    pub kid: String,
    pub alg: Algorithm,
    encoding: EncodingKey,
    jwk: Value,
}

fn rsa_key() -> &'static rsa::RsaPrivateKey {
    static KEY: OnceLock<rsa::RsaPrivateKey> = OnceLock::new();
    KEY.get_or_init(|| rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap())
}

impl Signer {
    fn ec(
        kid: &str,
        alg: Algorithm,
        curve: &str,
        rcgen_alg: &'static rcgen::SignatureAlgorithm,
    ) -> Self {
        let key = rcgen::KeyPair::generate_for(rcgen_alg).unwrap();
        let raw = key.public_key_raw().to_vec();
        let half = (raw.len() - 1) / 2;
        let encoding = EncodingKey::from_ec_pem(key.serialize_pem().as_bytes()).unwrap();
        let alg_name = format!("{alg:?}");
        Self {
            kid: kid.to_string(),
            alg,
            encoding,
            jwk: json!({
                "kty": "EC", "crv": curve, "use": "sig", "alg": alg_name, "kid": kid,
                "x": B64.encode(&raw[1..1 + half]), "y": B64.encode(&raw[1 + half..]),
            }),
        }
    }

    /// A fresh P-256 key, `ES256`.
    pub fn es256(kid: &str) -> Self {
        Self::ec(
            kid,
            Algorithm::ES256,
            "P-256",
            &rcgen::PKCS_ECDSA_P256_SHA256,
        )
    }

    /// A fresh P-384 key, `ES384`.
    pub fn es384(kid: &str) -> Self {
        Self::ec(
            kid,
            Algorithm::ES384,
            "P-384",
            &rcgen::PKCS_ECDSA_P384_SHA384,
        )
    }

    /// The process-wide 2048-bit RSA key (generating one is slow in a debug build) under `kid`,
    /// with `alg` one of the RS or PS algorithms.
    pub fn rsa(kid: &str, alg: Algorithm) -> Self {
        let key = rsa_key();
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let public = key.to_public_key();
        Self {
            kid: kid.to_string(),
            alg,
            encoding: EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
            jwk: json!({
                "kty": "RSA", "use": "sig", "alg": format!("{alg:?}"), "kid": kid,
                "n": B64.encode(public.n().to_bytes_be()), "e": B64.encode(public.e().to_bytes_be()),
            }),
        }
    }

    /// `RS256`, what Keycloak signs with by default.
    pub fn rs256(kid: &str) -> Self {
        Self::rsa(kid, Algorithm::RS256)
    }

    pub fn jwk(&self) -> Value {
        self.jwk.clone()
    }

    /// The same key without the `kid` (and `alg`) members in its JWK, for a JWKS that names none.
    pub fn anonymous_jwk(&self) -> Value {
        let mut jwk = self.jwk();
        let obj = jwk.as_object_mut().unwrap();
        obj.remove("kid");
        obj.remove("alg");
        jwk
    }

    /// A signed token whose header names this key's `kid`.
    pub fn sign(&self, claims: &Value) -> String {
        let mut header = Header::new(self.alg);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, claims, &self.encoding).unwrap()
    }

    /// A signed token with no `kid` in its header.
    pub fn sign_without_kid(&self, claims: &Value) -> String {
        jsonwebtoken::encode(&Header::new(self.alg), claims, &self.encoding).unwrap()
    }
}

/// `alg: none` with an empty signature: the classic forgery.
pub fn forge_none(claims: &Value) -> String {
    let header = B64.encode(json!({"alg": "none", "typ": "JWT"}).to_string());
    format!("{header}.{}.", B64.encode(claims.to_string()))
}

/// An `HS256` token keyed with `secret` (the algorithm-confusion attack signs with the public
/// key's bytes as the secret).
pub fn forge_hs256(kid: &str, claims: &Value, secret: &[u8]) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(&header, claims, &EncodingKey::from_secret(secret)).unwrap()
}

/// A JWKS document with `keys` as is.
pub fn jwks_document(keys: &[Value]) -> Vec<u8> {
    json!({ "keys": keys }).to_string().into_bytes()
}

/// The JWKS Keycloak serves: the signing keys plus the realm's RSA-OAEP encryption key, which a
/// verifier must skip.
pub fn keycloak_jwks(signers: &[&Signer]) -> Vec<u8> {
    let mut keys: Vec<Value> = signers.iter().map(|s| s.jwk()).collect();
    let mut enc = Signer::rs256("enc-key").jwk();
    enc["use"] = json!("enc");
    enc["alg"] = json!("RSA-OAEP");
    keys.push(enc);
    jwks_document(&keys)
}

#[derive(Clone)]
struct Serve {
    body: Vec<u8>,
    status: u16,
    hits: usize,
    delay: Duration,
}

/// A JWKS endpoint on `127.0.0.1` that tests reconfigure while it runs.
pub struct JwksServer {
    pub addr: SocketAddr,
    scheme: &'static str,
    state: Arc<Mutex<Serve>>,
}

async fn serve_jwks(State(state): State<Arc<Mutex<Serve>>>) -> impl IntoResponse {
    let (snapshot, delay) = {
        let mut s = state.lock().unwrap();
        s.hits += 1;
        (s.clone(), s.delay)
    };
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    (
        StatusCode::from_u16(snapshot.status).unwrap(),
        [(header::CONTENT_TYPE, "application/json")],
        snapshot.body,
    )
}

fn router(state: Arc<Mutex<Serve>>) -> Router {
    Router::new()
        .route("/certs", get(serve_jwks))
        .with_state(state)
}

fn fresh_state() -> Arc<Mutex<Serve>> {
    Arc::new(Mutex::new(Serve {
        body: jwks_document(&[]),
        status: 200,
        hits: 0,
        delay: Duration::ZERO,
    }))
}

impl JwksServer {
    /// Serves `keys` (Keycloak's layout) over plain HTTP on a free port.
    pub async fn start(signers: &[&Signer]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = Self::on(listener, "http");
        server.serve(signers);
        server
    }

    /// Like [`JwksServer::start`] on the IPv6 loopback, `None` when the host has none.
    pub async fn start_ipv6(signers: &[&Signer]) -> Option<Self> {
        let listener = TcpListener::bind("[::1]:0").await.ok()?;
        let server = Self::on(listener, "http");
        server.serve(signers);
        Some(server)
    }

    /// A free address nobody listens on yet; pass it to [`JwksServer::start_at`] later.
    pub async fn reserve() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    /// Like [`JwksServer::start`] on an address from [`JwksServer::reserve`].
    pub async fn start_at(addr: SocketAddr, signers: &[&Signer]) -> Self {
        let listener = TcpListener::bind(addr).await.unwrap();
        let server = Self::on(listener, "http");
        server.serve(signers);
        server
    }

    /// A handle that only knows the URL of a server that is not (or not yet) running.
    pub fn unbound(addr: SocketAddr) -> Self {
        Self {
            addr,
            scheme: "http",
            state: fresh_state(),
        }
    }

    pub fn with_scheme(mut self, scheme: &'static str) -> Self {
        self.scheme = scheme;
        self
    }

    fn on(listener: TcpListener, scheme: &'static str) -> Self {
        let addr = listener.local_addr().unwrap();
        let state = fresh_state();
        let app = router(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            addr,
            scheme,
            state,
        }
    }

    /// Serves `keys` over TLS with `cert_pem` and `key_pem`.
    pub async fn start_tls(cert_pem: &str, key_pem: &str, signers: &[&Signer]) -> Self {
        Self::start_tls_on("127.0.0.1:0", cert_pem, key_pem, signers)
            .await
            .unwrap()
    }

    /// Like [`JwksServer::start_tls`], bound to `bind`; `None` when it cannot be bound.
    pub async fn start_tls_on(
        bind: &str,
        cert_pem: &str,
        key_pem: &str,
        signers: &[&Signer],
    ) -> Option<Self> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(cert_pem.as_bytes())
                .map(Result::unwrap)
                .collect();
        let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let tcp = TcpListener::bind(bind).await.ok()?;
        let addr = tcp.local_addr().unwrap();
        let state = fresh_state();
        let app = router(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(TlsListener { tcp, acceptor }, app)
                .await
                .unwrap();
        });
        let server = Self {
            addr,
            scheme: "https",
            state,
        };
        server.serve(signers);
        Some(server)
    }

    /// `http(s)://<addr>/certs`.
    pub fn url(&self) -> String {
        format!("{}://{}/certs", self.scheme, self.addr)
    }

    /// Serves these keys from now on (status 200).
    pub fn serve(&self, signers: &[&Signer]) {
        self.set_body(keycloak_jwks(signers));
    }

    pub fn set_body(&self, body: Vec<u8>) {
        let mut s = self.state.lock().unwrap();
        s.body = body;
        s.status = 200;
    }

    pub fn set_status(&self, status: u16) {
        self.state.lock().unwrap().status = status;
    }

    pub fn set_delay(&self, delay: Duration) {
        self.state.lock().unwrap().delay = delay;
    }

    /// Requests received so far.
    pub fn hits(&self) -> usize {
        self.state.lock().unwrap().hits
    }
}

struct TlsListener {
    tcp: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let Ok((stream, addr)) = self.tcp.accept().await else {
                continue;
            };
            if let Ok(tls) = self.acceptor.accept(stream).await {
                return (tls, addr);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

/// `(ca PEM, server certificate PEM, server key PEM)` for a server reached as `127.0.0.1` or
/// `[::1]`.
pub fn tls_pair() -> (String, String, String) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "jwks test ca");
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    for ip in [
        std::net::IpAddr::from(std::net::Ipv4Addr::LOCALHOST),
        std::net::IpAddr::from(std::net::Ipv6Addr::LOCALHOST),
    ] {
        params.subject_alt_names.push(rcgen::SanType::IpAddress(ip));
    }
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let cert = params.signed_by(&key, &issuer).unwrap();
    (ca.pem(), cert.pem(), key.serialize_pem())
}
