//! The dashboard's static assets (`ui/`), compiled into the binary with `rust-embed` and served
//! under `/ui/`. Plain HTML, CSS and JavaScript modules written for the hub: no
//! third-party code.
//!
//! Caching: `/ui/` (the page) is `no-cache`. It references every asset under
//! `/ui/v/<build>/...`, where `<build>` is a hash of all embedded files, so a hub with different
//! assets has different asset URLs and those can be cached as immutable. Relative imports inside
//! the modules inherit the prefix. A request for another build id still gets the current file,
//! but `no-cache`, so a stale tab does not pin it.

use std::sync::LazyLock;

use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use rust_embed::Embed;
use xxhash_rust::xxh64::xxh64;

#[derive(Embed)]
#[folder = "ui/"]
struct Assets;

/// The page; `{{build}}` in it is replaced by [`build_id`].
const INDEX: &str = "index.html";
const BUILD_PLACEHOLDER: &str = "{{build}}";

static BUILD_ID: LazyLock<String> = LazyLock::new(|| {
    let mut names: Vec<String> = Assets::iter().map(|n| n.into_owned()).collect();
    names.sort();
    let mut bytes = Vec::new();
    for name in names {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        if let Some(file) = Assets::get(&name) {
            bytes.extend_from_slice(&file.metadata.sha256_hash());
        }
    }
    format!("{:012x}", xxh64(&bytes, 0) >> 16)
});

/// Hash of every embedded asset (name and content), 12 hex digits.
pub fn build_id() -> &'static str {
    &BUILD_ID
}

/// Every embedded file name, sorted.
pub fn names() -> Vec<String> {
    let mut names: Vec<String> = Assets::iter().map(|n| n.into_owned()).collect();
    names.sort();
    names
}

/// `Content-Type` by extension; the dashboard only ships these kinds.
pub fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

const NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");
const IMMUTABLE: HeaderValue = HeaderValue::from_static("public, max-age=31536000, immutable");

/// `GET /ui/`: the page, with this build's asset prefix.
pub async fn index() -> Response {
    let Some(file) = Assets::get(INDEX) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let page = String::from_utf8_lossy(&file.data).replace(BUILD_PLACEHOLDER, build_id());
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(content_type(INDEX)),
            ),
            (header::CACHE_CONTROL, NO_CACHE),
        ],
        page,
    )
        .into_response()
}

/// `GET /ui/v/{build}/{*path}`: one asset; immutable for the current build, `no-cache` for
/// another one. The page itself is only served at `/ui/`.
pub async fn asset(Path((build, path)): Path<(String, String)>) -> Response {
    if path == INDEX {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(file) = Assets::get(&path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let cache = if build == build_id() {
        IMMUTABLE
    } else {
        NO_CACHE
    };
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(content_type(&path)),
            ),
            (header::CACHE_CONTROL, cache),
        ],
        file.data.into_owned(),
    )
        .into_response()
}

/// `GET /ui` and `GET /`: to `/ui/`. The target is relative, so the redirect also works behind
/// a reverse proxy that mounts the hub under a path.
pub async fn to_index() -> Redirect {
    Redirect::temporary("ui/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_and_its_modules_are_embedded() {
        let names = names();
        for required in ["index.html", "app.js", "app.css", "favicon.svg"] {
            assert!(
                names.iter().any(|n| n == required),
                "{required} missing: {names:?}"
            );
        }
        assert_eq!(build_id().len(), 12);
        assert!(build_id().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn content_types_by_extension() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("pages/types.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("app.css"), "text/css; charset=utf-8");
        assert_eq!(content_type("favicon.svg"), "image/svg+xml");
        assert_eq!(content_type("README"), "application/octet-stream");
    }
}
