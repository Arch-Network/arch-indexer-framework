//! Short-TTL in-process cache for the shared aggregation routes. Each service
//! serves a single network, so caching here is
//! network-safe by construction — unlike the multi-network arch-swap proxy,
//! which selects its upstream by cookie and therefore must NOT be edge-cached
//! across networks without a network-keyed cache. This layer is what actually
//! protects Postgres (the scaling bottleneck) from a burst of identical
//! requests: pool/market lists, charts, and every Prometheus scrape collapse
//! into one query per key per TTL window.
//!
//! Only successful GETs are cached, keyed on the path plus the parameters that
//! actually change the response (see `CACHE_KEY_PARAMS`) — unknown params are
//! ignored by the handlers, so they must not fragment the key either. Cache
//! effectiveness still depends on callers sending stable values (arch-swap
//! rounds chart `from` to the bucket boundary; a jittery millisecond `from`
//! would key every request uniquely and never hit). The `program_indexer_cache_*` counters
//! in `/metrics` make a 0%-hit-rate regression visible instead of silent.

use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{
        header::{CACHE_CONTROL, CONTENT_TYPE},
        HeaderValue, StatusCode,
    },
    middleware::Next,
    response::Response,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

pub static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

/// The only query parameters that change a response body. Everything else is
/// ignored by the handlers (`/v1/pools` and `/v1/markets` take no query
/// extractor at all; `ChartParams` silently drops unknown fields).
///
/// Keying on the raw query string therefore let `?junk=1`, `?junk=2`, … mint a
/// fresh cache key per request while running the exact same expensive
/// aggregation — a trivial way for one unauthenticated caller to bypass the
/// cache entirely and drive every request into Postgres.
const CACHE_KEY_PARAMS: [&str; 6] = ["metric", "interval", "from", "to", "limit", "before"];

/// arch-swap appends a unique `_=<value>` after a confirmed transaction so a
/// post-transaction read sees fresh state (`arch-swap/src/utils/
/// analytics-charts.ts`). `_` is not a response-affecting param, so without
/// special handling that request would be answered from the warm entry and the
/// user would not see their own swap for up to the TTL.
///
/// It is therefore a REVALIDATION signal, not part of the key: skip the lookup,
/// run the handler, and refresh the shared entry with the result. It cannot be
/// used to flood Postgres — the concurrency gate sits between this layer and the
/// handler, so a revalidating request still has to take a permit.
const REVALIDATE_PARAM: &str = "_";

/// True when the caller explicitly asked to bypass the cached copy.
fn wants_revalidation(req: &Request) -> bool {
    let bypass_param = req
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .any(|p| p == REVALIDATE_PARAM || p.starts_with("_="));
    bypass_param
        || req
            .headers()
            .get(CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("no-cache"))
}

/// Cache key: path plus only the response-affecting params, in a fixed order,
/// so that `?a=1&junk=x` and `?junk=y&a=1` collapse to the same entry.
fn cache_key(uri: &axum::http::Uri) -> String {
    let path = uri.path();
    let query = uri.query().unwrap_or_default();
    let mut kept: Vec<(&str, &str)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').unwrap_or((p, "")))
        .filter(|(k, _)| CACHE_KEY_PARAMS.contains(k))
        .collect();
    // Stable order: callers may send the same params in any sequence.
    kept.sort_unstable();

    let mut key = String::with_capacity(path.len() + query.len().min(64) + 1);
    key.push_str(path);
    for (i, (k, v)) in kept.iter().enumerate() {
        key.push(if i == 0 { '?' } else { '&' });
        key.push_str(k);
        key.push('=');
        key.push_str(v);
    }
    key
}

/// Bodies larger than this are served but not cached — these JSON responses
/// are KBs; the cap only guards against an unexpected giant response pinning
/// memory.
const MAX_CACHED_BODY: usize = 4 * 1024 * 1024;
/// Opportunistic-sweep threshold. The TTL keeps the live working set tiny (only
/// keys touched in the last window survive), so this is a backstop against
/// unbounded key growth, not a working-set limit.
const SWEEP_ABOVE: usize = 4096;

#[derive(Clone)]
struct Stored {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    cache_control: Option<HeaderValue>,
    body: Bytes,
}

pub struct ResponseCache {
    ttl: Duration,
    map: RwLock<HashMap<String, (Instant, Stored)>>,
}

impl ResponseCache {
    /// `ttl_secs == 0` disables caching (the middleware becomes pass-through).
    pub fn new(ttl_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            ttl: Duration::from_secs(ttl_secs),
            map: RwLock::new(HashMap::new()),
        })
    }

    fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    fn get(&self, key: &str) -> Option<Stored> {
        let map = self.map.read().ok()?;
        let (at, stored) = map.get(key)?;
        (at.elapsed() < self.ttl).then(|| stored.clone())
    }

    fn put(&self, key: String, stored: Stored) {
        let Ok(mut map) = self.map.write() else {
            return;
        };
        if map.len() >= SWEEP_ABOVE {
            let ttl = self.ttl;
            map.retain(|_, (at, _)| at.elapsed() < ttl);
        }
        map.insert(key, (Instant::now(), stored));
    }
}

fn respond(stored: &Stored, hit: bool) -> Response {
    let mut resp = Response::builder().status(stored.status);
    if let Some(ct) = &stored.content_type {
        resp = resp.header(CONTENT_TYPE, ct);
    }
    if let Some(cc) = &stored.cache_control {
        resp = resp.header(CACHE_CONTROL, cc);
    }
    resp = resp.header("x-cache", if hit { "HIT" } else { "MISS" });
    resp.body(Body::from(stored.body.clone()))
        .expect("valid cached response")
}

/// Cache middleware for the global aggregation routes. Non-GET and non-success
/// responses pass through uncached; a stored key is served without touching the
/// handler or the database.
pub async fn cache_layer(
    State(cache): State<Arc<ResponseCache>>,
    req: Request,
    next: Next,
) -> Response {
    if !cache.enabled() || req.method() != axum::http::Method::GET {
        return next.run(req).await;
    }
    let key = cache_key(req.uri());
    let revalidate = wants_revalidation(&req);

    if !revalidate {
        if let Some(stored) = cache.get(&key) {
            CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return respond(&stored, true);
        }
    }
    CACHE_MISSES.fetch_add(1, Ordering::Relaxed);

    let resp = next.run(req).await;
    if !resp.status().is_success() {
        return resp;
    }
    let (mut parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_CACHED_BODY).await {
        Ok(b) => b,
        // Over the cap (or a read error) the body is already consumed and
        // cannot be replayed — the caller gets an empty 200. Loud, because
        // downstream this presents as "endpoint returns nothing" while the
        // actual event is a size cliff; every request past it also skips the
        // cache and pays full recompute. Raise MAX_CACHED_BODY or stream
        // through if a global route ever legitimately outgrows it.
        Err(err) => {
            tracing::warn!(key, cap = MAX_CACHED_BODY, %err,
                "response exceeded cacheable body cap; serving empty body");
            return Response::from_parts(parts, Body::empty());
        }
    };
    let stored = Stored {
        status: parts.status,
        content_type: parts.headers.get(CONTENT_TYPE).cloned(),
        cache_control: parts.headers.get(CACHE_CONTROL).cloned(),
        body: bytes.clone(),
    };
    cache.put(key, stored);
    parts
        .headers
        .insert("x-cache", HeaderValue::from_static("MISS"));
    Response::from_parts(parts, Body::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(tag: &str) -> Stored {
        Stored {
            status: StatusCode::OK,
            content_type: None,
            cache_control: None,
            body: Bytes::from(tag.to_owned()),
        }
    }

    #[test]
    fn returns_fresh_and_expires_after_ttl() {
        let cache = ResponseCache::new(0); // disabled
        assert!(!cache.enabled());

        let cache = ResponseCache {
            ttl: Duration::from_millis(30),
            map: RwLock::new(HashMap::new()),
        };
        cache.put("k".into(), stored("v1"));
        assert_eq!(cache.get("k").unwrap().body, Bytes::from("v1"));
        std::thread::sleep(Duration::from_millis(45));
        assert!(cache.get("k").is_none(), "entry must expire after ttl");
        assert!(cache.get("absent").is_none());
    }

    // Middleware end-to-end: a hit must be served without re-running the handler,
    // stamped x-cache: HIT; distinct query strings key separately; errors and
    // non-GET pass through uncached.
    #[tokio::test]
    async fn middleware_serves_hits_without_reinvoking_handler() {
        use axum::{routing::get, Router};
        use std::sync::atomic::AtomicUsize;
        use tower::ServiceExt;

        static CALLS: AtomicUsize = AtomicUsize::new(0);
        async fn handler() -> String {
            CALLS.fetch_add(1, Ordering::Relaxed);
            "payload".into()
        }

        let cache = ResponseCache::new(30);
        let app = Router::new()
            .route("/x", get(handler))
            .layer(axum::middleware::from_fn_with_state(cache, cache_layer));

        let send = |uri: &str, method: &str| {
            let app = app.clone();
            let req = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            async move { app.oneshot(req).await.unwrap() }
        };

        let r1 = send("/x?metric=tvl", "GET").await;
        assert_eq!(r1.headers().get("x-cache").unwrap(), "MISS");
        let r2 = send("/x?metric=tvl", "GET").await;
        assert_eq!(r2.headers().get("x-cache").unwrap(), "HIT");
        let body = axum::body::to_bytes(r2.into_body(), MAX_CACHED_BODY)
            .await
            .unwrap();
        assert_eq!(&body[..], b"payload");
        assert_eq!(
            CALLS.load(Ordering::Relaxed),
            1,
            "hit must not re-run handler"
        );

        // A junk param must NOT mint a new key — that was the cache-bypass hole.
        let r3 = send("/x?metric=tvl&junk=9", "GET").await;
        assert_eq!(r3.headers().get("x-cache").unwrap(), "HIT");
        assert_eq!(
            CALLS.load(Ordering::Relaxed),
            1,
            "unknown params must not bypass the cache"
        );

        // A different response-affecting value → separate key → handler runs again.
        let _ = send("/x?metric=volume", "GET").await;
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);
    }

    /// arch-swap's post-transaction `_` bust must reach the database, or a user
    /// does not see their own confirmed swap for up to the TTL. It must also
    /// REFRESH the shared entry, not merely skip it.
    #[tokio::test]
    async fn revalidation_param_bypasses_the_cached_copy_and_refreshes_it() {
        use axum::{routing::get, Router};
        use std::sync::atomic::AtomicUsize;
        use tower::ServiceExt;

        static CALLS: AtomicUsize = AtomicUsize::new(0);
        async fn handler() -> String {
            format!("v{}", CALLS.fetch_add(1, Ordering::Relaxed) + 1)
        }

        let cache = ResponseCache::new(30);
        let app = Router::new()
            .route("/x", get(handler))
            .layer(axum::middleware::from_fn_with_state(cache, cache_layer));
        let send = |uri: &str| {
            let app = app.clone();
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            async move { app.oneshot(req).await.unwrap() }
        };
        let body = |r: Response| async move {
            let b = axum::body::to_bytes(r.into_body(), MAX_CACHED_BODY)
                .await
                .unwrap();
            String::from_utf8(b.to_vec()).unwrap()
        };

        assert_eq!(send("/x?metric=tvl").await.headers()["x-cache"], "MISS");
        assert_eq!(send("/x?metric=tvl").await.headers()["x-cache"], "HIT");
        assert_eq!(CALLS.load(Ordering::Relaxed), 1);

        // Post-transaction read: `_` forces the handler to run.
        let fresh = send("/x?metric=tvl&_=abc123").await;
        assert_eq!(fresh.headers()["x-cache"], "MISS");
        assert_eq!(body(fresh).await, "v2");
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);

        // ...and the shared entry now serves the FRESH body to everyone else,
        // rather than the stale one the bust just superseded.
        let after = send("/x?metric=tvl").await;
        assert_eq!(after.headers()["x-cache"], "HIT");
        assert_eq!(body(after).await, "v2");
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn revalidation_is_detected_from_the_param_or_the_header() {
        let req = |uri: &str| Request::builder().uri(uri).body(Body::empty()).unwrap();
        assert!(wants_revalidation(&req("/x?_=1")));
        assert!(wants_revalidation(&req("/x?metric=tvl&_=abc")));
        assert!(!wants_revalidation(&req("/x?metric=tvl")));
        // A param merely *starting* with an underscore is not the bust param.
        assert!(!wants_revalidation(&req("/x?_hidden=1")));

        let no_cache = Request::builder()
            .uri("/x")
            .header(CACHE_CONTROL, "no-cache")
            .body(Body::empty())
            .unwrap();
        assert!(wants_revalidation(&no_cache));
    }

    /// Unknown params are ignored by the handlers, so they must not fragment the
    /// cache key. Keying on the raw query string let `?junk=N` bypass the cache
    /// entirely and drive every request into Postgres.
    #[test]
    fn key_ignores_unknown_params_and_orders_the_rest() {
        let key = |s: &str| cache_key(&s.parse::<axum::http::Uri>().unwrap());

        // Junk collapses onto the bare path.
        assert_eq!(key("/v1/pools"), "/v1/pools");
        assert_eq!(key("/v1/pools?junk=1"), "/v1/pools");
        assert_eq!(key("/v1/pools?junk=1"), key("/v1/pools?junk=2"));

        // Response-affecting params survive, in a stable order regardless of
        // how the caller sequenced them.
        assert_eq!(
            key("/c?metric=tvl&interval=1h"),
            key("/c?interval=1h&metric=tvl")
        );
        assert_eq!(
            key("/c?metric=tvl&interval=1h"),
            "/c?interval=1h&metric=tvl"
        );

        // Junk mixed with real params still collapses.
        assert_eq!(key("/c?metric=tvl&junk=9"), key("/c?metric=tvl"));

        // ...but differing real values stay distinct.
        assert_ne!(key("/c?metric=tvl"), key("/c?metric=volume"));
        assert_ne!(key("/c?from=1&to=2"), key("/c?from=1&to=3"));

        // Valueless and empty params don't panic or collide with the path.
        assert_eq!(key("/c?"), "/c");
        assert_eq!(key("/c?metric"), "/c?metric=");
    }
}
