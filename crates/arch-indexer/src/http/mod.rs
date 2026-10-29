//! HTTP scaffolding: the runtime's routes plus every indexer's [`Routes`],
//! each group behind its cache and concurrency gate, CORS, and the version
//! header on every response.

pub mod cache;
pub mod chart;
pub mod paging;
pub mod prometheus;
pub mod routes;

use crate::build_info::BuildInfo;
use crate::config::Config;
use crate::indexer::{ApiState, DynIndexer, Routes};
use axum::{
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json, Router,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub type ApiError = (StatusCode, String);
pub type ApiResult = Result<Json<Value>, ApiError>;

/// Log a failed query and answer an opaque 500.
pub fn internal(err: impl std::fmt::Display) -> ApiError {
    tracing::error!(%err, "query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
}

/// Canonicalize an address to lowercase hex, decoding base58 (mirrors the
/// indexer's `normalize_to_hex`). Tables store addresses as lowercase hex and
/// wallets send base58. `None` when the input doesn't resolve to a 32-byte
/// pubkey, so junk is refused before any work happens.
pub fn normalize_address(s: &str) -> Option<String> {
    if s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Some(s.to_lowercase());
    }
    if let Ok(bytes) = bs58::decode(s).into_vec() {
        if bytes.len() == 32 {
            return Some(hex::encode(bytes));
        }
    }
    None
}

/// Header naming the build that served the response, e.g. `0.2.0+ea07a48`.
pub const VERSION_HEADER: &str = "x-indexer-version";

/// State of the runtime's own routes.
#[derive(Clone)]
pub struct HostState {
    pub api: ApiState,
    pub build: Arc<BuildInfo>,
    pub network: String,
    pub indexers: Arc<[Arc<dyn DynIndexer>]>,
    /// Every checkpoint key this host's indexers own; the sync and metrics
    /// views report only these, since other services may share the database.
    pub checkpoint_keys: Arc<Vec<String>>,
    /// `METRICS_PREFIX`: the head of the runtime's own metric names.
    pub metrics_prefix: Arc<str>,
}

/// How long a request waits for a permit before it is shed. A permit is held
/// for tens of milliseconds on the per-address routes, so a page that fires
/// its reads at once queues here for a moment instead of losing some of them.
const GATE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Cap a group's concurrent handlers at the gate's permits: a request waits
/// up to `GATE_WAIT` for one, then sheds as a retryable 503. It waits on the
/// semaphore, never on the connection pool, so Postgres still sees at most
/// the permits and the ungated probes keep their connections. Requests that
/// never reach a handler (cache hits) never consume a permit.
///
/// A global cap, not per-IP fairness: per-IP limiting belongs at the edge.
async fn concurrency_gate(State(sem): State<Arc<Semaphore>>, req: Request, next: Next) -> Response {
    match tokio::time::timeout(GATE_WAIT, sem.acquire()).await {
        Ok(Ok(_permit)) => next.run(req).await,
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            "busy, retry shortly",
        )
            .into_response(),
    }
}

/// The permits of the `no_store` group.
const NO_STORE_PERMITS: usize = 1;

/// The whole API: the runtime's routes and every indexer's groups.
///
/// INVARIANT: `query_concurrency + per_address_concurrency` plus the
/// `no_store` group's one permit must stay BELOW `DB_MAX_CONNECTIONS`
/// (default 10). Every group draws from the same serving pool, and the
/// ungated `/healthz`, `/readyz`, `/v1/sync-status` and `/metrics` need a
/// connection left over, or load makes a dead replica look healthy. The gates
/// are shared by every indexer mounted, so the budget holds however many there
/// are; an indexer that spawns its own detached database work must count it
/// here too.
pub fn router(
    api: ApiState,
    config: &Config,
    build: BuildInfo,
    indexers: &[Arc<dyn DynIndexer>],
) -> Router {
    use axum::routing::get;

    let mut groups = Routes::default();
    for ix in indexers {
        let Routes {
            cached,
            per_address,
            no_store,
        } = ix.routes(&api);
        groups.cached = groups.cached.merge(cached);
        groups.per_address = groups.per_address.merge(per_address);
        groups.no_store = groups.no_store.merge(no_store);
    }

    let checkpoint_keys = indexers
        .iter()
        .flat_map(|ix| {
            ix.program_ids()
                .into_iter()
                .map(|p| ix.checkpoint_key(&p))
                .collect::<Vec<_>>()
        })
        .collect();
    let version = HeaderValue::from_str(&build.describe())
        .unwrap_or_else(|_| HeaderValue::from_static("unknown"));
    let host = HostState {
        api,
        build: Arc::new(build),
        network: config.network().to_string(),
        indexers: indexers.iter().cloned().collect(),
        checkpoint_keys: Arc::new(checkpoint_keys),
        metrics_prefix: config.metrics_prefix.as_str().into(),
    };

    let response_cache = cache::ResponseCache::new(config.cache_ttl_secs);

    // Aggregate reads: identical for every caller, pure functions of committed
    // data. The in-process cache collapses identical requests (and every
    // Prometheus scrape) into one DB hit — the load-shedding layer that
    // protects Postgres. `public, s-maxage` lets a CDN in front share
    // responses too; `stale-while-revalidate` keeps the last good body serving
    // through a brief origin blip.
    //
    // Layer order (last added = outermost): cache → gate → header stamp →
    // handler. map_response (innermost) stamps the header first, so the cache
    // stores the body WITH the header and replays it on a hit; and because the
    // cache sits outside the gate, a HIT returns without consuming a permit.
    // /metrics is cached but NEVER gated: a scrape that 503s under load blinds
    // the alerting exactly when it is needed.
    let metrics = Router::new()
        .route("/metrics", get(routes::prometheus_metrics))
        .with_state(host.clone())
        .layer(axum::middleware::map_response(cache_public))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&response_cache),
            cache::cache_layer,
        ));

    let cached = groups
        .cached
        .layer(axum::middleware::map_response(cache_public))
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(Semaphore::new(config.query_concurrency)),
            concurrency_gate,
        ))
        .layer(axum::middleware::from_fn_with_state(
            response_cache,
            cache::cache_layer,
        ));

    // Per-address reads: high key cardinality, so they stay out of the shared
    // in-process cache and out of shared HTTP caches — a short private window
    // is enough for one viewer's repeated polls. With no server-side cache
    // every request reaches Postgres; its own gate keeps that path off the
    // aggregate routes' budget.
    let per_address = groups
        .per_address
        .layer(axum::middleware::map_response(cache_private))
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(Semaphore::new(config.per_address_concurrency)),
            concurrency_gate,
        ));

    // Long or secret reads: never stored anywhere (the response cache's key
    // ignores headers, so a hit would skip an auth check), one at a time.
    let no_store = groups
        .no_store
        .layer(axum::middleware::map_response(
            |mut response: Response| async move {
                response.headers_mut().insert(
                    axum::http::header::CACHE_CONTROL,
                    HeaderValue::from_static("no-store"),
                );
                response
            },
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(Semaphore::new(NO_STORE_PERMITS)),
            concurrency_gate,
        ));

    let probes = Router::new()
        .route("/v1/version", get(routes::version))
        .route("/healthz", get(routes::healthz))
        .route("/readyz", get(routes::readyz))
        .route("/v1/sync-status", get(routes::sync_status))
        .with_state(host);

    Router::new()
        .merge(no_store)
        .merge(probes)
        .merge(metrics)
        .merge(cached)
        .merge(per_address)
        // Read-only public data; browsers may hit us directly. The version
        // header must be explicitly exposed or a browser cannot read it off a
        // cross-origin response, even though it is sent.
        .layer(
            tower_http::cors::CorsLayer::permissive()
                .expose_headers([axum::http::HeaderName::from_static(VERSION_HEADER)]),
        )
        // Outermost, so EVERY response carries it: cached replays, 503s from
        // the gate, and handler errors alike.
        .layer(axum::middleware::map_response_with_state(
            version,
            stamp_version,
        ))
}

async fn stamp_version(State(version): State<HeaderValue>, mut resp: Response) -> Response {
    resp.headers_mut()
        .insert(axum::http::HeaderName::from_static(VERSION_HEADER), version);
    resp
}

/// Shared-cache directive for aggregate reads; errors are left uncached so a
/// transient failure isn't pinned. `s-maxage` targets shared caches (a CDN /
/// this origin's in-proc layer); no browser `max-age`, so browsers revalidate.
async fn cache_public(mut resp: Response) -> Response {
    if resp.status().is_success() {
        resp.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("public, s-maxage=30, stale-while-revalidate=120"),
        );
    }
    resp
}

/// Private, browser-only short cache for per-address responses — never stored
/// by a shared cache.
async fn cache_private(mut resp: Response) -> Response {
    if resp.status().is_success() {
        resp.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("private, max-age=10"),
        );
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get};
    use tower::ServiceExt;

    /// Excess concurrency must shed as a retryable 503 once the wait runs
    /// out, not queue onto the connection pool — queueing there is what
    /// exhausts Postgres and starves /readyz.
    #[tokio::test(start_paused = true)]
    async fn gate_sheds_excess_concurrency_as_503() {
        let sem = Arc::new(Semaphore::new(1));
        let app = Router::new().route("/x", get(|| async { "ok" })).layer(
            axum::middleware::from_fn_with_state(Arc::clone(&sem), concurrency_gate),
        );

        let req = || Request::builder().uri("/x").body(Body::empty()).unwrap();

        // A permit is available: the handler runs.
        let ok = app.clone().oneshot(req()).await.unwrap();
        assert_eq!(ok.status(), StatusCode::OK);

        // Hold the only permit past the wait: the next request is shed.
        let held = sem.try_acquire().unwrap();
        let busy = app.clone().oneshot(req()).await.unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(busy.headers().get("retry-after").unwrap(), "1");

        // Released within the wait: the queued request runs instead of
        // shedding — a page's simultaneous reads all land.
        let queued = tokio::spawn(app.clone().oneshot(req()));
        tokio::time::sleep(GATE_WAIT / 4).await;
        drop(held);
        assert_eq!(queued.await.unwrap().unwrap().status(), StatusCode::OK);

        // Released: capacity returns.
        let ok = app.oneshot(req()).await.unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
    }

    /// A bug report quoting the version header must name the build that
    /// actually produced the body — so the header has to survive a cached
    /// replay and a gate rejection, not just the happy path.
    #[tokio::test(start_paused = true)]
    async fn version_header_is_stamped_on_every_response() {
        let expected = "1.2.3+abcdef0";
        let stamp = || {
            axum::middleware::map_response_with_state(
                HeaderValue::from_static(expected),
                stamp_version,
            )
        };
        let req = || Request::builder().uri("/x").body(Body::empty()).unwrap();

        // Happy path.
        let app = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(stamp());
        let ok = app.clone().oneshot(req()).await.unwrap();
        assert_eq!(ok.headers()[VERSION_HEADER], expected);

        // A 503 shed by the gate still names the build.
        let sem = Arc::new(Semaphore::new(1));
        let gated = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&sem),
                concurrency_gate,
            ))
            .layer(stamp());
        let _held = sem.try_acquire().unwrap();
        let busy = gated.oneshot(req()).await.unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(busy.headers()[VERSION_HEADER], expected);

        // A cache HIT is rebuilt from stored parts; the stamp layer sits outside
        // the cache, so the replay must carry it too.
        let cached = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                cache::ResponseCache::new(30),
                cache::cache_layer,
            ))
            .layer(stamp());
        assert_eq!(
            cached.clone().oneshot(req()).await.unwrap().headers()["x-cache"],
            "MISS"
        );
        let hit = cached.oneshot(req()).await.unwrap();
        assert_eq!(hit.headers()["x-cache"], "HIT");
        assert_eq!(hit.headers()[VERSION_HEADER], expected);
    }

    /// The gates must leave headroom in the pool for the ungated routes
    /// (/healthz, /readyz, /v1/sync-status, /metrics), or exhaustion makes a
    /// dead replica look healthy.
    #[test]
    fn gate_budgets_stay_below_the_connection_pool() {
        let db_max = crate::db::DEFAULT_MAX_CONNECTIONS as usize;
        let budget = crate::config::DEFAULT_QUERY_CONCURRENCY
            + crate::config::DEFAULT_PER_ADDRESS_CONCURRENCY
            + NO_STORE_PERMITS;
        assert!(
            budget < db_max,
            "concurrent DB users ({budget}) must stay below the pool ({db_max}) \
             so health checks always have a connection"
        );
    }

    #[test]
    fn normalize_address_canonicalizes_base58_and_hex_case() {
        let bytes = [0xABu8; 32];
        let hex_lower = hex::encode(bytes);
        let b58 = bs58::encode(bytes).into_string();
        assert_eq!(normalize_address(&b58).as_deref(), Some(hex_lower.as_str()));
        assert_eq!(
            normalize_address(&hex_lower.to_uppercase()).as_deref(),
            Some(hex_lower.as_str())
        );
        assert_eq!(
            normalize_address(&hex_lower).as_deref(),
            Some(hex_lower.as_str())
        );
        // Junk that isn't a 32-byte key is rejected, not forwarded.
        assert_eq!(normalize_address("hello"), None);
        assert_eq!(normalize_address("abcd12"), None); // hex but not 32 bytes
        assert_eq!(normalize_address(""), None);
    }
}
