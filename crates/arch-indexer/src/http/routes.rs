//! The runtime's own routes: liveness, readiness, version, sync status and the
//! Prometheus text endpoint every indexer appends to.

use super::{cache, internal, ApiResult, HostState};
use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::fmt::Write;

pub async fn healthz() -> &'static str {
    "ok"
}

/// Readiness (vs. `/healthz` liveness): confirms the DB is actually reachable,
/// so a load balancer stops routing to a replica whose pool is exhausted or
/// whose database is down.
pub async fn readyz(State(state): State<HostState>) -> Result<&'static str, (StatusCode, String)> {
    sqlx::query("SELECT 1")
        .execute(&state.api.pool)
        .await
        .map_err(|e| {
            tracing::warn!(%e, "readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready".into())
        })?;
    Ok("ready")
}

/// What this build is, what schema it is talking to and what revision of the
/// numbers each indexer produces.
///
/// Deliberately does NOT reach the indexer. A version endpoint that depends on
/// an upstream being healthy is useless in exactly the incident where you need
/// it.
pub async fn version(State(state): State<HostState>) -> ApiResult {
    // An unreadable schema version means the DB is unreachable, not that there
    // is no schema — report null rather than failing the endpoint.
    let schema_versions = crate::db::schema_versions(&state.api.pool)
        .await
        .map(|rows| {
            Value::Object(
                rows.into_iter()
                    .map(|(namespace, version)| (namespace, json!(version)))
                    .collect(),
            )
        })
        .unwrap_or_else(|err| {
            tracing::warn!(%err, "could not read schema versions");
            Value::Null
        });
    let indexers: Vec<Value> = state
        .indexers
        .iter()
        .map(|ix| {
            json!({
                "id": ix.id(),
                "program_ids": ix.program_ids(),
                "data_revision": ix.data_revision(),
            })
        })
        .collect();
    let b = &state.build;
    Ok(Json(json!({
        "service": b.service,
        "version": b.version,
        "commit": b.git_sha,
        "built_at": b.built_at,
        "network": state.network,
        "schema_versions": schema_versions,
        "indexers": indexers,
    })))
}

/// This host's cursors: each checkpoint, the indexer tip its last completed
/// pass began at (`scan_marks`), and any replay standing for it.
///
/// A checkpoint above `tip_height` is a cursor from a chain that no longer
/// exists: the chain restarted from genesis and the tail polls forever while
/// `updated_at` stays fresh. Nothing else on the API reports that.
pub async fn sync_status(State(state): State<HostState>) -> ApiResult {
    let rows: Vec<(String, i64, chrono::DateTime<chrono::Utc>, Option<i64>)> = sqlx::query_as(
        "SELECT c.name, c.block_height, c.updated_at, s.tip_height
         FROM checkpoints c LEFT JOIN scan_marks s USING (name)
         WHERE c.name = ANY($1)
         ORDER BY c.name",
    )
    .bind(state.checkpoint_keys.as_slice())
    .fetch_all(&state.api.pool)
    .await
    .map_err(internal)?;
    let checkpoints: Vec<Value> = rows
        .into_iter()
        .map(|(name, height, updated_at, tip_height)| json!({ "name": name, "block_height": height, "updated_at": updated_at, "tip_height": tip_height }))
        .collect();
    // A replay keeps its checkpoint's updated_at fresh while it walks history
    // below block_height; this is where it shows.
    let replays: Vec<(String, i32, i64)> = sqlx::query_as(
        "SELECT checkpoint, data_revision, through_height FROM replay_requests
         WHERE checkpoint = ANY($1) ORDER BY checkpoint",
    )
    .bind(state.checkpoint_keys.as_slice())
    .fetch_all(&state.api.pool)
    .await
    .map_err(internal)?;
    let replays: Vec<Value> = replays
        .into_iter()
        .map(|(checkpoint, revision, through)| json!({ "checkpoint": checkpoint, "data_revision": revision, "through_height": through }))
        .collect();
    Ok(Json(
        json!({ "checkpoints": checkpoints, "replays": replays }),
    ))
}

/// Prometheus text format: the runtime's service-health gauges, then each
/// indexer's `metrics` lines.
pub async fn prometheus_metrics(
    State(state): State<HostState>,
) -> Result<String, (StatusCode, String)> {
    let mut out = String::new();
    service_health(&state, &mut out).await.map_err(internal)?;
    for ix in state.indexers.iter() {
        ix.metrics(&state.api.pool, &mut out)
            .await
            .map_err(internal)?;
    }
    Ok(out)
}

async fn service_health(state: &HostState, out: &mut String) -> anyhow::Result<()> {
    let pool = &state.api.pool;
    // Constant-1 gauge whose LABELS carry the payload — the Prometheus idiom
    // for build metadata, so a chart can be annotated where the build changed.
    let b = &state.build;
    out.push_str("# TYPE program_indexer_build_info gauge\n");
    writeln!(
        out,
        "program_indexer_build_info{{service=\"{}\",version=\"{}\",commit=\"{}\",network=\"{}\"}} 1",
        b.service, b.version, b.git_sha, state.network
    )?;
    out.push_str("# TYPE program_indexer_data_revision gauge\n");
    for ix in state.indexers.iter() {
        writeln!(
            out,
            "program_indexer_data_revision{{indexer=\"{}\"}} {}",
            ix.id(),
            ix.data_revision()
        )?;
    }
    if let Ok(versions) = crate::db::schema_versions(pool).await {
        out.push_str("# TYPE program_indexer_schema_version gauge\n");
        for (namespace, v) in versions {
            writeln!(
                out,
                "program_indexer_schema_version{{namespace=\"{namespace}\"}} {v}"
            )?;
        }
    }

    // How stale each cursor is: the primary "ingestion frozen" detector.
    let checkpoints: Vec<(String, i64, f64)> = sqlx::query_as(
        "SELECT name, block_height, EXTRACT(EPOCH FROM now() - updated_at)::float8
         FROM checkpoints WHERE name = ANY($1) ORDER BY name",
    )
    .bind(state.checkpoint_keys.as_slice())
    .fetch_all(pool)
    .await?;
    out.push_str("# TYPE program_indexer_checkpoint_height gauge\n");
    out.push_str("# TYPE program_indexer_checkpoint_age_seconds gauge\n");
    for (name, height, age) in &checkpoints {
        writeln!(
            out,
            "program_indexer_checkpoint_height{{source=\"{name}\"}} {height}"
        )?;
        writeln!(
            out,
            "program_indexer_checkpoint_age_seconds{{source=\"{name}\"}} {age}"
        )?;
    }

    // Decoder gaps: an instruction the decoder recognized as its program's but
    // could not decode. A nonzero count means data is being dropped — the
    // signal that a program upgrade outran the decoder. Alert on
    // `increase(...)`, not on `> 0`: known-undecoded instructions make it
    // nonzero from the first backfill.
    let ids: Vec<&str> = state.indexers.iter().map(|ix| ix.id()).collect();
    let unknowns: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT decoder, discriminator, count(*) FROM decoder_unknowns
         WHERE decoder = ANY($1) GROUP BY 1, 2 ORDER BY 1, 2",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    out.push_str("# TYPE program_indexer_decoder_unknown_total counter\n");
    for (decoder, disc, n) in &unknowns {
        writeln!(
            out,
            "program_indexer_decoder_unknown_total{{decoder=\"{decoder}\",discriminator=\"{disc}\"}} {n}"
        )?;
    }

    // Cache effectiveness: a hit ratio near zero means callers are sending
    // unstable keys (e.g. a jittery `from`) and the cache is doing nothing.
    let hits = cache::CACHE_HITS.load(std::sync::atomic::Ordering::Relaxed);
    let misses = cache::CACHE_MISSES.load(std::sync::atomic::Ordering::Relaxed);
    out.push_str("# TYPE program_indexer_cache_requests_total counter\n");
    writeln!(
        out,
        "program_indexer_cache_requests_total{{result=\"hit\"}} {hits}"
    )?;
    writeln!(
        out,
        "program_indexer_cache_requests_total{{result=\"miss\"}} {misses}"
    )?;
    Ok(())
}
