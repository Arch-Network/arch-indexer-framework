//! Postgres pools and the namespaced migration runner.
//!
//! Several indexers share one database, and each ships its own sqlx migration
//! set numbered from 1. sqlx 0.8 records every set in one `_sqlx_migrations`
//! table keyed by version alone, so two sets would collide. The runner here
//! applies a [`Migrator`]'s migrations itself and records them in
//! `program_indexer_migrations`, keyed by `(namespace, version)`: the runtime's
//! own tables under [`RUNTIME_NAMESPACE`], each indexer's under its
//! [`crate::ProgramIndexer::id`].

use sqlx::migrate::{Migration, Migrator};
use sqlx::Executor;

pub type PgPool = sqlx::PgPool;
pub type PgTransaction<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// Namespace of the runtime's own migrations (`migrations/` in this crate).
/// No indexer may take it as its id.
pub const RUNTIME_NAMESPACE: &str = "runtime";

/// The runtime's shared tables: checkpoints, scan_marks, replay_requests,
/// mint_decimals, decoder_unknowns.
pub static RUNTIME_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// Pool size, overridable per service via `DB_MAX_CONNECTIONS`.
pub fn max_connections() -> u32 {
    std::env::var("DB_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_CONNECTIONS)
}

/// Pool for migrations, ingest and sampling: no statement timeout, since a
/// migration or a backfill page can legitimately run long.
pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections())
        .connect(database_url)
        .await?;
    Ok(pool)
}

/// Serving pool for the read API. Same as [`connect`] but every connection
/// carries a hard `statement_timeout`, so a pathological query (e.g. a chart
/// request coerced into a huge range) can't pin a Postgres backend forever.
///
/// `acquire_timeout` is the readiness backstop. sqlx defaults to 30s, so when
/// the pool is exhausted `/readyz`'s `SELECT 1` would block for 30s before
/// erroring — long enough for a load balancer to consider the replica healthy
/// while it serves nothing. Failing fast makes exhaustion visible.
pub async fn connect_serving(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections())
        .acquire_timeout(std::time::Duration::from_secs(5))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute("SET statement_timeout = '15s'").await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await?;
    Ok(pool)
}

const MIGRATIONS_TABLE: &str = "CREATE TABLE IF NOT EXISTS program_indexer_migrations (
    namespace         text        NOT NULL,
    version           bigint      NOT NULL,
    description       text        NOT NULL,
    checksum          bytea       NOT NULL,
    installed_on      timestamptz NOT NULL DEFAULT now(),
    execution_time_ms bigint      NOT NULL,
    PRIMARY KEY (namespace, version)
)";

/// Apply `migrator`'s pending up-migrations under `namespace`, in version
/// order, each in its own transaction with its bookkeeping row (a `no_tx`
/// migration runs bare, then records itself). An applied migration whose
/// checksum no longer matches is refused, as is an applied version the set
/// no longer contains unless the migrator sets `ignore_missing`.
///
/// Serialized per schema by an advisory lock, so services booting together
/// (an API and an ingest process) apply each migration once.
pub async fn migrate(pool: &PgPool, namespace: &str, migrator: &Migrator) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    let lock = "SELECT pg_advisory_lock(hashtextextended('program_indexer_migrations:' || current_schema(), 0))";
    conn.execute(lock).await?;
    let result = apply(&mut conn, namespace, migrator).await;
    let unlock = "SELECT pg_advisory_unlock(hashtextextended('program_indexer_migrations:' || current_schema(), 0))";
    conn.execute(unlock).await?;
    result
}

async fn apply(
    conn: &mut sqlx::PgConnection,
    namespace: &str,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    conn.execute(MIGRATIONS_TABLE).await?;
    let applied: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, checksum FROM program_indexer_migrations WHERE namespace = $1",
    )
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await?;
    for m in plan(namespace, migrator, &applied)? {
        let started = std::time::Instant::now();
        let failed = |e| anyhow::anyhow!("migration {namespace}/{} failed: {e}", m.version);
        // A bare `&str` runs on the simple-query protocol, so a migration
        // may hold several statements.
        if m.no_tx {
            conn.execute(m.sql.as_str()).await.map_err(failed)?;
            record(namespace, &m, started).execute(&mut *conn).await?;
        } else {
            let mut tx = sqlx::Connection::begin(&mut *conn).await?;
            tx.execute(m.sql.as_str()).await.map_err(failed)?;
            record(namespace, &m, started).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        tracing::info!(namespace, version = m.version, description = %m.description, "migration applied");
    }
    Ok(())
}

/// A migration to apply, owned so nothing borrowed from the migrator is held
/// across an await.
struct Pending {
    version: i64,
    description: String,
    checksum: Vec<u8>,
    sql: String,
    no_tx: bool,
}

/// Check the applied migrations against the set; the pending ones in version
/// order.
fn plan(
    namespace: &str,
    migrator: &Migrator,
    applied: &[(i64, Vec<u8>)],
) -> anyhow::Result<Vec<Pending>> {
    let ups: Vec<&Migration> = migrator
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .collect();
    for (version, checksum) in applied {
        match ups.iter().find(|m| m.version == *version) {
            Some(m) => anyhow::ensure!(
                m.checksum.as_ref() == checksum.as_slice(),
                "migration {namespace}/{version} ({}) was changed after it was applied",
                m.description
            ),
            None => anyhow::ensure!(
                migrator.ignore_missing,
                "migration {namespace}/{version} was applied but is missing from this build"
            ),
        }
    }
    let mut pending: Vec<Pending> = ups
        .into_iter()
        .filter(|m| !applied.iter().any(|(v, _)| *v == m.version))
        .map(|m| Pending {
            version: m.version,
            description: m.description.to_string(),
            checksum: m.checksum.to_vec(),
            sql: m.sql.to_string(),
            no_tx: m.no_tx,
        })
        .collect();
    pending.sort_by_key(|m| m.version);
    Ok(pending)
}

/// The bookkeeping row of an applied migration.
fn record<'q>(
    namespace: &'q str,
    m: &'q Pending,
    started: std::time::Instant,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(
        "INSERT INTO program_indexer_migrations
           (namespace, version, description, checksum, execution_time_ms)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(namespace)
    .bind(m.version)
    .bind(&m.description)
    .bind(&m.checksum)
    .bind(started.elapsed().as_millis() as i64)
}

/// Highest applied migration per namespace.
pub async fn schema_versions(pool: &PgPool) -> anyhow::Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT namespace, max(version) FROM program_indexer_migrations GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(pool)
    .await?)
}
