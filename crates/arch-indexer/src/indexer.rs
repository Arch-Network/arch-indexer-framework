//! The contract between the runtime and one program's indexing code.

use crate::db::{PgPool, PgTransaction};
use crate::event::IndexerEvent;
use crate::indexer_client::IndexerClient;
use crate::input::IndexedTx;
use crate::runner::Runner;
use axum::Router;
use sqlx::migrate::Migrator;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the ingest-side hooks (`prepare`, `sample`, `on_activity`,
/// `background`) work with: the unbounded ingest pool and the indexer client.
#[derive(Clone)]
pub struct Ctx {
    pub pool: PgPool,
    pub client: IndexerClient,
}

/// What route handlers work with: the serving pool (15 s statement timeout,
/// 5 s acquire timeout) and the indexer client.
#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub client: IndexerClient,
}

/// An indexer's routes, by load domain. Each group is mounted behind the
/// host's layers for it; the host shares one gate per group across every
/// indexer it runs, so the connection budget holds however many it mounts.
/// Routers carry their state already (`.with_state(...)`).
#[derive(Default)]
pub struct Routes {
    /// Aggregate reads identical for every caller: the shared response cache
    /// (`CACHE_TTL_SECS`), the `API_QUERY_CONCURRENCY` gate and
    /// `cache-control: public, s-maxage=30, stale-while-revalidate=120`.
    pub cached: Router,
    /// Per-address reads: no shared cache, the `API_PORTFOLIO_CONCURRENCY`
    /// gate and `cache-control: private, max-age=10`.
    pub per_address: Router,
    /// Long or operator reads: no cache, one permit, `cache-control: no-store`.
    pub no_store: Router,
}

/// A long-running loop an indexer adds to the ingest process (e.g. a feed
/// poller). It should never return: the host stops the process when any
/// background task ends, so the platform restarts it.
pub struct BackgroundTask {
    pub name: &'static str,
    pub future: BoxFuture<'static, anyhow::Result<()>>,
}

impl BackgroundTask {
    pub fn new(
        name: &'static str,
        future: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> Self {
        Self {
            name,
            future: Box::pin(future),
        }
    }
}

/// One program's indexing: decoder, persistence, schema, sampling, routes and
/// metrics. The runtime owns paging, checkpoints, replays, scan marks, mint
/// decimals, unknown-instruction markers and the HTTP scaffolding around it.
///
/// Implementors may write every `-> impl Future` method as `async fn`; the
/// future must be `Send`.
pub trait ProgramIndexer: Send + Sync + 'static {
    type Event: IndexerEvent;

    /// Stable identifier, `[a-z][a-z0-9_]*`, never `runtime`: the migration
    /// namespace, the default checkpoint key and the label of the indexer's
    /// `decoder_unknowns` rows.
    fn id(&self) -> &'static str;

    /// Lowercase 64-hex program ids whose export this indexer decodes. Each
    /// one is paged on its own cursor ([`Self::checkpoint_key`]).
    fn program_ids(&self) -> Vec<String>;

    /// Cursor for one program id. Keys must be unique across every indexer a
    /// host runs; the default suits an indexer with one program id.
    fn checkpoint_key(&self, program_id: &str) -> String {
        let _ = program_id;
        self.id().to_string()
    }

    /// Whether the program's events live in log lines (`emit!` →
    /// `Program data:`). The runner then fetches a transaction's logs when the
    /// export carried none.
    fn needs_logs(&self) -> bool {
        false
    }

    /// The revision of the numbers this build produces. A `replay_requests`
    /// row for one of this indexer's checkpoint keys is acted on only by a
    /// build whose revision is at least the row's.
    fn data_revision(&self) -> u32 {
        1
    }

    /// Pure: one successful, unrolled-back transaction touching `program_id`
    /// to events. Never touches the database or the network. An error skips
    /// the transaction (logged) without stalling the page.
    fn decode(&self, program_id: &str, tx: &IndexedTx) -> anyhow::Result<Vec<Self::Event>>;

    /// Write one page's events (unknown markers already removed) inside the
    /// page's transaction, idempotently: a crash between commit and checkpoint
    /// re-ingests the page. Returns rows written.
    fn persist(
        &self,
        tx: &mut PgTransaction<'_>,
        events: &[Self::Event],
    ) -> impl Future<Output = anyhow::Result<u64>> + Send;

    /// Remove every row this indexer derived from `txid` (a transaction
    /// rolled back after it was ingested). Returns rows deleted.
    fn delete_tx(
        &self,
        tx: &mut PgTransaction<'_>,
        txid: &str,
    ) -> impl Future<Output = anyhow::Result<u64>> + Send;

    /// This indexer's schema, applied under the namespace [`Self::id`] after
    /// the runtime's own migrations. Typically
    /// `static MIGRATOR: Migrator = sqlx::migrate!("./migrations");`.
    fn migrator(&self) -> &'static Migrator;

    /// Runs once at boot of an ingesting process, after migrations and before
    /// the first pass (e.g. carrying a legacy cursor forward).
    fn prepare(&self, ctx: &Ctx) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = ctx;
        async { Ok(()) }
    }

    /// Periodic state sampling, every `SAMPLE_INTERVAL_SECS`. An error is
    /// logged; the next tick runs regardless.
    fn sample(&self, ctx: &Ctx) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = ctx;
        async { Ok(()) }
    }

    /// Runs after every tail pass, failed ones included (the indexers that did
    /// not fail still wrote): event-triggered sampling. Returns items
    /// refreshed, for the log line.
    fn on_activity(&self, ctx: &Ctx) -> impl Future<Output = anyhow::Result<u64>> + Send {
        let _ = ctx;
        async { Ok(0) }
    }

    /// Derivation over rows already stored (flows, rates, any column filled
    /// after ingest), every `ENRICH_INTERVAL_SECS` and once at the end of a
    /// `backfill`, so a fresh backfill is immediately queryable. An error is
    /// logged; the next tick runs regardless.
    fn enrich(&self, ctx: &Ctx) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = ctx;
        async { Ok(()) }
    }

    /// Extra long-running loops of the ingest process.
    fn background(&self, ctx: &Ctx) -> Vec<BackgroundTask> {
        let _ = ctx;
        Vec::new()
    }

    /// HTTP routes, mounted by the API process.
    fn routes(&self, state: &ApiState) -> Routes {
        let _ = state;
        Routes::default()
    }

    /// Append Prometheus text-format lines to `/metrics`.
    fn metrics(
        &self,
        pool: &PgPool,
        out: &mut String,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = (pool, out);
        async { Ok(()) }
    }
}

/// Object-safe form of [`ProgramIndexer`], so one host runs indexers with
/// different event types. Every `ProgramIndexer` is one; a host takes
/// `Box<dyn DynIndexer>`.
pub trait DynIndexer: Send + Sync + 'static {
    fn id(&self) -> &'static str;
    fn program_ids(&self) -> Vec<String>;
    fn checkpoint_key(&self, program_id: &str) -> String;
    fn data_revision(&self) -> u32;
    fn migrator(&self) -> &'static Migrator;
    /// One catch-up pass of one program's cursor; see [`Runner::catch_up`].
    fn catch_up_program<'a>(
        &'a self,
        runner: &'a Runner,
        program_id: &'a str,
        safe_tip: Option<i64>,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;
    fn delete_tx<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        txid: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;
    fn prepare<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>>;
    fn sample<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>>;
    fn on_activity<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<u64>>;
    fn enrich<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>>;
    fn background(&self, ctx: &Ctx) -> Vec<BackgroundTask>;
    fn routes(&self, state: &ApiState) -> Routes;
    fn metrics<'a>(
        &'a self,
        pool: &'a PgPool,
        out: &'a mut String,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
}

impl<T: ProgramIndexer> DynIndexer for T {
    fn id(&self) -> &'static str {
        ProgramIndexer::id(self)
    }
    fn program_ids(&self) -> Vec<String> {
        ProgramIndexer::program_ids(self)
    }
    fn checkpoint_key(&self, program_id: &str) -> String {
        ProgramIndexer::checkpoint_key(self, program_id)
    }
    fn data_revision(&self) -> u32 {
        ProgramIndexer::data_revision(self)
    }
    fn migrator(&self) -> &'static Migrator {
        ProgramIndexer::migrator(self)
    }
    fn catch_up_program<'a>(
        &'a self,
        runner: &'a Runner,
        program_id: &'a str,
        safe_tip: Option<i64>,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(runner.catch_up_typed(self, program_id, safe_tip))
    }
    fn delete_tx<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        txid: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(ProgramIndexer::delete_tx(self, tx, txid))
    }
    fn prepare<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(ProgramIndexer::prepare(self, ctx))
    }
    fn sample<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(ProgramIndexer::sample(self, ctx))
    }
    fn on_activity<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(ProgramIndexer::on_activity(self, ctx))
    }
    fn enrich<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(ProgramIndexer::enrich(self, ctx))
    }
    fn background(&self, ctx: &Ctx) -> Vec<BackgroundTask> {
        ProgramIndexer::background(self, ctx)
    }
    fn routes(&self, state: &ApiState) -> Routes {
        ProgramIndexer::routes(self, state)
    }
    fn metrics<'a>(
        &'a self,
        pool: &'a PgPool,
        out: &'a mut String,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(ProgramIndexer::metrics(self, pool, out))
    }
}

/// Refuse a set of indexers one host cannot run side by side: a malformed or
/// reserved id, two indexers with one id (one migration namespace), a program
/// id that is not 64 lowercase hex, or two cursors with one checkpoint key.
pub fn validate(indexers: &[std::sync::Arc<dyn DynIndexer>]) -> anyhow::Result<()> {
    let mut ids = std::collections::HashSet::new();
    let mut keys = std::collections::HashSet::new();
    for ix in indexers {
        let id = ix.id();
        anyhow::ensure!(
            id.starts_with(|c: char| c.is_ascii_lowercase())
                && id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "indexer id {id:?} must match [a-z][a-z0-9_]*"
        );
        anyhow::ensure!(
            id != crate::db::RUNTIME_NAMESPACE,
            "indexer id {id:?} is reserved for the runtime's migrations"
        );
        anyhow::ensure!(ids.insert(id), "two indexers share the id {id:?}");
        for program in ix.program_ids() {
            anyhow::ensure!(
                program.len() == 64
                    && program
                        .chars()
                        .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
                "indexer {id}: program id {program:?} must be 64 lowercase hex"
            );
            let key = ix.checkpoint_key(&program);
            anyhow::ensure!(
                keys.insert(key.clone()),
                "indexer {id}: checkpoint key {key:?} is already taken"
            );
        }
    }
    Ok(())
}
