//! The contract between the runtime and one program's indexing code.

use crate::db::{PgPool, PgTransaction};
use crate::event::IndexerEvent;
use crate::explorer::ExplorerClient;
use crate::export::{DayFacts, Holding};
use crate::input::IndexedTx;
use crate::prices::PriceCtx;
use crate::runner::Runner;
use axum::Router;
use chrono::{DateTime, Utc};
use sqlx::migrate::Migrator;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the ingest-side hooks (`prepare`, `sample`, `on_activity`,
/// `priced_mints`, `derive_prices`, `enrich`, `background`) work with: the
/// unbounded ingest pool and the indexer client.
#[derive(Clone)]
pub struct Ctx {
    pub pool: PgPool,
    pub client: ExplorerClient,
}

/// What route handlers work with: the serving pool (15 s statement timeout,
/// 5 s acquire timeout) and the indexer client.
#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub client: ExplorerClient,
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

    /// The earliest instant this indexer holds activity for (its first
    /// event's `ts`), `None` while it holds none. The minimum across a
    /// host's indexers anchors a price feed's cold start: the runtime fetches
    /// candles from a day before it.
    fn earliest_activity(
        &self,
        pool: &PgPool,
    ) -> impl Future<Output = anyhow::Result<Option<DateTime<Utc>>>> + Send {
        let _ = pool;
        async { Ok(None) }
    }

    /// The mints this indexer's [`Self::derive_prices`] prices (e.g. every
    /// vault share mint), lowercase hex. Every other indexer receives them in
    /// [`PriceCtx::claimed_mints`] and leaves them alone, so no indexer reads
    /// another's tables to learn them. Asked at the start of every enrich
    /// pass.
    fn priced_mints(&self, ctx: &Ctx) -> impl Future<Output = anyhow::Result<Vec<String>>> + Send {
        let _ = ctx;
        async { Ok(Vec::new()) }
    }

    /// Write `price_candles` for mints this indexer prices from its own state
    /// (a pool route, a vault's NAV), never one in `prices.feed_mints` or
    /// `prices.claimed_mints`. Runs every enrich pass after the runtime's
    /// feed sync, in registration order, before any indexer's `enrich`. An
    /// error is logged; the other indexers still derive.
    fn derive_prices(
        &self,
        ctx: &Ctx,
        prices: &PriceCtx,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = (ctx, prices);
        async { Ok(()) }
    }

    /// Derivation over rows already stored (flows, rates, USD stamps from
    /// `price_candles`, any column filled after ingest), every
    /// `ENRICH_INTERVAL_SECS` after every indexer's `derive_prices`, and once
    /// at the end of a `backfill`, so a fresh backfill is immediately
    /// queryable. An error is logged; the next tick runs regardless.
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

    /// Append Prometheus text-format lines to `/metrics`. The runtime groups
    /// the whole scrape by metric family and keeps one `# HELP`/`# TYPE` per
    /// family, so two indexers may each declare a family they share.
    fn metrics(
        &self,
        pool: &PgPool,
        out: &mut String,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = (pool, out);
        async { Ok(()) }
    }

    /// Why the day ending at `end` is not yet complete in this indexer's
    /// data beyond the runtime's checks (e.g. a vault share ledger not
    /// reconciled past `end`), or `None`. Reads through `tx`, the export's
    /// one snapshot ([`crate::export`]).
    fn day_incomplete(
        &self,
        tx: &mut PgTransaction<'_>,
        end: DateTime<Utc>,
    ) -> impl Future<Output = anyhow::Result<Option<String>>> + Send {
        let _ = (tx, end);
        async { Ok(None) }
    }

    /// This indexer's facts of `[start, end)` for a day export, by top-level
    /// key, each query limited to `cap` rows. Reads through `tx`.
    fn day_facts(
        &self,
        tx: &mut PgTransaction<'_>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> impl Future<Output = anyhow::Result<DayFacts>> + Send {
        let _ = (tx, start, end, cap);
        async { Ok(DayFacts::default()) }
    }

    /// What each wallet held in this indexer's venues over `[start, end)`,
    /// rows with `close` and `average` both 0 left out, the first `cap` in
    /// `(wallet, venue, address, side)` order. Reads through `tx`.
    fn day_holdings(
        &self,
        tx: &mut PgTransaction<'_>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> impl Future<Output = anyhow::Result<Vec<Holding>>> + Send {
        let _ = (tx, start, end, cap);
        async { Ok(Vec::new()) }
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
    fn earliest_activity<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, anyhow::Result<Option<DateTime<Utc>>>>;
    fn priced_mints<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<Vec<String>>>;
    fn derive_prices<'a>(
        &'a self,
        ctx: &'a Ctx,
        prices: &'a PriceCtx,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
    fn enrich<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<()>>;
    fn background(&self, ctx: &Ctx) -> Vec<BackgroundTask>;
    fn routes(&self, state: &ApiState) -> Routes;
    fn metrics<'a>(
        &'a self,
        pool: &'a PgPool,
        out: &'a mut String,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
    fn day_incomplete<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        end: DateTime<Utc>,
    ) -> BoxFuture<'a, anyhow::Result<Option<String>>>;
    fn day_facts<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> BoxFuture<'a, anyhow::Result<DayFacts>>;
    fn day_holdings<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> BoxFuture<'a, anyhow::Result<Vec<Holding>>>;
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
    fn earliest_activity<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, anyhow::Result<Option<DateTime<Utc>>>> {
        Box::pin(ProgramIndexer::earliest_activity(self, pool))
    }
    fn priced_mints<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, anyhow::Result<Vec<String>>> {
        Box::pin(ProgramIndexer::priced_mints(self, ctx))
    }
    fn derive_prices<'a>(
        &'a self,
        ctx: &'a Ctx,
        prices: &'a PriceCtx,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(ProgramIndexer::derive_prices(self, ctx, prices))
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
    fn day_incomplete<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        end: DateTime<Utc>,
    ) -> BoxFuture<'a, anyhow::Result<Option<String>>> {
        Box::pin(ProgramIndexer::day_incomplete(self, tx, end))
    }
    fn day_facts<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> BoxFuture<'a, anyhow::Result<DayFacts>> {
        Box::pin(ProgramIndexer::day_facts(self, tx, start, end, cap))
    }
    fn day_holdings<'a>(
        &'a self,
        tx: &'a mut PgTransaction<'static>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        cap: i64,
    ) -> BoxFuture<'a, anyhow::Result<Vec<Holding>>> {
        Box::pin(ProgramIndexer::day_holdings(self, tx, start, end, cap))
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
