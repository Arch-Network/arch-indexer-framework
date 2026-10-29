# arch-indexer-framework

A framework for Arch program indexers. You write the program-specific part (decoder, tables, sampling, routes) as a `ProgramIndexer`. The framework runs it against [arch-rust-indexer](https://github.com/Arch-Network/arch-rust-indexer)'s program export and handles:

- paging, checkpoints, confirmation gating and replays
- per-program Postgres migrations
- sampler and enricher loops
- an HTTP API with health, version, sync status, response cache, concurrency gates and Prometheus metrics

## Where it sits

```
arch-rust-indexer            data provider: indexes the chain and serves each program's
(Arch-Network, service)      transactions over REST (the program export) and JSON-RPC
        │
        ▼
arch-indexer (this repo)     framework for processors: pages the export, owns cursors,
                             replays, migrations, host process and HTTP scaffolding
        │
        ▼
program repos                processors: one ProgramIndexer per program (CLAMM,
(clamm, vaults, lend, …)     arch-vaults, arch-lend) plus a thin service binary
```

The data provider is the only datasource today. A second one is expected: block reports or a Kafka feed consumed in-process, for example inside arch-rust-indexer itself. That is why decoders depend only on `arch-indexer-core`. A datasource trait will be added when the second datasource exists.

## Crates

| Crate | For | Depends on |
|---|---|---|
| [`arch-indexer-core`](crates/arch-indexer-core) | Decoders. Provides the transaction shape the export carries (`IndexedTx`, `InstructionNode`, `TokenTransfer`, `TxStatus`), log attribution with the node's 255-byte line rule (`ProgramLogs`, `ProgramData`, `ProgramMessage`, `LOG_LINE_LIMIT`), `ByteReader`, strict `base64_decode`, `anchor_discriminator` / `event_discriminator`, and the event contract (`IndexerEvent`, `UnknownInstruction`). | `anyhow`, `bs58`, `chrono`, `hex`, `serde`, `sha2`, `tracing`. No database, HTTP or async runtime. |
| [`arch-indexer`](crates/arch-indexer) | Services. Provides the `ProgramIndexer` trait and its object-safe form `DynIndexer`, the runner, the arch-rust-indexer client (`explorer::ExplorerClient`), checkpoints, scan marks and replays, namespaced migrations, `run` / `Host` / `Mode`, HTTP scaffolding (`http::{chart, paging, cache}`), `twa`, and metrics. It re-exports the `arch-indexer-core` items its signatures use. | `arch-indexer-core`, `sqlx` 0.8 (Postgres), `axum` 0.8, `tokio`, `reqwest` |

A program repo usually splits the same way:
- a decoder crate on `arch-indexer-core`: decoder, event types and account math
- an indexer crate on `arch-indexer`: `persist`, migrations, samplers, routes and the binary

## Install

```toml
[dependencies]
arch-indexer = "0.1"
# A decoder-only crate needs just:
# arch-indexer-core = "0.1"

# For `sqlx::migrate!` and your own queries; same 0.8 line, so Cargo unifies it.
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "tls-rustls-ring-native-roots", "postgres", "chrono", "bigdecimal", "migrate", "macros", "json"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
anyhow = "1"
```

Before a release is on crates.io, pin a commit instead (public repo, so no token is needed):

```toml
arch-indexer = { git = "https://github.com/Arch-Network/arch-indexer-framework", rev = "<commit>" }
```

`arch_indexer::{axum, sqlx}` re-export the exact versions the trait signatures use.

## The trait

```rust
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
```

`DynIndexer` is the object-safe form. Every `ProgramIndexer` gets it from a blanket impl, so a host takes `Vec<Box<dyn DynIndexer>>` and can run indexers with different event types side by side.

The values the hooks receive:

```rust
/// What the ingest-side hooks (`prepare`, `sample`, `on_activity`,
/// `background`) work with: the unbounded ingest pool and the indexer client.
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
```

### Events (`arch-indexer-core`)

```rust
/// What an `arch_indexer::ProgramIndexer`'s decoder emits. A program defines one
/// event type (usually an enum over its rows) and implements this for it.
pub trait IndexerEvent: Send + Sync + 'static {
    /// Label the runner counts the event under in its per-pass log line
    /// (`by_kind`), e.g. `"swap"` or `"lending_deposit"`.
    fn kind(&self) -> &'static str;

    /// The marker inside the event, when it is one. The runner writes markers
    /// to `decoder_unknowns` itself and never hands them to `persist`.
    fn as_unknown(&self) -> Option<&UnknownInstruction> {
        None
    }
}

/// An instruction or event a decoder recognized as its program's but had no
/// decode path for. Dropping it silently would hide a program upgrade that
/// outran the decoder, so the decoder emits this marker and the runner
/// persists it to `decoder_unknowns`, in the page's transaction.
#[derive(Debug, Clone)]
pub struct UnknownInstruction {
    /// The emitting indexer's `arch_indexer::ProgramIndexer::id`; `/metrics` and
    /// the per-host views select markers by it.
    pub decoder: &'static str,
    /// What was not understood, in the decoder's own vocabulary (a hex
    /// discriminator, `no-event:<ix>`, `event:<disc>:v<n>`, …).
    pub discriminator: String,
    pub txid: String,
    pub ix_path: String,
    pub block_height: i64,
    pub ts: DateTime<Utc>,
}
```

- Each program defines one event enum.
- Undecodable instructions become an `Unknown(UnknownInstruction)` variant, returned by `as_unknown()`.
- The runner writes those markers to `decoder_unknowns` in the page's transaction, and removes them before calling `persist`.
- Set `UnknownInstruction::decoder` to your `id()`: `/metrics` selects markers by it.

## Running

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    arch_indexer::run(
        arch_indexer::build_info!(),
        arch_indexer::Config::from_env()?,
        vec![Box::new(MyIndexer::from_env()?)],
    )
    .await
}
```

`run` initializes tracing from `RUST_LOG` (default `info`), validates the indexers, applies migrations and runs the mode named by the first argument:

| Mode | Runs |
|---|---|
| `all` (default) | `prepare`, then in one process: the tail (`on_activity` after each pass), the sampler, the enricher, `background` tasks and the HTTP API |
| `api` | HTTP API only, on the serving pool (15 s statement timeout) |
| `ingest` | tail, sampler, enricher and background tasks; no HTTP |
| `backfill` | `prepare`, one catch-up pass to the tip (or `INGEST_END_HEIGHT`), one `enrich` pass, then exit |
| `delete-tx <txid>` | every indexer's `delete_tx` plus the txid's `decoder_unknowns` rows, in one database transaction; for a transaction rolled back after it was ingested |

How it stops:
- If any long-running loop exits, `run` returns an error, so the platform restarts the process. This is safe because ingest is idempotent and checkpointed.
- SIGTERM or Ctrl-C drains in-flight HTTP requests, then returns `Ok`.

`build_info!()` captures the **binary's** package name and version, plus the commit from `RAILWAY_GIT_COMMIT_SHA` or `GIT_SHA`. `GIT_SHA` can come from the build environment, or from your build script via `cargo:rustc-env=GIT_SHA=…`. The result is served as the `x-indexer-version` header on every response and returned by `/v1/version`.

`Host::new(build, config, indexers)` exposes the parts for tests and custom wiring:
- `.migrate(&pool)`, `.prepare(&ctx)`, `.runner(pool)`, `.router(serving_pool)`, `.run(&self, Mode)`
- it refuses ids that are malformed, duplicated or `runtime`, program ids that are not 64 lowercase hex, and duplicate checkpoint keys

### HTTP

The runtime serves:
- `/healthz` and `/readyz` (the latter checks the database)
- `/v1/version`: build, network, schema version per migration namespace, and data revision per indexer
- `/v1/sync-status`: checkpoints, scan-mark tips and standing replays
- `/metrics`: Prometheus text, i.e. the `program_indexer_*` service gauges followed by each indexer's `metrics` lines

Each indexer's `Routes` are mounted by group:

| Group | Cache | Gate |
|---|---|---|
| `cached` | shared in-process response cache (`CACHE_TTL_SECS`), `public, s-maxage=30, stale-while-revalidate=120` | `API_QUERY_CONCURRENCY` |
| `per_address` | `private, max-age=10` | `API_PORTFOLIO_CONCURRENCY` |
| `no_store` | `no-store` | 1 permit |

Gates are shared across all indexers, so the connection budget holds however many are mounted. All routes get permissive CORS.

Helpers for route handlers:
- `http::{internal, normalize_address, ApiResult}`
- `http::chart`: bucketed windows, `Point`, `chart_response`, `merge_series`, `series_window`
- `http::paging`: `EventsParams`, `page_of`, `EventCursor`
- `twa::twa_buckets`

## Environment

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | Postgres. For a schema, append `?options=-c%20search_path%3D<schema>`. |
| `DB_MAX_CONNECTIONS` | 10 | Size of each pool (ingest and serving). |
| `INDEXER_BASE_URL` | required | arch-rust-indexer origin; REST root is `{base}/api/v1[/{network}]`. |
| `INDEXER_API_KEY` | – | Sent as `X-API-Key`. Redirects are never followed, so the key cannot leak to another host. |
| `INDEXER_NETWORK` | – (`default`) | Network path segment (alphanumeric or hyphen). |
| `ARCH_RPC_URL` | – | Chain RPC used as a fallback by `ExplorerClient::read_account_data` and `program_accounts_by_prefix`. |
| `TAIL_INTERVAL_SECS` | 5 | Tail cadence; `on_activity` runs after each pass. |
| `SAMPLE_INTERVAL_SECS` | 300 | `sample` cadence. |
| `ENRICH_INTERVAL_SECS` | 60 | `enrich` cadence. |
| `EXPORT_PAGE_LIMIT` | 1000 | Export page size. |
| `INGEST_CONFIRMATION_DEPTH` | 0 | Ingest only up to `tip - N`. |
| `INGEST_END_HEIGHT` | – | Bounded run; the checkpoint stops there. |
| `API_BIND_ADDR` / `PORT` | `[::]:8080` | HTTP bind address. |
| `CACHE_TTL_SECS` | 30 | Response cache TTL; 0 disables it. |
| `API_QUERY_CONCURRENCY` | 3 | Permits of the `cached` group. |
| `API_PORTFOLIO_CONCURRENCY` | 2 | Permits of the `per_address` group. |
| `RUST_LOG` | `info` | Tracing filter. |

Each indexer reads its own program ids and other settings in its constructor. `arch_indexer::config::{optional_program_id, required_program_id}` normalize a value to 64 lowercase hex and refuse anything else.

## Migrations and namespaces

- **Where they live:** put the program's schema in your crate's `migrations/` as a normal sqlx set numbered from `0001`, and return it from `migrator()`: `static MIGRATOR: Migrator = sqlx::migrate!("./migrations");`.
- **How they are recorded:** each set is applied under namespace = `id()` and recorded in `program_indexer_migrations (namespace, version, …)`, never in sqlx's `_sqlx_migrations`. Several indexers can therefore share one database even though all of them number from 1.
- **Order:** the runtime's own namespace `runtime` runs first. It holds `checkpoints`, `scan_marks`, `replay_requests`, `mint_decimals` and `decoder_unknowns`. Each indexer follows, in registration order.
- **Transactions and locking:** each migration runs in its own transaction unless the file is marked `-- no-transaction`. A per-schema advisory lock serializes concurrent boots.
- **Refusals:** a migration edited after it was applied stops the boot. So does an applied version missing from the set, unless the `Migrator` sets `ignore_missing`. Never edit a merged migration; add a new one.
- **Table names are global:** never create a table the runtime owns, and keep each program's table names distinct.
- **Adopting an existing database:** write a baseline with `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`, as the runtime's own migration does.

## Replays and data revisions

`data_revision()` is the revision of the numbers a build produces. To re-derive history after a decoder fix, a migration requests a replay of one of your checkpoint keys:

```sql
INSERT INTO replay_requests (checkpoint, data_revision) VALUES ('<checkpoint_key>', <rev>)
ON CONFLICT (checkpoint) DO UPDATE
  SET data_revision = GREATEST(replay_requests.data_revision, EXCLUDED.data_revision),
      through_height = 0;
```

In the same change, bump `data_revision()` to at least `<rev>`. Then:
- **Older builds** (lower `data_revision()`) ignore the request. A previous build still running through a deploy never walks history with the old decoder.
- **Qualifying builds** walk the cursor from genesis and keep progress in `through_height`, leaving the max-merged checkpoint alone. They delete the request once a pass reaches the tip.

## Checkpoint keys

- **Default:** the key is `id()`, one cursor per indexer.
- **Several program ids:** override `checkpoint_key(program_id)`, for example `format!("vaults:{program_id}")`.
- **Uniqueness:** keys must be unique across a host.
- **`decode` input:** it receives the program id whose page is being decoded.

## Minimal implementor

This example compiles against this release; `crates/arch-indexer/tests/common/mod.rs` has a fuller one that exercises every hook.

```rust
use arch_indexer::axum::{extract::State, routing::get, Json, Router};
use arch_indexer::http::{internal, ApiResult};
use arch_indexer::sqlx::migrate::Migrator;
use arch_indexer::{
    ApiState, ByteReader, Ctx, IndexedTx, IndexerEvent, PgPool, PgTransaction, ProgramIndexer,
    Routes, UnknownInstruction,
};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub enum MyEvent {
    Transfer {
        txid: String,
        ix_path: String,
        amount: i64,
        ts: chrono::DateTime<chrono::Utc>,
    },
    Unknown(UnknownInstruction),
}

impl IndexerEvent for MyEvent {
    fn kind(&self) -> &'static str {
        match self {
            Self::Transfer { .. } => "transfer",
            Self::Unknown(_) => "unknown",
        }
    }
    fn as_unknown(&self) -> Option<&UnknownInstruction> {
        match self {
            Self::Unknown(u) => Some(u),
            _ => None,
        }
    }
}

pub struct MyIndexer {
    program_id: String,
}

impl MyIndexer {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            program_id: arch_indexer::config::required_program_id("MY_PROGRAM_ID")?,
        })
    }
}

impl ProgramIndexer for MyIndexer {
    type Event = MyEvent;

    fn id(&self) -> &'static str {
        "my_program"
    }

    fn program_ids(&self) -> Vec<String> {
        vec![self.program_id.clone()]
    }

    fn decode(&self, program_id: &str, tx: &IndexedTx) -> anyhow::Result<Vec<MyEvent>> {
        let mut out = Vec::new();
        for ix in tx.walk().into_iter().filter(|ix| ix.program_id == program_id) {
            let data = ix.data();
            let mut r = ByteReader::new(&data);
            match r.u8()? {
                1 => out.push(MyEvent::Transfer {
                    txid: tx.txid.clone(),
                    ix_path: ix.ix_path.clone(),
                    amount: r.u64()? as i64,
                    ts: tx.ts,
                }),
                tag => out.push(MyEvent::Unknown(UnknownInstruction {
                    decoder: "my_program",
                    discriminator: format!("{tag:02x}"),
                    txid: tx.txid.clone(),
                    ix_path: ix.ix_path.clone(),
                    block_height: tx.block_height,
                    ts: tx.ts,
                })),
            }
        }
        Ok(out)
    }

    async fn persist(&self, tx: &mut PgTransaction<'_>, events: &[MyEvent]) -> anyhow::Result<u64> {
        let mut written = 0;
        for ev in events {
            if let MyEvent::Transfer { txid, ix_path, amount, ts } = ev {
                written += sqlx::query(
                    "INSERT INTO my_transfers (txid, ix_path, amount, ts)
                     VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                )
                .bind(txid)
                .bind(ix_path)
                .bind(amount)
                .bind(ts)
                .execute(&mut **tx)
                .await?
                .rows_affected();
            }
        }
        Ok(written)
    }

    async fn delete_tx(&self, tx: &mut PgTransaction<'_>, txid: &str) -> anyhow::Result<u64> {
        Ok(sqlx::query("DELETE FROM my_transfers WHERE txid = $1")
            .bind(txid)
            .execute(&mut **tx)
            .await?
            .rows_affected())
    }

    fn migrator(&self) -> &'static Migrator {
        &MIGRATOR
    }

    fn routes(&self, state: &ApiState) -> Routes {
        Routes {
            cached: Router::new()
                .route("/v1/my/transfers", get(transfers))
                .with_state(state.clone()),
            ..Default::default()
        }
    }

    async fn sample(&self, ctx: &Ctx) -> anyhow::Result<()> {
        // Read accounts through `ctx.client`, write state points through `ctx.pool`.
        let _ = (&ctx.pool, &ctx.client);
        Ok(())
    }

    async fn metrics(&self, pool: &PgPool, out: &mut String) -> anyhow::Result<()> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM my_transfers")
            .fetch_one(pool)
            .await?;
        out.push_str(&format!("my_transfers_total {n}\n"));
        Ok(())
    }
}

async fn transfers(State(state): State<ApiState>) -> ApiResult {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM my_transfers")
        .fetch_one(&state.pool)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "count": n })))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    arch_indexer::run(
        arch_indexer::build_info!(),
        arch_indexer::Config::from_env()?,
        vec![Box::new(MyIndexer::from_env()?)],
    )
    .await
}
```

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
docker run -d --rm --name arch-indexer-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=arch_indexer_test -p 5432:5432 postgres:16
ARCH_INDEXER_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/arch_indexer_test \
  cargo test --workspace -- --include-ignored
```

The Postgres-backed tests are `#[ignore]` so a bare `cargo test` passes without a database. Each one runs in its own schema.

## Releasing

1. Bump `version` in the root `Cargo.toml` (`[workspace.package]`) and `arch-indexer-core`'s version in `[workspace.dependencies]`, in a PR.
2. Merge it. `.github/workflows/publish.yml` publishes each crate whose version crates.io lacks (`arch-indexer-core` first, then `arch-indexer`) using the `CRATES_IO_TOKEN` secret. It then creates the `arch-indexer-core-v<version>` and `arch-indexer-v<version>` GitHub releases.

## License

MIT
