//! Shared harness for the Postgres-backed tests: an isolated schema per test,
//! an in-process fake of the arch-rust-indexer API, and `ExampleIndexer`, a
//! minimal `ProgramIndexer` exercising every hook.
//!
//! Run with PROGRAM_INDEXER_TEST_DATABASE_URL set to a disposable database:
//!   cargo test -- --ignored
#![allow(dead_code)]

use arch_program_indexer::axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use arch_program_indexer::http::{internal, normalize_address, ApiResult};
use arch_program_indexer::input::IndexedTx;
use arch_program_indexer::sqlx::migrate::Migrator;
use arch_program_indexer::{
    ApiState, BackgroundTask, ByteReader, Ctx, IndexerEvent, PgPool, PgTransaction, ProgramIndexer,
    Routes, UnknownInstruction,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DB_ENV: &str = "PROGRAM_INDEXER_TEST_DATABASE_URL";

/// A fresh schema on the test database, removed by [`TestDb::drop_schema`].
pub struct TestDb {
    admin: PgPool,
    pub pool: PgPool,
    /// The database URL with `search_path` set to the schema, as a host's
    /// `DATABASE_URL` would carry it.
    pub url: String,
    schema: String,
}

impl TestDb {
    pub async fn new(prefix: &str) -> Self {
        let base = std::env::var(DB_ENV).expect("isolated test DB required");
        let admin = arch_program_indexer::db::connect(&base).await.unwrap();
        let schema = format!("{prefix}_{}", std::process::id());
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let sep = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{sep}options=-c%20search_path%3D{schema}");
        let pool = arch_program_indexer::db::connect(&url).await.unwrap();
        Self {
            admin,
            pool,
            url,
            schema,
        }
    }

    /// Another pool on the schema, as the host's serving pool.
    pub async fn another_pool(&self) -> PgPool {
        arch_program_indexer::db::connect_serving(&self.url)
            .await
            .unwrap()
    }

    pub async fn drop_schema(self) {
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

/// The example program's instruction: tag 1 is a transfer with a u64 LE
/// amount; any other tag is undecodable.
pub const TRANSFER_TAG: u8 = 1;

static EXAMPLE_MIGRATOR: Migrator = sqlx::migrate!("tests/example_migrations");
static OTHER_MIGRATOR: Migrator = sqlx::migrate!("tests/other_migrations");

#[derive(Debug, Clone)]
pub struct Transfer {
    pub program_id: String,
    pub txid: String,
    pub ix_path: String,
    pub amount: i64,
    /// The `Program log:` line its frame wrote, from the transaction's logs.
    pub memo: Option<String>,
    pub block_height: i64,
    pub ts: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub enum ExampleEvent {
    Transfer(Transfer),
    Unknown(UnknownInstruction),
}

impl IndexerEvent for ExampleEvent {
    fn kind(&self) -> &'static str {
        match self {
            Self::Transfer(_) => "transfer",
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

/// What the hooks observed, for assertions.
#[derive(Default)]
pub struct Observed {
    pub samples: AtomicU64,
    pub activity: AtomicU64,
    pub prepared: AtomicU64,
    pub enriched: AtomicU64,
    pub persisted_batches: Mutex<Vec<usize>>,
}

pub struct ExampleIndexer {
    pub programs: Vec<String>,
    pub revision: u32,
    pub observed: Arc<Observed>,
}

impl ExampleIndexer {
    pub fn new(programs: Vec<String>) -> Self {
        Self {
            programs,
            revision: 1,
            observed: Arc::default(),
        }
    }
}

impl ProgramIndexer for ExampleIndexer {
    type Event = ExampleEvent;

    fn id(&self) -> &'static str {
        "example"
    }

    fn program_ids(&self) -> Vec<String> {
        self.programs.clone()
    }

    fn checkpoint_key(&self, program_id: &str) -> String {
        format!("example:{program_id}")
    }

    fn needs_logs(&self) -> bool {
        true
    }

    fn data_revision(&self) -> u32 {
        self.revision
    }

    fn decode(&self, program_id: &str, tx: &IndexedTx) -> anyhow::Result<Vec<ExampleEvent>> {
        let logs = tx.program_logs();
        let mut out = Vec::new();
        for ix in tx
            .walk()
            .into_iter()
            .filter(|ix| ix.program_id == program_id)
        {
            let data = ix.data();
            let mut r = ByteReader::new(&data);
            match r.u8()? {
                TRANSFER_TAG => {
                    let memo = logs
                        .messages
                        .iter()
                        .find(|m| m.ix_path == ix.ix_path)
                        .map(|m| m.message.clone());
                    out.push(ExampleEvent::Transfer(Transfer {
                        program_id: program_id.to_string(),
                        txid: tx.txid.clone(),
                        ix_path: ix.ix_path.clone(),
                        amount: r.u64()? as i64,
                        memo,
                        block_height: tx.block_height,
                        ts: tx.ts,
                    }));
                }
                tag => out.push(ExampleEvent::Unknown(UnknownInstruction {
                    decoder: "example",
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

    async fn persist(
        &self,
        tx: &mut PgTransaction<'_>,
        events: &[ExampleEvent],
    ) -> anyhow::Result<u64> {
        self.observed
            .persisted_batches
            .lock()
            .unwrap()
            .push(events.len());
        let mut written = 0;
        for ev in events {
            let ExampleEvent::Transfer(t) = ev else {
                anyhow::bail!("persist was handed an unknown marker");
            };
            written += sqlx::query(
                "INSERT INTO example_transfers
                   (txid, ix_path, program_id, amount, memo, block_height, ts)
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
            )
            .bind(&t.txid)
            .bind(&t.ix_path)
            .bind(&t.program_id)
            .bind(t.amount)
            .bind(&t.memo)
            .bind(t.block_height)
            .bind(t.ts)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        }
        Ok(written)
    }

    async fn delete_tx(&self, tx: &mut PgTransaction<'_>, txid: &str) -> anyhow::Result<u64> {
        Ok(sqlx::query("DELETE FROM example_transfers WHERE txid = $1")
            .bind(txid)
            .execute(&mut **tx)
            .await?
            .rows_affected())
    }

    fn migrator(&self) -> &'static Migrator {
        &EXAMPLE_MIGRATOR
    }

    async fn prepare(&self, _ctx: &Ctx) -> anyhow::Result<()> {
        self.observed.prepared.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn sample(&self, ctx: &Ctx) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO example_samples (total) SELECT coalesce(sum(amount), 0) FROM example_transfers")
            .execute(&ctx.pool)
            .await?;
        self.observed.samples.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn enrich(&self, _ctx: &Ctx) -> anyhow::Result<()> {
        self.observed.enriched.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn on_activity(&self, _ctx: &Ctx) -> anyhow::Result<u64> {
        Ok(self.observed.activity.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn background(&self, _ctx: &Ctx) -> Vec<BackgroundTask> {
        vec![BackgroundTask::new("example_feed", std::future::pending())]
    }

    fn routes(&self, state: &ApiState) -> Routes {
        Routes {
            cached: Router::new()
                .route("/v1/example/transfers", get(transfers))
                .with_state(state.clone()),
            per_address: Router::new()
                .route("/v1/example/accounts/{addr}", get(account))
                .with_state(state.clone()),
            no_store: Router::new()
                .route("/v1/example/admin", get(|| async { "admin" }))
                .with_state(state.clone()),
        }
    }

    async fn metrics(&self, pool: &PgPool, out: &mut String) -> anyhow::Result<()> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM example_transfers")
            .fetch_one(pool)
            .await?;
        out.push_str("# TYPE example_transfers_total counter\n");
        out.push_str(&format!("example_transfers_total {n}\n"));
        Ok(())
    }
}

async fn transfers(State(state): State<ApiState>) -> ApiResult {
    let (count, total): (i64, Option<i64>) =
        sqlx::query_as("SELECT count(*), sum(amount)::bigint FROM example_transfers")
            .fetch_one(&state.pool)
            .await
            .map_err(internal)?;
    Ok(Json(json!({ "count": count, "total": total })))
}

async fn account(Path(addr): Path<String>) -> ApiResult {
    let addr = normalize_address(&addr).ok_or((
        arch_program_indexer::axum::http::StatusCode::BAD_REQUEST,
        "addr must be a 32-byte pubkey".to_string(),
    ))?;
    Ok(Json(json!({ "addr": addr })))
}

/// A second indexer with its own migration set, numbered from 1 like the
/// example's, to prove the namespaces keep them apart.
pub struct OtherIndexer;

#[derive(Debug)]
pub struct Never;

impl IndexerEvent for Never {
    fn kind(&self) -> &'static str {
        "never"
    }
}

impl ProgramIndexer for OtherIndexer {
    type Event = Never;
    fn id(&self) -> &'static str {
        "other"
    }
    fn program_ids(&self) -> Vec<String> {
        vec![]
    }
    fn decode(&self, _: &str, _: &IndexedTx) -> anyhow::Result<Vec<Never>> {
        Ok(vec![])
    }
    async fn persist(&self, _: &mut PgTransaction<'_>, _: &[Never]) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn delete_tx(&self, _: &mut PgTransaction<'_>, _: &str) -> anyhow::Result<u64> {
        Ok(0)
    }
    fn migrator(&self) -> &'static Migrator {
        &OTHER_MIGRATOR
    }
}

/// A 32-byte program id as the hex the export carries and its base58 as the
/// runtime logs print it.
pub fn program(byte: u8) -> (String, String) {
    let bytes = [byte; 32];
    (hex::encode(bytes), bs58::encode(bytes).into_string())
}

/// One exported transaction with a single outer instruction.
pub fn tx(
    txid: &str,
    height: i64,
    program_hex: &str,
    data: &[u8],
    status: &str,
    rolled_back: bool,
    logs: Option<Vec<String>>,
) -> Value {
    let mut v = json!({
        "txid": txid,
        "block_height": height,
        "ts": format!("2026-10-14T00:00:{:02}Z", height % 60),
        "instructions": [{
            "program_id": program_hex,
            "ix_path": "0",
            "data_hex": hex::encode(data),
            "accounts": [],
        }],
        "token_transfers": [{
            "ix_path": "0",
            "source": "aa",
            "destination": "bb",
            "mint": format!("mint-{txid}"),
            "amount": "1",
            "decimals": 6,
        }],
        "status": { "type": status },
        "rolled_back": rolled_back,
    });
    if let Some(logs) = logs {
        v["logs"] = json!(logs);
    }
    v
}

pub fn transfer_data(amount: u64) -> Vec<u8> {
    let mut d = vec![TRANSFER_TAG];
    d.extend_from_slice(&amount.to_le_bytes());
    d
}

/// What the fake indexer serves.
#[derive(Default)]
pub struct Upstream {
    /// Export pages per program id hex, in order; the `cursor` query names the
    /// page index.
    pub pages: std::collections::HashMap<String, Vec<Value>>,
    /// `GET /transactions/{txid}` bodies.
    pub logs: std::collections::HashMap<String, Value>,
    pub tip: Value,
    /// Every request path and query received.
    pub requests: Vec<String>,
}

/// Serve `upstream` as the arch-rust-indexer API on a local port; returns
/// its base URL.
pub async fn fake_indexer(upstream: Arc<Mutex<Upstream>>) -> String {
    use arch_program_indexer::axum::extract::{Query, Request};
    use std::collections::HashMap;

    async fn export(
        State(up): State<Arc<Mutex<Upstream>>>,
        Path(program): Path<String>,
        Query(q): Query<HashMap<String, String>>,
        req: Request,
    ) -> Json<Value> {
        let mut up = up.lock().unwrap();
        up.requests.push(req.uri().to_string());
        let index: usize = q.get("cursor").map_or(0, |c| c.parse().unwrap());
        Json(
            up.pages
                .get(&program)
                .and_then(|p| p.get(index).cloned())
                .unwrap_or_else(|| {
                    json!({"transactions": [], "next_cursor": null, "complete_through_height": 0})
                }),
        )
    }
    async fn transaction(
        State(up): State<Arc<Mutex<Upstream>>>,
        Path(txid): Path<String>,
        req: Request,
    ) -> Result<Json<Value>, arch_program_indexer::axum::http::StatusCode> {
        let mut up = up.lock().unwrap();
        up.requests.push(req.uri().to_string());
        up.logs
            .get(&txid)
            .cloned()
            .map(Json)
            .ok_or(arch_program_indexer::axum::http::StatusCode::NOT_FOUND)
    }
    async fn blocks(State(up): State<Arc<Mutex<Upstream>>>, req: Request) -> Json<Value> {
        let mut up = up.lock().unwrap();
        up.requests.push(req.uri().to_string());
        Json(json!({ "blocks": [up.tip.clone()] }))
    }

    let app = Router::new()
        .route(
            "/api/v1/programs/{program}/transactions/export",
            get(export),
        )
        .route("/api/v1/transactions/{txid}", get(transaction))
        .route("/api/v1/blocks", get(blocks))
        .with_state(upstream);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        arch_program_indexer::axum::serve(listener, app)
            .await
            .unwrap()
    });
    url
}

pub fn config(indexer_base_url: String, database_url: String) -> arch_program_indexer::Config {
    arch_program_indexer::Config {
        database_url,
        indexer_base_url,
        indexer_api_key: None,
        indexer_network: None,
        api_bind_addr: "127.0.0.1:0".into(),
        tail_interval_secs: 5,
        sample_interval_secs: 300,
        enrich_interval_secs: 60,
        export_page_limit: 1000,
        ingest_confirmation_depth: 0,
        ingest_end_height: None,
        cache_ttl_secs: 30,
        query_concurrency: 3,
        per_address_concurrency: 2,
    }
}
