//! The price spine, the enrich phases, the day-export hooks and the metrics
//! scrape against Postgres: feed sync (Coinbase through a fake, constants,
//! the Pyth→Coinbase resume rule, the cold-start anchor), derive_prices
//! ordered after the feeds and before any enrich with claims honoured, one
//! snapshot across the day hooks, and one `# TYPE` per family.
mod common;

use arch_indexer::axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use arch_indexer::export::{self, Completeness, DayFacts, Holding};
use arch_indexer::input::IndexedTx;
use arch_indexer::prices::{FeedSync, PriceFeed};
use arch_indexer::sqlx::migrate::Migrator;
use arch_indexer::{
    build_info, Ctx, DynIndexer, Host, IndexerEvent, PgPool, PgTransaction, PriceCtx,
    ProgramIndexer,
};
use chrono::{DateTime, Duration, DurationRound, SecondsFormat, Utc};
use common::{config, TestDb};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

static NOOP_MIGRATOR: Migrator = sqlx::migrate!("tests/noop_migrations");

type Log = Arc<Mutex<Vec<String>>>;

#[derive(Debug)]
struct NoEvent;

impl IndexerEvent for NoEvent {
    fn kind(&self) -> &'static str {
        "none"
    }
}

/// An indexer that only implements the hooks under test, recording each call.
struct HookIndexer {
    id: &'static str,
    log: Log,
    /// `None`: `priced_mints` fails.
    claims: Option<Vec<String>>,
    /// `Err`: `earliest_activity` fails.
    earliest: Result<Option<DateTime<Utc>>, ()>,
    incomplete: Option<String>,
    facts: Vec<(&'static str, Vec<i64>)>,
    references: Vec<&'static str>,
    holdings: Vec<Holding>,
    metrics: &'static str,
}

impl HookIndexer {
    fn new(id: &'static str, log: &Log) -> Self {
        Self {
            id,
            log: log.clone(),
            claims: Some(Vec::new()),
            earliest: Ok(None),
            incomplete: None,
            facts: Vec::new(),
            references: Vec::new(),
            holdings: Vec::new(),
            metrics: "",
        }
    }

    fn record(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }
}

fn sorted(set: &HashSet<String>) -> Vec<String> {
    let mut v: Vec<String> = set.iter().cloned().collect();
    v.sort();
    v
}

impl ProgramIndexer for HookIndexer {
    type Event = NoEvent;

    fn id(&self) -> &'static str {
        self.id
    }
    fn program_ids(&self) -> Vec<String> {
        Vec::new()
    }
    fn decode(&self, _: &str, _: &IndexedTx) -> anyhow::Result<Vec<NoEvent>> {
        Ok(Vec::new())
    }
    async fn persist(&self, _: &mut PgTransaction<'_>, _: &[NoEvent]) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn delete_tx(&self, _: &mut PgTransaction<'_>, _: &str) -> anyhow::Result<u64> {
        Ok(0)
    }
    fn migrator(&self) -> &'static Migrator {
        &NOOP_MIGRATOR
    }

    async fn earliest_activity(&self, _pool: &PgPool) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.earliest
            .map_err(|()| anyhow::anyhow!("{} has no events table", self.id))
    }

    async fn priced_mints(&self, _ctx: &Ctx) -> anyhow::Result<Vec<String>> {
        self.record(format!("{}.priced_mints", self.id));
        self.claims
            .clone()
            .ok_or_else(|| anyhow::anyhow!("{} cannot list its mints", self.id))
    }

    async fn derive_prices(&self, ctx: &Ctx, prices: &PriceCtx) -> anyhow::Result<()> {
        // The feed candles are written before any derivation runs.
        let candles: i64 = sqlx::query_scalar("SELECT count(*) FROM price_candles")
            .fetch_one(&ctx.pool)
            .await?;
        self.record(format!(
            "{}.derive_prices feeds={:?} claimed={:?} candles={candles}",
            self.id,
            sorted(&prices.feed_mints),
            sorted(&prices.claimed_mints),
        ));
        Ok(())
    }

    async fn enrich(&self, _ctx: &Ctx) -> anyhow::Result<()> {
        self.record(format!("{}.enrich", self.id));
        Ok(())
    }

    async fn metrics(&self, _pool: &PgPool, out: &mut String) -> anyhow::Result<()> {
        out.push_str(self.metrics);
        Ok(())
    }

    async fn day_incomplete(
        &self,
        tx: &mut PgTransaction<'_>,
        _end: DateTime<Utc>,
    ) -> anyhow::Result<Option<String>> {
        // Reads through the export's transaction.
        let _: i64 = sqlx::query_scalar("SELECT count(*) FROM checkpoints")
            .fetch_one(&mut **tx)
            .await?;
        Ok(self.incomplete.clone())
    }

    async fn day_facts(
        &self,
        tx: &mut PgTransaction<'_>,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
        cap: i64,
    ) -> anyhow::Result<DayFacts> {
        let checkpoints: i64 = sqlx::query_scalar("SELECT count(*) FROM checkpoints")
            .fetch_one(&mut **tx)
            .await?;
        let mut facts = DayFacts::default();
        for (key, rows) in &self.facts {
            let rows: Vec<i64> = rows.iter().copied().take(cap as usize).collect();
            facts.insert_rows(key, &rows)?;
        }
        for key in &self.references {
            facts.insert_reference(key, &serde_json::json!({ "checkpoints": checkpoints }))?;
        }
        Ok(facts)
    }

    async fn day_holdings(
        &self,
        _tx: &mut PgTransaction<'_>,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
        cap: i64,
    ) -> anyhow::Result<Vec<Holding>> {
        Ok(self.holdings.iter().take(cap as usize).cloned().collect())
    }
}

fn host(db_url: &str, feeds: &str, coinbase: &str, indexers: Vec<HookIndexer>) -> Host {
    let mut config = config("http://127.0.0.1:1".into(), db_url.into());
    config.price_feeds = arch_indexer::prices::parse_price_feeds(feeds).unwrap();
    config.coinbase_api_url = coinbase.into();
    Host::new(
        build_info!(),
        config,
        indexers
            .into_iter()
            .map(|ix| Box::new(ix) as Box<dyn DynIndexer>)
            .collect(),
    )
    .unwrap()
}

/// Coinbase's candle route: hourly rows over `[start, end]`, newest first,
/// the close a function of the hour; products in `failing` answer 404.
#[derive(Default)]
struct Coinbase {
    requests: Vec<(String, String, String)>,
    failing: HashSet<String>,
}

fn close_of(t: i64) -> f64 {
    100.0 + ((t / 3600) % 100) as f64
}

async fn fake_coinbase(state: Arc<Mutex<Coinbase>>) -> String {
    async fn candles(
        State(state): State<Arc<Mutex<Coinbase>>>,
        Path(product): Path<String>,
        Query(q): Query<HashMap<String, String>>,
    ) -> Response {
        let mut s = state.lock().unwrap();
        assert_eq!(q["granularity"], "3600");
        s.requests
            .push((product.clone(), q["start"].clone(), q["end"].clone()));
        if s.failing.contains(&product) {
            return (StatusCode::NOT_FOUND, "NotFound").into_response();
        }
        let start: DateTime<Utc> = q["start"].parse().unwrap();
        let end: DateTime<Utc> = q["end"].parse().unwrap();
        let mut t = start.duration_trunc(Duration::hours(1)).unwrap();
        if t < start {
            t += Duration::hours(1);
        }
        let mut rows = Vec::new();
        while t <= end {
            let ts = t.timestamp();
            rows.push((ts, 1.0, 2.0, 1.5, close_of(ts), 10.0));
            t += Duration::hours(1);
        }
        rows.reverse();
        Json(rows).into_response()
    }
    let app = Router::new()
        .route("/products/{product}/candles", get(candles))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { arch_indexer::axum::serve(listener, app).await.unwrap() });
    url
}

fn secs(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

async fn candles_of(pool: &PgPool, mint: &str) -> Vec<(DateTime<Utc>, f64, String)> {
    sqlx::query_as(
        r#"SELECT bucket_ts, close_usd::float8, source FROM price_candles
           WHERE mint = $1 AND "interval" = '1h' ORDER BY bucket_ts"#,
    )
    .bind(mint)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// A cold feed starts a day before the earliest activity any indexer holds,
/// in chunks Coinbase accepts; a warm one resumes at its newest FEED candle,
/// a Pyth one included, inclusive; a failing product or an indexer that
/// cannot date its activity costs only the mints that need it.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn feeds_sync_coinbase_from_the_earliest_activity_and_resume_past_pyth() {
    let test = TestDb::new("hooks_feeds").await;
    let pool = test.pool.clone();
    let coinbase = Arc::new(Mutex::new(Coinbase::default()));
    coinbase.lock().unwrap().failing.insert("FAIL-USD".into());
    let url = fake_coinbase(coinbase.clone()).await;
    let log = Log::default();

    let now = Utc::now();
    let earliest_a = now - Duration::days(24);
    let earliest_b = now - Duration::days(3);
    let mut a = HookIndexer::new("a", &log);
    a.earliest = Ok(Some(earliest_b));
    let mut b = HookIndexer::new("b", &log);
    b.earliest = Ok(Some(earliest_a));
    let c = HookIndexer::new("c", &log);
    let feeds = r#"{"BTC":"coinbase:BTC-USD","fail":"coinbase:FAIL-USD","usd":"const:1.0"}"#;
    let host = host(&test.url, feeds, &url, vec![a, b, c]);
    host.migrate(&pool).await.unwrap();

    // A warm mint: Pyth wrote up to `pyth_at`; a pool candle after it is not
    // a feed candle and does not move the cursor.
    let pyth_at = (now - Duration::hours(5))
        .duration_trunc(Duration::hours(1))
        .unwrap();
    sqlx::query(
        r#"INSERT INTO price_candles (mint, "interval", bucket_ts, close_usd, source) VALUES
           ('eth', '1h', $1, 2500, 'pyth'),
           ('eth', '1h', $1 - interval '1 hour', 2400, 'pyth'),
           ('eth', '1h', $1 + interval '2 hours', 9, 'pool:p1')"#,
    )
    .bind(pyth_at)
    .execute(&pool)
    .await
    .unwrap();

    let enricher = host.enricher(host.runner(pool.clone()).ctx());
    enricher
        .feeds
        .sync_coinbase(&pool, &host.indexers)
        .await
        .unwrap();

    let requests = coinbase.lock().unwrap().requests.clone();
    let btc: Vec<_> = requests.iter().filter(|r| r.0 == "BTC-USD").collect();
    // Twenty-five days from a day before the earliest activity: 10 + 10 + 5.
    assert_eq!(btc.len(), 3, "{btc:?}");
    assert_eq!(btc[0].1, secs(earliest_a - Duration::days(1)));
    assert_eq!(
        btc[0].2,
        secs(earliest_a - Duration::days(1) + Duration::days(10))
    );
    assert_eq!(
        btc[1].1,
        secs(earliest_a - Duration::days(1) + Duration::days(10) + Duration::seconds(1))
    );
    let written = candles_of(&pool, "btc").await;
    assert!(written.len() >= 24 * 25 - 1, "{} candles", written.len());
    assert!(written
        .iter()
        .all(|(t, close, source)| source == "coinbase" && *close == close_of(t.timestamp())));
    assert!(requests.iter().any(|r| r.0 == "FAIL-USD"));
    assert!(candles_of(&pool, "fail").await.is_empty());

    // Unconfigured mints are not fetched; the Pyth history is not either.
    assert!(!requests.iter().any(|r| r.0 == "ETH-USD"));

    // Resume: switch `eth` to Coinbase. It starts at the Pyth candle,
    // inclusive, and takes that bucket over; older Pyth rows stay history.
    let host = self::host(
        &test.url,
        r#"{"eth":"coinbase:ETH-USD"}"#,
        &url,
        vec![HookIndexer::new("a", &log)],
    );
    let feeds = FeedSync::from_config(&host.config);
    feeds.sync_coinbase(&pool, &host.indexers).await.unwrap();
    let eth: Vec<_> = coinbase
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r.0 == "ETH-USD")
        .cloned()
        .collect();
    assert_eq!(eth.len(), 1);
    assert_eq!(eth[0].1, secs(pyth_at));
    let eth = candles_of(&pool, "eth").await;
    assert_eq!(
        eth[0],
        (pyth_at - Duration::hours(1), 2400.0, "pyth".to_string())
    );
    assert_eq!(
        eth[1],
        (pyth_at, close_of(pyth_at.timestamp()), "coinbase".into())
    );
    assert!(eth[2..].iter().all(|(_, _, s)| s == "coinbase"), "{eth:?}");

    // A cold mint whose anchor cannot be read fails alone; a warm one still
    // syncs, never asking for the anchor.
    let mut broken = HookIndexer::new("broken", &log);
    broken.earliest = Err(());
    let host = self::host(
        &test.url,
        r#"{"eth":"coinbase:ETH-USD","sol":"coinbase:SOL-USD"}"#,
        &url,
        vec![broken],
    );
    let before = coinbase.lock().unwrap().requests.len();
    FeedSync::from_config(&host.config)
        .sync_coinbase(&pool, &host.indexers)
        .await
        .unwrap();
    let after: Vec<_> = coinbase.lock().unwrap().requests[before..].to_vec();
    assert!(after.iter().all(|r| r.0 == "ETH-USD"), "{after:?}");
    assert!(!after.is_empty());
    assert!(candles_of(&pool, "sol").await.is_empty());

    // No activity anywhere: a week back.
    let host = self::host(
        &test.url,
        r#"{"ada":"coinbase:ADA-USD"}"#,
        &url,
        vec![HookIndexer::new("a", &log)],
    );
    let before = coinbase.lock().unwrap().requests.len();
    FeedSync::from_config(&host.config)
        .sync_coinbase(&pool, &host.indexers)
        .await
        .unwrap();
    let start: DateTime<Utc> = coinbase.lock().unwrap().requests[before].1.parse().unwrap();
    let lookback = Utc::now() - start;
    assert!(
        lookback >= Duration::days(7) && lookback < Duration::days(7) + Duration::minutes(1),
        "{lookback}"
    );

    test.drop_schema().await;
}

/// A constant feed is one candle at epoch, its value refreshed in place.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn const_feeds_write_one_epoch_candle() {
    let test = TestDb::new("hooks_const").await;
    let pool = test.pool.clone();
    let epoch = DateTime::from_timestamp(0, 0).unwrap();
    let feeds = |value: f64| {
        FeedSync::new(
            BTreeMap::from([
                ("usd".to_string(), PriceFeed::Const(value)),
                ("btc".to_string(), PriceFeed::Coinbase("BTC-USD".into())),
            ]),
            "http://127.0.0.1:1".into(),
        )
    };
    let host = host(&test.url, "{}", "http://127.0.0.1:1", vec![]);
    host.migrate(&pool).await.unwrap();
    feeds(1.0).ensure_const_candles(&pool).await.unwrap();
    feeds(1.0).ensure_const_candles(&pool).await.unwrap();
    assert_eq!(
        candles_of(&pool, "usd").await,
        vec![(epoch, 1.0, "const".to_string())]
    );
    feeds(0.999).ensure_const_candles(&pool).await.unwrap();
    assert_eq!(
        candles_of(&pool, "usd").await,
        vec![(epoch, 0.999, "const".to_string())]
    );
    assert!(candles_of(&pool, "btc").await.is_empty());
    assert_eq!(
        feeds(1.0).feed_mints(),
        HashSet::from(["usd".to_string(), "btc".to_string()])
    );
    test.drop_schema().await;
}

/// One pass: feed candles, then every derive_prices (each told the feed
/// mints and the OTHER indexers' claims), then every enrich. An indexer that
/// cannot report its claims skips derivation for the pass; enrich still runs.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn an_enrich_pass_derives_prices_between_the_feeds_and_enrich() {
    let test = TestDb::new("hooks_enrich").await;
    let pool = test.pool.clone();
    let log = Log::default();
    let mut vaults = HookIndexer::new("vaults", &log);
    vaults.claims = Some(vec!["share".into()]);
    let pools = HookIndexer::new("pools", &log);
    let host = host(
        &test.url,
        r#"{"USD":"const:1.0"}"#,
        "http://127.0.0.1:1",
        vec![pools, vaults],
    );
    host.migrate(&pool).await.unwrap();
    host.enricher(host.runner(pool.clone()).ctx())
        .enrich_once()
        .await;
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "pools.priced_mints",
            "vaults.priced_mints",
            r#"pools.derive_prices feeds=["usd"] claimed=["share"] candles=1"#,
            r#"vaults.derive_prices feeds=["usd"] claimed=[] candles=1"#,
            "pools.enrich",
            "vaults.enrich",
        ]
    );

    let log = Log::default();
    let mut vaults = HookIndexer::new("vaults", &log);
    vaults.claims = None;
    let pools = HookIndexer::new("pools", &log);
    let host = self::host(&test.url, "{}", "http://127.0.0.1:1", vec![pools, vaults]);
    host.enricher(host.runner(pool.clone()).ctx())
        .enrich_once()
        .await;
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "pools.priced_mints",
            "vaults.priced_mints",
            "pools.enrich",
            "vaults.enrich",
        ]
    );
    test.drop_schema().await;
}

fn holding(wallet: &str, venue: &str, side: &str) -> Holding {
    Holding {
        wallet: wallet.into(),
        venue: venue.into(),
        address: format!("{venue}-1"),
        mint: Some("m".into()),
        side: side.into(),
        close: 5.into(),
        average: 3.into(),
        shares_close: None,
        shares_average: None,
    }
}

/// The day hooks read one snapshot: completeness from the runtime's tables,
/// then each indexer's reason, facts and holdings, merged.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn the_day_hooks_compose_over_one_snapshot() {
    let test = TestDb::new("hooks_export").await;
    let pool = test.pool.clone();
    let log = Log::default();
    let mut lend = HookIndexer::new("lend", &log);
    lend.facts = vec![("balances", vec![1, 2, 3])];
    lend.references = vec!["banks"];
    lend.holdings = vec![
        holding("bb", "lending", "supplied"),
        holding("aa", "lending", "supplied"),
    ];
    let mut vaults = HookIndexer::new("vaults", &log);
    vaults.incomplete = Some("day not complete: vault v2 shares reconciled through never".into());
    vaults.facts = vec![("vault_events", vec![7])];
    vaults.references = vec!["share_marks"];
    vaults.holdings = vec![holding("aa", "vault", "deposited")];
    let host = host(&test.url, "{}", "http://127.0.0.1:1", vec![lend, vaults]);
    host.migrate(&pool).await.unwrap();

    let end: DateTime<Utc> = "2026-10-02T08:00:00Z".parse().unwrap();
    let start = end - Duration::days(1);
    sqlx::raw_sql(
        "INSERT INTO checkpoints (name, block_height) VALUES ('lend', 10);
         INSERT INTO scan_marks (name, checkpoint_height, tip_height, tip_ts)
         VALUES ('lend', 10, 12, '2026-10-03T00:00:00Z');",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = export::snapshot(&pool).await.unwrap();
    let completeness = Completeness::load(&mut tx).await.unwrap();
    assert_eq!(completeness.checkpoints.len(), 1);
    assert_eq!(completeness.scans[0].tip_height, 12);
    // The runtime passes the day; the vaults indexer refuses it.
    assert_eq!(completeness.incomplete(end), None);
    assert_eq!(
        export::day_incomplete(&mut tx, &host.indexers, &completeness, end)
            .await
            .unwrap()
            .as_deref(),
        Some("day not complete: vault v2 shares reconciled through never")
    );

    // A write after the snapshot began is not seen by the hooks.
    sqlx::query("INSERT INTO checkpoints (name, block_height) VALUES ('late', 1)")
        .execute(&pool)
        .await
        .unwrap();
    let facts = export::day_facts(&mut tx, &host.indexers, start, end, 2)
        .await
        .unwrap();
    assert_eq!(
        facts.facts.keys().copied().collect::<Vec<_>>(),
        ["balances", "banks", "share_marks", "vault_events"]
    );
    assert_eq!(facts.facts["balances"], serde_json::json!([1, 2]));
    assert_eq!(
        facts.facts["banks"],
        serde_json::json!({ "checkpoints": 1 })
    );
    assert_eq!(facts.counted_rows, 3);
    let holdings = export::day_holdings(&mut tx, &host.indexers, start, end, 2)
        .await
        .unwrap();
    assert_eq!(
        holdings,
        vec![
            holding("aa", "lending", "supplied"),
            holding("aa", "vault", "deposited"),
        ]
    );
    // The snapshot is read-only.
    assert!(sqlx::query("INSERT INTO checkpoints (name) VALUES ('x')")
        .execute(&mut *tx)
        .await
        .is_err());
    drop(tx);

    // A replay pending answers first, before any indexer is asked.
    sqlx::query("INSERT INTO replay_requests (checkpoint, data_revision) VALUES ('lend', 2)")
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = export::snapshot(&pool).await.unwrap();
    let completeness = Completeness::load(&mut tx).await.unwrap();
    assert_eq!(
        export::day_incomplete(&mut tx, &host.indexers, &completeness, end)
            .await
            .unwrap()
            .as_deref(),
        Some("day not complete: replay pending for lend")
    );
    drop(tx);

    // Two indexers writing one key is refused, naming the second.
    let mut a = HookIndexer::new("a", &log);
    a.facts = vec![("swaps", vec![1])];
    let mut b = HookIndexer::new("b", &log);
    b.facts = vec![("swaps", vec![2])];
    let clash = self::host(&test.url, "{}", "http://127.0.0.1:1", vec![a, b]);
    let mut tx = export::snapshot(&pool).await.unwrap();
    let err = export::day_facts(&mut tx, &clash.indexers, start, end, 10)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "indexer b: day fact \"swaps\" is already set"
    );
    drop(tx);
    test.drop_schema().await;
}

/// Two indexers declaring one family yield one `# TYPE` and one contiguous
/// group; the runtime's families carry `METRICS_PREFIX`.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn the_scrape_types_each_family_once_under_the_configured_prefix() {
    let test = TestDb::new("hooks_metrics").await;
    let pool = test.pool.clone();
    let log = Log::default();
    let mut clamm = HookIndexer::new("clamm", &log);
    clamm.metrics = "# TYPE apa_events_unpriced gauge\n\
                     apa_events_unpriced{kind=\"swap\"} 3\n";
    let mut lend = HookIndexer::new("lend", &log);
    lend.metrics = "# TYPE apa_events_unpriced gauge\n\
                    apa_events_unpriced{kind=\"lending\"} 0\n";
    let host = host(&test.url, "{}", "http://127.0.0.1:1", vec![clamm, lend]);
    host.migrate(&pool).await.unwrap();

    let scrape = |host: &Host| {
        let app = host.router(pool.clone());
        async move {
            let res = app
                .oneshot(
                    Request::builder()
                        .uri("/metrics")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let body = arch_indexer::axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            String::from_utf8(body.to_vec()).unwrap()
        }
    };
    let metrics = scrape(&host).await;
    let types: Vec<&str> = metrics
        .lines()
        .filter(|l| l.starts_with("# TYPE"))
        .collect();
    let unique: HashSet<&&str> = types.iter().collect();
    assert_eq!(types.len(), unique.len(), "{metrics}");
    assert!(metrics.contains(
        "# TYPE apa_events_unpriced gauge\n\
         apa_events_unpriced{kind=\"swap\"} 3\n\
         apa_events_unpriced{kind=\"lending\"} 0\n"
    ));
    assert!(metrics.contains("# TYPE apa_build_info gauge\napa_build_info{"));
    assert!(metrics.contains("apa_schema_version{namespace=\"runtime\"} 2"));
    assert!(metrics.contains("# TYPE apa_cache_requests_total counter"));

    let mut config = host.config.clone();
    config.metrics_prefix = "program_indexer".into();
    let renamed = Host::new(
        build_info!(),
        config,
        vec![Box::new(HookIndexer::new("clamm", &log)) as Box<dyn DynIndexer>],
    )
    .unwrap();
    let metrics = scrape(&renamed).await;
    assert!(metrics.contains("program_indexer_build_info{"), "{metrics}");
    assert!(!metrics.contains("apa_"), "{metrics}");
    test.drop_schema().await;
}
