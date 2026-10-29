//! `ExampleIndexer` end to end against Postgres and a fake arch-rust-indexer:
//! ingest through the host, every hook, the HTTP groups, replays gated by the
//! data revision, and delete-by-txid.
mod common;

use arch_program_indexer::axum::body::Body;
use arch_program_indexer::axum::http::{Request, StatusCode};
use arch_program_indexer::sampler::Sampler;
use arch_program_indexer::{build_info, checkpoints, DynIndexer, Host, Mode};
use common::*;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower::ServiceExt;

fn host(upstream: &str, url: &str, indexer: ExampleIndexer) -> Host {
    Host::new(
        build_info!(),
        config(upstream.into(), url.into()),
        vec![Box::new(indexer)],
    )
    .unwrap()
}

async fn get(
    app: &arch_program_indexer::axum::Router,
    uri: &str,
) -> (StatusCode, Value, Vec<(String, String)>) {
    let res = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
        .collect();
    let body = arch_program_indexer::axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = serde_json::from_slice(&body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
    (status, body, headers)
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

#[tokio::test]
#[ignore = "requires PostgreSQL via PROGRAM_INDEXER_TEST_DATABASE_URL"]
async fn an_example_indexer_runs_end_to_end() {
    let test = TestDb::new("example_indexer_test").await;
    let pool = test.pool.clone();
    let (program_hex, program_b58) = program(0x11);
    let key = format!("example:{program_hex}");
    let frame = |memo: &str| {
        vec![
            format!("Program {program_b58} invoke [1]"),
            format!("Program log: {memo}"),
            format!("Program {program_b58} success"),
        ]
    };

    let upstream = Arc::new(Mutex::new(Upstream {
        tip: json!({"height": 20, "timestamp": "2026-10-14T00:01:00Z"}),
        ..Default::default()
    }));
    {
        let mut up = upstream.lock().unwrap();
        up.pages.insert(
            program_hex.clone(),
            vec![
                json!({
                    "transactions": [
                        tx("t1", 10, &program_hex, &transfer_data(5), "Processed", false, Some(frame("hello"))),
                        tx("t2", 11, &program_hex, &transfer_data(7), "Failed", false, None),
                        tx("t3", 12, &program_hex, &transfer_data(9), "Processed", true, None),
                        tx("t4", 13, &program_hex, &[0x09], "Processed", false, None),
                    ],
                    "next_cursor": "1",
                    "complete_through_height": 13,
                }),
                json!({
                    "transactions": [
                        tx("t5", 15, &program_hex, &transfer_data(11), "Processed", false, None),
                    ],
                    "next_cursor": null,
                    "complete_through_height": 20,
                }),
            ],
        );
        up.logs.insert(
            "t5".into(),
            json!({ "txid": "t5", "logs": frame("fetched") }),
        );
    }
    let upstream_url = fake_indexer(Arc::clone(&upstream)).await;

    // A backfill through the host: runtime and indexer migrations, prepare,
    // one pass to the tip.
    let indexer = ExampleIndexer::new(vec![program_hex.clone()]);
    let observed = Arc::clone(&indexer.observed);
    let host = host(&upstream_url, &test.url, indexer);
    host.run(Mode::Backfill).await.unwrap();
    assert_eq!(observed.prepared.load(Ordering::Relaxed), 1);
    // A backfill ends with one enrich pass, so it is immediately queryable.
    assert_eq!(observed.enriched.load(Ordering::Relaxed), 1);

    // Only the successful, unrolled-back transactions decode; the transfer
    // whose export lacked logs got them from the indexer.
    let rows: Vec<(String, i64, Option<String>)> =
        sqlx::query_as("SELECT txid, amount, memo FROM example_transfers ORDER BY txid")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            ("t1".into(), 5, Some("hello".into())),
            ("t5".into(), 11, Some("fetched".into())),
        ]
    );
    // The undecodable instruction is a marker, written by the runtime and
    // never handed to `persist`.
    let unknowns: Vec<(String, String, String)> =
        sqlx::query_as("SELECT decoder, discriminator, txid FROM decoder_unknowns")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(unknowns, vec![("example".into(), "09".into(), "t4".into())]);
    assert_eq!(*observed.persisted_batches.lock().unwrap(), vec![1, 1]);
    let mints: Vec<(String, i32)> =
        sqlx::query_as("SELECT mint, decimals FROM mint_decimals ORDER BY mint")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        mints,
        vec![
            ("mint-t1".into(), 6),
            ("mint-t4".into(), 6),
            ("mint-t5".into(), 6)
        ]
    );
    assert_eq!(checkpoints::get(&pool, &key).await.unwrap(), 20);
    let mark: (i64, i64) =
        sqlx::query_as("SELECT checkpoint_height, tip_height FROM scan_marks WHERE name = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(mark, (20, 20));
    {
        let up = upstream.lock().unwrap();
        let fetched: Vec<&String> = up
            .requests
            .iter()
            .filter(|r| r.starts_with("/api/v1/transactions/"))
            .collect();
        // Logs are fetched only for a recognized transaction that had none.
        assert_eq!(
            fetched,
            vec!["/api/v1/transactions/t4", "/api/v1/transactions/t5"]
        );
        assert!(up.requests.iter().any(|r| r.contains("from_height=0")));
    }
    upstream.lock().unwrap().requests.clear();

    // A second pass resumes from the checkpoint and writes nothing new: the
    // inserts are idempotent.
    let runner = host.runner(pool.clone());
    assert_eq!(runner.catch_up().await.unwrap(), 0);
    assert!(upstream
        .lock()
        .unwrap()
        .requests
        .iter()
        .any(|r| r.contains("from_height=20")));

    // A replay requested for a later data revision is left for the build
    // that has it.
    sqlx::query("INSERT INTO replay_requests (checkpoint, data_revision) VALUES ($1, 2)")
        .bind(&key)
        .execute(&pool)
        .await
        .unwrap();
    upstream.lock().unwrap().requests.clear();
    runner.catch_up().await.unwrap();
    assert!(!upstream
        .lock()
        .unwrap()
        .requests
        .iter()
        .any(|r| r.contains("from_height=0")));
    let standing: i64 = sqlx::query_scalar("SELECT count(*) FROM replay_requests")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(standing, 1);
    // That build walks from genesis, then retires the request; the
    // checkpoint never moved backwards.
    let mut revised = ExampleIndexer::new(vec![program_hex.clone()]);
    revised.revision = 2;
    let revised_host = self::host(&upstream_url, &test.url, revised);
    upstream.lock().unwrap().requests.clear();
    revised_host.runner(pool.clone()).catch_up().await.unwrap();
    assert!(upstream
        .lock()
        .unwrap()
        .requests
        .iter()
        .any(|r| r.contains("from_height=0")));
    let standing: i64 = sqlx::query_scalar("SELECT count(*) FROM replay_requests")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(standing, 0);
    assert_eq!(checkpoints::get(&pool, &key).await.unwrap(), 20);

    // The sampler runs every indexer's `sample`.
    Sampler {
        ctx: runner.ctx(),
        indexers: host.indexers.clone(),
    }
    .sample_once()
    .await;
    let sampled: i64 = sqlx::query_scalar("SELECT total FROM example_samples")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sampled, 16);
    assert_eq!(observed.samples.load(Ordering::Relaxed), 1);

    // The tail runs `on_activity` after every pass.
    let tail = tokio::spawn(async move { runner.run_forever(Duration::from_secs(3600)).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while observed.activity.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("on_activity ran after the pass");
    tail.abort();

    // The HTTP API: the indexer's groups behind their layers, the runtime's
    // views scoped to this host's cursors.
    checkpoints::set(&pool, "foreign", 5).await.unwrap();
    let app = host.router(test.another_pool().await);

    let (status, body, headers) = get(&app, "/v1/example/transfers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "count": 2, "total": 16 }));
    assert_eq!(header(&headers, "x-cache"), Some("MISS"));
    assert_eq!(
        header(&headers, "cache-control"),
        Some("public, s-maxage=30, stale-while-revalidate=120")
    );
    assert_eq!(
        header(&headers, "x-indexer-version"),
        Some(host.build.describe().as_str())
    );
    let (_, _, headers) = get(&app, "/v1/example/transfers").await;
    assert_eq!(header(&headers, "x-cache"), Some("HIT"));

    let (status, body, headers) = get(
        &app,
        &format!(
            "/v1/example/accounts/{}",
            bs58::encode([7u8; 32]).into_string()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["addr"], json!(hex::encode([7u8; 32])));
    assert_eq!(
        header(&headers, "cache-control"),
        Some("private, max-age=10")
    );
    assert_eq!(header(&headers, "x-cache"), None);

    let (status, _, headers) = get(&app, "/v1/example/admin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header(&headers, "cache-control"), Some("no-store"));

    let (status, body, _) = get(&app, "/v1/sync-status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["checkpoints"].as_array().unwrap().len(), 1);
    assert_eq!(body["checkpoints"][0]["name"], json!(key));
    assert_eq!(body["checkpoints"][0]["block_height"], json!(20));
    assert_eq!(body["checkpoints"][0]["tip_height"], json!(20));
    assert_eq!(body["replays"], json!([]));

    let (status, body, _) = get(&app, "/v1/version").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["service"], json!("arch-program-indexer"));
    assert_eq!(body["network"], json!("default"));
    assert_eq!(
        body["schema_versions"],
        json!({ "example": 1, "runtime": 1 })
    );
    assert_eq!(
        body["indexers"],
        json!([{ "id": "example", "program_ids": [program_hex], "data_revision": 1 }])
    );

    let (status, metrics, _) = get(&app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let metrics = metrics.as_str().unwrap();
    for line in [
        format!("program_indexer_checkpoint_height{{source=\"{key}\"}} 20"),
        "program_indexer_decoder_unknown_total{decoder=\"example\",discriminator=\"09\"} 1".into(),
        "program_indexer_data_revision{indexer=\"example\"} 1".into(),
        "program_indexer_schema_version{namespace=\"example\"} 1".into(),
        "example_transfers_total 2".into(),
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in\n{metrics}");
    }
    assert!(!metrics.contains("foreign"), "{metrics}");

    assert_eq!(get(&app, "/readyz").await.0, StatusCode::OK);
    assert_eq!(get(&app, "/healthz").await.0, StatusCode::OK);

    // delete-tx removes the indexer's rows and the runtime's markers.
    host.run(Mode::DeleteTx("t4".into())).await.unwrap();
    host.run(Mode::DeleteTx("t1".into())).await.unwrap();
    let left: Vec<(String,)> = sqlx::query_as("SELECT txid FROM example_transfers")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(left, vec![("t5".into(),)]);
    let markers: i64 = sqlx::query_scalar("SELECT count(*) FROM decoder_unknowns")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(markers, 0);

    test.drop_schema().await;
}

/// Indexers that cannot share a host are refused before anything runs.
#[test]
fn a_host_refuses_colliding_or_malformed_indexers() {
    let build = || build_info!();
    let cfg = || config("http://127.0.0.1:1".into(), "postgres://unused".into());
    let a = "ab".repeat(32);
    let twice: Vec<Box<dyn DynIndexer>> = vec![
        Box::new(ExampleIndexer::new(vec![a.clone()])),
        Box::new(ExampleIndexer::new(vec!["cd".repeat(32)])),
    ];
    let err = Host::new(build(), cfg(), twice).err().unwrap();
    assert!(err.to_string().contains("share the id"), "{err}");

    let base58 = vec![Box::new(ExampleIndexer::new(vec![
        bs58::encode([1u8; 32]).into_string()
    ])) as Box<dyn DynIndexer>];
    let err = Host::new(build(), cfg(), base58).err().unwrap();
    assert!(err.to_string().contains("64 lowercase hex"), "{err}");

    let one_cursor_twice =
        vec![Box::new(ExampleIndexer::new(vec![a.clone(), a])) as Box<dyn DynIndexer>];
    let err = Host::new(build(), cfg(), one_cursor_twice).err().unwrap();
    assert!(err.to_string().contains("already taken"), "{err}");

    let both: Vec<Box<dyn DynIndexer>> = vec![
        Box::new(ExampleIndexer::new(vec!["ab".repeat(32)])),
        Box::new(OtherIndexer),
    ];
    assert!(Host::new(build(), cfg(), both).is_ok());
}

/// `Mode::All`, as `run` starts it: migrations, the tail with its
/// `on_activity`, the sampler, background tasks and the HTTP API in one
/// process.
#[tokio::test]
#[ignore = "requires PostgreSQL via PROGRAM_INDEXER_TEST_DATABASE_URL"]
async fn the_all_mode_ingests_samples_and_serves() {
    let test = TestDb::new("all_mode_test").await;
    let (program_hex, _) = program(0x22);
    let upstream = Arc::new(Mutex::new(Upstream {
        tip: json!({"height": 3, "timestamp": "2026-10-14T00:00:03Z"}),
        ..Default::default()
    }));
    upstream.lock().unwrap().pages.insert(
        program_hex.clone(),
        vec![json!({
            "transactions": [tx("a1", 3, &program_hex, &transfer_data(4), "Processed", false, Some(vec![]))],
            "next_cursor": null,
            "complete_through_height": 3,
        })],
    );
    let upstream_url = fake_indexer(upstream).await;
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let indexer = ExampleIndexer::new(vec![program_hex]);
    let observed = Arc::clone(&indexer.observed);
    let mut host = host(&upstream_url, &test.url, indexer);
    host.config.api_bind_addr = format!("127.0.0.1:{port}");
    let service = tokio::spawn(async move { host.run(Mode::All).await });

    let base = format!("http://127.0.0.1:{port}");
    let body = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(res) = reqwest::get(format!("{base}/v1/example/transfers?_=1")).await {
                let body: Value = res.json().await.unwrap_or_default();
                if body["count"] == json!(1)
                    && observed.activity.load(Ordering::Relaxed) > 0
                    && observed.enriched.load(Ordering::Relaxed) > 0
                {
                    return body;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the service ingested and served");
    assert_eq!(body["total"], json!(4));
    assert!(observed.samples.load(Ordering::Relaxed) >= 1);
    assert_eq!(observed.prepared.load(Ordering::Relaxed), 1);
    assert!(observed.enriched.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        reqwest::get(format!("{base}/healthz"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    assert!(!service.is_finished(), "no loop exited");
    service.abort();

    test.drop_schema().await;
}
