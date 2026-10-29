//! Real-Postgres check that one refused cursor does not stop the others.
mod common;

use arch_indexer::explorer::ExplorerClient;
use arch_indexer::runner::Runner;
use arch_indexer::{checkpoints, db, DynIndexer};
use common::{ExampleIndexer, TestDb};
use std::io::{Read, Write};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn a_refused_cursor_does_not_stop_the_others() {
    let test = TestDb::new("cursor_isolation_test").await;
    let pool = test.pool.clone();
    db::migrate(&pool, db::RUNTIME_NAMESPACE, &db::RUNTIME_MIGRATOR)
        .await
        .unwrap();

    let refused = "1".repeat(64);
    let served = "2".repeat(64);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn({
        let refused = refused.clone();
        move || {
            // Two passes; the first finds the tip unreadable.
            let mut tips = 0;
            for _ in 0..6 {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut buf = [0; 1024];
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buf[..n]);
                }
                let request = String::from_utf8(request).unwrap();
                let (status, body) = if request.starts_with("GET /api/v1/blocks?") {
                    tips += 1;
                    if tips == 1 {
                        ("500 Internal Server Error", "")
                    } else {
                        (
                            "200 OK",
                            r#"{"blocks":[{"height":50,"timestamp":"2026-10-14T00:01:02Z"}]}"#,
                        )
                    }
                } else if request.contains(&format!("/programs/{refused}/")) {
                    (
                        "429 Too Many Requests",
                        r#"{"error":"rate_limit_exceeded"}"#,
                    )
                } else {
                    (
                        "200 OK",
                        r#"{"transactions":[],"next_cursor":null,"complete_through_height":42}"#,
                    )
                };
                write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        }
    });
    let indexer: Arc<dyn DynIndexer> =
        Arc::new(ExampleIndexer::new(vec![refused.clone(), served.clone()]));
    let runner = Runner {
        pool: pool.clone(),
        client: ExplorerClient::new(format!("http://{address}"), None, None),
        indexers: vec![indexer],
        page_limit: 1000,
        confirmation_depth: 0,
        end_height: None,
    };
    let marks = || {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (String, i64, i64, chrono::DateTime<chrono::Utc>)>(
                "SELECT name, checkpoint_height, tip_height, tip_ts FROM scan_marks",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
        }
    };
    // Ungated, the tip only dates the scan marks: an unreadable one stops
    // no cursor, and no pass is marked.
    let err = runner.catch_up().await.unwrap_err().to_string();
    assert!(err.contains(&format!("example:{refused}")), "{err}");
    assert_eq!(
        checkpoints::get(&pool, &format!("example:{served}"))
            .await
            .unwrap(),
        42
    );
    assert_eq!(marks().await, vec![]);

    let err = runner.catch_up().await.unwrap_err().to_string();
    assert!(err.contains(&format!("example:{refused}")), "{err}");
    assert!(!err.contains(&served), "{err}");
    assert_eq!(
        checkpoints::get(&pool, &format!("example:{served}"))
            .await
            .unwrap(),
        42
    );
    // The refused cursor never landed a page, yet has a row: completeness
    // checks walk `checkpoints` and must see it.
    let rows: Vec<(String, i64)> = checkpoints::all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(name, height, _)| (name, height))
        .collect();
    assert_eq!(
        rows,
        vec![
            (format!("example:{refused}"), 0),
            (format!("example:{served}"), 42)
        ]
    );
    // Only the cursor whose pass completed is marked as scanned, through the
    // tip the pass began with.
    assert_eq!(
        marks().await,
        vec![(
            format!("example:{served}"),
            42,
            50,
            "2026-10-14T00:01:02Z".parse().unwrap()
        )]
    );
    // Joined last: a runner that stops at the refusal never makes the second
    // request, and the server would wait for it forever.
    server.join().unwrap();

    test.drop_schema().await;
}
