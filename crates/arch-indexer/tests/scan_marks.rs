//! Real-Postgres check of `checkpoints::mark_scanned`.
mod common;

use arch_indexer::{checkpoints, db};
use chrono::{DateTime, Utc};
use common::TestDb;

#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn a_scan_mark_takes_the_current_checkpoint_and_replaces_the_last_mark() {
    let test = TestDb::new("scan_marks_test").await;
    let pool = test.pool.clone();
    db::migrate(&pool, db::RUNTIME_NAMESPACE, &db::RUNTIME_MIGRATOR)
        .await
        .unwrap();

    let marks = || async {
        sqlx::query_as::<_, (String, i64, i64, DateTime<Utc>)>(
            "SELECT name, checkpoint_height, tip_height, tip_ts FROM scan_marks ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    };
    let first: DateTime<Utc> = "2026-10-13T00:00:05Z".parse().unwrap();
    let second: DateTime<Utc> = "2026-10-14T00:00:07Z".parse().unwrap();

    checkpoints::set(&pool, "clamm", 10).await.unwrap();
    checkpoints::mark_scanned(&pool, "clamm", 100, first)
        .await
        .unwrap();
    // A cursor with no checkpoint row has nothing to vouch for.
    checkpoints::mark_scanned(&pool, "arch_lend", 100, first)
        .await
        .unwrap();
    assert_eq!(marks().await, vec![("clamm".into(), 10, 100, first)]);

    checkpoints::set(&pool, "clamm", 20).await.unwrap();
    checkpoints::mark_scanned(&pool, "clamm", 200, second)
        .await
        .unwrap();
    assert_eq!(marks().await, vec![("clamm".into(), 20, 200, second)]);

    // Checkpoints only move forward.
    checkpoints::set(&pool, "clamm", 5).await.unwrap();
    assert_eq!(checkpoints::get(&pool, "clamm").await.unwrap(), 20);

    test.drop_schema().await;
}
