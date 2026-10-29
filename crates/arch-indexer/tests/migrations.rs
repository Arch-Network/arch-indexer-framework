//! Real-Postgres checks of the namespaced migration runner.
mod common;

use arch_indexer::{build_info, db, Host};
use common::{config, ExampleIndexer, OtherIndexer, TestDb};

fn host(url: &str) -> Host {
    Host::new(
        build_info!(),
        config("http://127.0.0.1:1".into(), url.into()),
        vec![
            Box::new(ExampleIndexer::new(vec!["ab".repeat(32)])),
            Box::new(OtherIndexer),
        ],
    )
    .unwrap()
}

/// Two indexers whose migration sets both start at version 1 share one
/// database: each set is recorded under its own namespace, after the
/// runtime's, and a second boot applies nothing.
#[tokio::test]
#[ignore = "requires PostgreSQL via ARCH_INDEXER_TEST_DATABASE_URL"]
async fn indexers_number_their_migrations_independently() {
    let test = TestDb::new("migrations_test").await;
    let pool = test.pool.clone();
    // An existing analytics database already has the runtime's tables, with
    // data: the runtime adopts them as they stand.
    sqlx::raw_sql(
        "CREATE TABLE checkpoints (
             name text PRIMARY KEY,
             block_height bigint NOT NULL DEFAULT 0,
             updated_at timestamptz NOT NULL DEFAULT now());
         INSERT INTO checkpoints (name, block_height) VALUES ('clamm', 77);",
    )
    .execute(&pool)
    .await
    .unwrap();

    let host = host(&test.url);
    host.migrate(&pool).await.unwrap();
    host.migrate(&pool).await.unwrap();

    let applied: Vec<(String, i64)> =
        sqlx::query_as("SELECT namespace, version FROM program_indexer_migrations ORDER BY 1, 2")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        applied,
        vec![
            ("example".into(), 1),
            ("other".into(), 1),
            ("other".into(), 2),
            ("runtime".into(), 1),
        ]
    );
    assert_eq!(
        db::schema_versions(&pool).await.unwrap(),
        vec![
            ("example".into(), 1),
            ("other".into(), 2),
            ("runtime".into(), 1)
        ]
    );
    let seeded: i64 = sqlx::query_scalar("SELECT count(*) FROM other_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(seeded, 1, "a migration runs exactly once");
    let kept: i64 = sqlx::query_scalar("SELECT block_height FROM checkpoints WHERE name = 'clamm'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(kept, 77);
    for table in [
        "scan_marks",
        "replay_requests",
        "mint_decimals",
        "decoder_unknowns",
        "example_transfers",
    ] {
        sqlx::query(&format!("SELECT count(*) FROM {table}"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"));
    }

    // An applied migration edited afterwards is refused, naming it.
    sqlx::query(
        "UPDATE program_indexer_migrations SET checksum = '\\x00'
         WHERE namespace = 'other' AND version = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    let err = host.migrate(&pool).await.unwrap_err();
    assert!(format!("{err:#}").contains("other/1"), "{err:#}");

    // Restored, it passes again; an applied migration this build no longer
    // has is refused too.
    sqlx::query(
        "UPDATE program_indexer_migrations SET checksum = $1
         WHERE namespace = 'other' AND version = 1",
    )
    .bind(other_v1_checksum())
    .execute(&pool)
    .await
    .unwrap();
    host.migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO program_indexer_migrations
           (namespace, version, description, checksum, execution_time_ms)
         VALUES ('example', 99, 'from a later build', '\\x00', 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let err = host.migrate(&pool).await.unwrap_err();
    assert!(format!("{err:#}").contains("example/99"), "{err:#}");

    test.drop_schema().await;
}

fn other_v1_checksum() -> Vec<u8> {
    use arch_indexer::ProgramIndexer;
    OtherIndexer
        .migrator()
        .iter()
        .find(|m| m.version == 1)
        .unwrap()
        .checksum
        .to_vec()
}
