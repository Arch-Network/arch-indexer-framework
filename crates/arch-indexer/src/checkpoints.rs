//! Per-cursor ingestion state: `checkpoints`, `scan_marks` and
//! `replay_requests`. A cursor is a [`crate::ProgramIndexer::checkpoint_key`],
//! one per (indexer, program id).

use crate::db::PgPool;

/// The cursor's block height; 0 when it has no row.
pub async fn get(pool: &PgPool, name: &str) -> anyhow::Result<i64> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT block_height FROM checkpoints WHERE name = $1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0).unwrap_or(0))
}

/// Max-merge: checkpoints only move forward. The export endpoint can report
/// `complete_through_height` below our current checkpoint when a single block
/// height straddles pages — that means "nothing newly complete", not "rewind".
pub async fn set(pool: &PgPool, name: &str, block_height: i64) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO checkpoints (name, block_height, updated_at)
         VALUES ($1, $2, now())
         ON CONFLICT (name) DO UPDATE
           SET block_height = GREATEST(checkpoints.block_height, EXCLUDED.block_height),
               updated_at = now()",
    )
    .bind(name)
    .bind(block_height)
    .execute(pool)
    .await?;
    Ok(())
}

/// Give every configured cursor a row before its pass pages, at the height 0
/// that `get` already reads a missing row as, so ingest is unchanged. A cursor
/// whose first page never lands then shows (at 0, aging) instead of not at
/// all. DO NOTHING, not `set`: bumping updated_at would hide a wedged cursor
/// from the age checks.
pub async fn register(pool: &PgPool, names: &[String]) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO checkpoints (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING",
    )
    .bind(names)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record that a pass of this cursor, begun with indexer block `tip_height`
/// (stamped `tip_ts`) as its safe tip, reached the program's last page and
/// committed: every transaction of the program stamped before `tip_ts` is
/// ingested. Reads the checkpoint as it stands now, so a later rewind of it
/// voids the mark.
pub async fn mark_scanned(
    pool: &PgPool,
    name: &str,
    tip_height: i64,
    tip_ts: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO scan_marks (name, checkpoint_height, tip_height, tip_ts, updated_at)
         SELECT name, block_height, $2, $3, now() FROM checkpoints WHERE name = $1
         ON CONFLICT (name) DO UPDATE
           SET checkpoint_height = EXCLUDED.checkpoint_height,
               tip_height = EXCLUDED.tip_height,
               tip_ts = EXCLUDED.tip_ts,
               updated_at = now()",
    )
    .bind(name)
    .bind(tip_height)
    .bind(tip_ts)
    .execute(pool)
    .await?;
    Ok(())
}

/// Where a replay requested for this cursor resumes, if one stands for
/// `data_revision` (the owning indexer's) or an earlier one: 0 until a page of
/// it lands. The checkpoint row is left alone, and a request for a later
/// revision is left for the build that has its decoder: this one, still
/// running through that deploy, must neither walk history with the decoder
/// the replay replaces nor retire the request.
pub async fn replay_from(
    pool: &PgPool,
    name: &str,
    data_revision: u32,
) -> anyhow::Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT through_height FROM replay_requests WHERE checkpoint = $1 AND data_revision <= $2",
    )
    .bind(name)
    .bind(data_revision as i32)
    .fetch_optional(pool)
    .await?)
}

/// How far the replay has walked, max-merged as `set` merges the checkpoint.
pub async fn replay_progress(
    pool: &PgPool,
    name: &str,
    data_revision: u32,
    block_height: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE replay_requests SET through_height = GREATEST(through_height, $3)
         WHERE checkpoint = $1 AND data_revision <= $2",
    )
    .bind(name)
    .bind(data_revision as i32)
    .bind(block_height)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn replay_done(pool: &PgPool, name: &str, data_revision: u32) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM replay_requests WHERE checkpoint = $1 AND data_revision <= $2")
        .bind(name)
        .bind(data_revision as i32)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn all(
    pool: &PgPool,
) -> anyhow::Result<Vec<(String, i64, chrono::DateTime<chrono::Utc>)>> {
    let rows =
        sqlx::query_as("SELECT name, block_height, updated_at FROM checkpoints ORDER BY name")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}
