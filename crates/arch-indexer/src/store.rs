//! The runtime's own writes: mint decimals, unknown-instruction markers and
//! the cross-indexer delete of a rolled-back transaction.

use crate::db::{PgPool, PgTransaction};
use crate::event::UnknownInstruction;
use crate::indexer::DynIndexer;
use std::sync::Arc;

/// Mint decimals observed on token transfers during ingestion — the atoms→UI
/// conversion source for anything that values a mint.
pub async fn upsert_mint_decimals(pool: &PgPool, pairs: &[(String, i32)]) -> anyhow::Result<()> {
    for (mint, decimals) in pairs {
        sqlx::query(
            "INSERT INTO mint_decimals (mint, decimals) VALUES ($1, $2)
             ON CONFLICT (mint) DO UPDATE SET decimals = EXCLUDED.decimals",
        )
        .bind(mint)
        .bind(decimals)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn insert_unknown(
    tx: &mut PgTransaction<'_>,
    e: &UnknownInstruction,
) -> anyhow::Result<u64> {
    let r = sqlx::query(
        "INSERT INTO decoder_unknowns (decoder, discriminator, txid, ix_path, block_height, ts)
         VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
    )
    .bind(e.decoder)
    .bind(&e.discriminator)
    .bind(&e.txid)
    .bind(&e.ix_path)
    .bind(e.block_height)
    .bind(e.ts)
    .execute(&mut **tx)
    .await?;
    Ok(r.rows_affected())
}

/// Drop every row derived from a rolled-back transaction — each indexer's
/// (`ProgramIndexer::delete_tx`) and its unknown markers — in one database
/// transaction, so aggregates computed at query time self-correct. Returns
/// rows deleted.
///
/// **Not wired into the ingest loop, and cannot be**: the export flags
/// `rolled_back` at read time (and ingest skips those), but it exposes no
/// "changed since cursor" feed, so nothing tells us a previously-ingested txid
/// has disappeared. It is the manual reconciliation tool for the residual case
/// that no confirmation-depth lag can prevent: a Bitcoin-anchor rollback can
/// revert a transaction that was already final on Arch. Run it with the
/// host's `delete-tx <txid>` mode.
pub async fn delete_tx(
    pool: &PgPool,
    indexers: &[Arc<dyn DynIndexer>],
    txid: &str,
) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let mut deleted = 0;
    for ix in indexers {
        deleted += ix.delete_tx(&mut tx, txid).await?;
    }
    deleted += sqlx::query("DELETE FROM decoder_unknowns WHERE txid = $1")
        .bind(txid)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(deleted)
}
