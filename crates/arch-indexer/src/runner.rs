//! Backfill and tail are the same loop with different cursors: page a
//! program's transactions from its checkpoint, decode, persist (idempotently),
//! advance the checkpoint. Run once = backfill; run forever = tail.
//!
//! Ordering guarantee: a page's events are committed in ONE database
//! transaction and the checkpoint is written only after that commit returns.
//! A crash between the two re-ingests the page, which the indexer's
//! idempotent inserts absorb; a failed insert (or a failed fetch of anything
//! the page needs) returns before the checkpoint moves, so the checkpoint
//! never advances past data that was not persisted.

use crate::db::PgPool;
use crate::event::IndexerEvent;
use crate::explorer::ExplorerClient;
use crate::indexer::{Ctx, DynIndexer, ProgramIndexer};
use crate::{checkpoints, store};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub struct Runner {
    pub pool: PgPool,
    pub client: ExplorerClient,
    pub indexers: Vec<Arc<dyn DynIndexer>>,
    pub page_limit: u32,
    /// Ingest only up to `tip - confirmation_depth`. 0 (the default) ingests to
    /// the tip.
    ///
    /// Arch blocks are not final on inclusion: pending blocks can be cleared by
    /// consensus. The export carries `rolled_back` and the runner skips a
    /// transaction flagged at read time, but forward-only pagination never
    /// re-emits a row whose flag flipped after the cursor passed it, so a
    /// transaction ingested and reverted later leaves a phantom row that
    /// nothing removes.
    ///
    /// A depth lag shrinks that window. It does NOT close the residual case: a
    /// Bitcoin-anchor rollback can revert a transaction that was already final
    /// on Arch, and only reconciliation (`store::delete_tx`) or a full replay
    /// heals that.
    pub confirmation_depth: i64,
    /// Ingest nothing above this height (inclusive bound), for a bounded
    /// backfill or replay. `None` = to the tip. Clamped together with the
    /// confirmation depth; the checkpoint never passes it, so a later run
    /// without the bound resumes from exactly here.
    pub end_height: Option<i64>,
}

/// Highest block height it is safe to ingest, or `None` when neither gate is
/// on. Kept pure so the arithmetic is testable without an indexer.
fn safe_tip(tip: i64, confirmation_depth: i64, end_height: Option<i64>) -> Option<i64> {
    let depth = (confirmation_depth > 0).then(|| tip - confirmation_depth);
    match (depth, end_height) {
        (Some(d), Some(e)) => Some(d.min(e)),
        (Some(d), None) => Some(d),
        (None, e) => e,
    }
}

/// The height a pass's scan mark must stay at or below: the safe tip, when a
/// gate holds ingestion below the tip. `None` = the tip block itself.
fn mark_below(tip: i64, safe_tip: Option<i64>) -> Option<i64> {
    safe_tip.filter(|st| *st < tip)
}

/// A checkpoint must never advance past what was actually ingested, so it is
/// clamped to the safe tip when a gate is on. `checkpoints::set` max-merges,
/// so a clamp below the stored value leaves it untouched rather than rewinding.
fn checkpoint_height(complete_through_height: i64, safe_tip: Option<i64>) -> i64 {
    match safe_tip {
        Some(st) => complete_through_height.min(st),
        None => complete_through_height,
    }
}

/// What one catch-up pass did for one cursor. Logged as one structured line
/// per pass so a wedge (unknowns climbing, logs missing, transfers absent)
/// is visible without reading raw transactions.
#[derive(Debug, Default)]
struct PassStats {
    pages: u64,
    scanned: u64,
    recognized: u64,
    deferred: u64,
    skipped_failed: u64,
    skipped_rolled_back: u64,
    logs_fetched: u64,
    logs_unavailable: u64,
    decode_failed: u64,
    unknown: u64,
    written: u64,
    by_kind: BTreeMap<&'static str, u64>,
}

impl PassStats {
    fn count(&mut self, ev: &impl IndexerEvent) {
        let kind = if ev.as_unknown().is_some() {
            self.unknown += 1;
            "unknown"
        } else {
            ev.kind()
        };
        *self.by_kind.entry(kind).or_default() += 1;
    }
}

impl Runner {
    pub fn ctx(&self) -> Ctx {
        Ctx {
            pool: self.pool.clone(),
            client: self.client.clone(),
        }
    }

    /// One catch-up pass for every cursor of every indexer. Returns rows
    /// written. A cursor that fails leaves the others to run, and the pass
    /// then fails naming it.
    pub async fn catch_up(&self) -> anyhow::Result<u64> {
        let cursors: Vec<(&Arc<dyn DynIndexer>, String, String)> = self
            .indexers
            .iter()
            .flat_map(|ix| {
                ix.program_ids()
                    .into_iter()
                    .map(move |program| (ix, ix.checkpoint_key(&program), program))
            })
            .collect();
        // Every pass, not only at boot: a TRUNCATE while the service runs (a
        // chain reset) must not leave a cursor invisible to completeness
        // checks until its first page lands.
        let names: Vec<String> = cursors.iter().map(|(_, key, _)| key.clone()).collect();
        checkpoints::register(&self.pool, &names).await?;
        // Resolved once per pass, before any decoding. When a gate is on, a
        // failure to read the tip aborts the pass rather than silently
        // ingesting unconfirmed blocks — the loop retries on the next tick.
        // Without one the tip only dates the scan marks, so the pass goes on
        // without them.
        let gated = self.confirmation_depth != 0 || self.end_height.is_some();
        let tip = match self.client.latest_block(None).await {
            Ok(tip) => Some(tip),
            Err(err) if gated => return Err(err),
            Err(err) => {
                tracing::warn!(%err, "indexer tip unreadable; no scan marks this pass");
                None
            }
        };
        let safe_tip = tip
            .as_ref()
            .and_then(|t| safe_tip(t.height, self.confirmation_depth, self.end_height));
        // The block each completed pass marks as scanned through: the tip, or
        // the newest block at or below the safe tip when a gate holds
        // ingestion below it. Read before any cursor pages, so a pass that
        // reaches the program's last page has seen every transaction of it
        // stamped before this block's time.
        //
        // Trusts the indexer to hold every block below its newest row. A hole
        // it fills later lets a pass mark past transactions it never saw.
        let below = tip.as_ref().and_then(|t| mark_below(t.height, safe_tip));
        let mark = match below {
            None => tip,
            Some(height) => self
                .client
                .latest_block(Some(height))
                .await
                .inspect_err(|err| {
                    tracing::warn!(%err, height, "mark block unreadable; no scan marks this pass")
                })
                .ok(),
        };

        let mut total = 0u64;
        let mut failed = Vec::new();
        for (ix, name, program) in &cursors {
            let pass = async {
                let written = ix.catch_up_program(self, program, safe_tip).await?;
                if let Some(mark) = &mark {
                    checkpoints::mark_scanned(&self.pool, name, mark.height, mark.timestamp)
                        .await?;
                }
                anyhow::Ok(written)
            };
            match pass.await {
                Ok(written) => total += written,
                Err(err) => {
                    tracing::error!(checkpoint = %name, %err, "catch-up failed");
                    failed.push(name.clone());
                }
            }
        }
        anyhow::ensure!(
            failed.is_empty(),
            "catch_up failed for {}",
            failed.join(", ")
        );
        Ok(total)
    }

    /// One cursor's pass, typed by its indexer; reached through
    /// [`DynIndexer::catch_up_program`].
    pub(crate) async fn catch_up_typed<I: ProgramIndexer>(
        &self,
        indexer: &I,
        program_id: &str,
        safe_tip: Option<i64>,
    ) -> anyhow::Result<u64> {
        let checkpoint = indexer.checkpoint_key(program_id);
        let name = checkpoint.as_str();
        let revision = indexer.data_revision();
        let mut stats = PassStats::default();
        // Pagination contract: from_height stays FIXED while a cursor is in
        // play (the cursor encodes progress); the checkpoint still advances
        // per page so an interrupted run resumes safely. A requested replay
        // walks from genesis and keeps that same progress on the request
        // instead, so the max-merged checkpoint stays where it is and an
        // interrupted replay resumes where it stopped.
        let replay = checkpoints::replay_from(&self.pool, name, revision).await?;
        let from_height = match replay {
            Some(height) => {
                tracing::info!(
                    checkpoint = name,
                    from_height = height,
                    "replaying requested history"
                );
                height
            }
            None => checkpoints::get(&self.pool, name).await?,
        };
        let mut last_height;
        let mut cursor: Option<String> = None;
        loop {
            let mut page = self
                .client
                .export_page(program_id, from_height, cursor.as_deref(), self.page_limit)
                .await?;
            stats.pages += 1;

            let mut events: Vec<I::Event> = Vec::new();
            let mut decimals: Vec<(String, i32)> = Vec::new();
            for tx in &mut page.transactions {
                stats.scanned += 1;
                // Above the safe tip: not confirmed deeply enough to trust.
                // Skipped, not dropped — the checkpoint stays below it, so a
                // later pass ingests it once it has aged in.
                if safe_tip.is_some_and(|st| tx.block_height > st) {
                    stats.deferred += 1;
                    continue;
                }
                // Failed or rolled back: the trace (and any event emitted
                // before the failure) is on the ledger, the effects are not.
                if !tx.effects_happened() {
                    if tx.rolled_back {
                        stats.skipped_rolled_back += 1;
                    } else {
                        stats.skipped_failed += 1;
                    }
                    continue;
                }
                if !tx.touches_program(program_id) {
                    continue;
                }
                stats.recognized += 1;
                // An event-in-logs program whose export gave no logs: fetch
                // them. A transport error aborts the page (retried next
                // tick, checkpoint untouched); an indexer that has no row
                // is recorded and the decoder emits its markers.
                if indexer.needs_logs() && tx.logs.is_empty() {
                    match self.client.transaction_logs(&tx.txid).await? {
                        Some(logs) if !logs.is_empty() => {
                            stats.logs_fetched += 1;
                            tx.logs = logs;
                        }
                        _ => stats.logs_unavailable += 1,
                    }
                }
                match indexer.decode(program_id, tx) {
                    Ok(evs) => {
                        for ev in &evs {
                            stats.count(ev);
                        }
                        events.extend(evs)
                    }
                    // Never stall ingestion on one bad tx: log loudly, skip;
                    // replay after a decoder fix picks it up (idempotent inserts).
                    Err(err) => {
                        stats.decode_failed += 1;
                        tracing::error!(txid = %tx.txid, checkpoint = name, %err, "decode failed")
                    }
                }
                for t in &tx.token_transfers {
                    decimals.push((t.mint.clone(), t.decimals));
                }
            }
            decimals.sort();
            decimals.dedup();
            if !decimals.is_empty() {
                store::upsert_mint_decimals(&self.pool, &decimals).await?;
            }
            if !events.is_empty() {
                stats.written += persist_page(&self.pool, indexer, events).await?;
            }
            last_height = checkpoint_height(page.complete_through_height, safe_tip);
            checkpoints::set(&self.pool, name, last_height).await?;
            if replay.is_some() {
                checkpoints::replay_progress(&self.pool, name, revision, last_height).await?;
            }

            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break, // caught up to tip for this cursor
            }
        }
        // A bounded run (INGEST_END_HEIGHT) stops short of the tip; the
        // request waits for a run that reaches it.
        if replay.is_some() && self.end_height.is_none() {
            checkpoints::replay_done(&self.pool, name, revision).await?;
            tracing::info!(checkpoint = name, "replay from genesis complete");
        }
        if stats.recognized > 0 || stats.skipped_failed + stats.skipped_rolled_back > 0 {
            tracing::info!(
                checkpoint = name,
                height = last_height,
                pages = stats.pages,
                scanned = stats.scanned,
                recognized = stats.recognized,
                written = stats.written,
                deferred = stats.deferred,
                skipped_failed = stats.skipped_failed,
                skipped_rolled_back = stats.skipped_rolled_back,
                logs_fetched = stats.logs_fetched,
                logs_unavailable = stats.logs_unavailable,
                decode_failed = stats.decode_failed,
                unknown = stats.unknown,
                by_kind = ?stats.by_kind,
                "caught up"
            );
        } else {
            tracing::debug!(checkpoint = name, height = last_height, "caught up");
        }
        Ok(stats.written)
    }

    /// Tail mode: catch up, run every indexer's `on_activity`, sleep, repeat.
    /// Stream-gap healing is implicit — the checkpoint never advances past
    /// what was actually ingested.
    pub async fn run_forever(&self, interval: Duration) -> anyhow::Result<()> {
        let ctx = self.ctx();
        loop {
            match self.catch_up().await {
                Ok(n) if n > 0 => tracing::info!(rows = n, "ingested"),
                Ok(_) => {}
                Err(err) => tracing::error!(%err, "catch_up failed; retrying next tick"),
            }
            // Follows a failed pass too: the indexers that did not fail still
            // wrote their events.
            for ix in &self.indexers {
                match ix.on_activity(&ctx).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(indexer = ix.id(), refreshed = n, "on_activity")
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(indexer = ix.id(), %err, "on_activity failed"),
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// One page's events in one transaction: unknown markers to
/// `decoder_unknowns`, the rest to the indexer's `persist`.
async fn persist_page<I: ProgramIndexer>(
    pool: &PgPool,
    indexer: &I,
    events: Vec<I::Event>,
) -> anyhow::Result<u64> {
    let (unknowns, events): (Vec<I::Event>, Vec<I::Event>) =
        events.into_iter().partition(|e| e.as_unknown().is_some());
    let mut tx = pool.begin().await?;
    let mut written = 0u64;
    for marker in unknowns.iter().filter_map(IndexerEvent::as_unknown) {
        written += store::insert_unknown(&mut tx, marker).await?;
    }
    if !events.is_empty() {
        written += indexer.persist(&mut tx, &events).await?;
    }
    tx.commit().await?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_zero_leaves_ingestion_at_the_tip() {
        assert_eq!(safe_tip(100, 0, None), None);
        // No gate → the checkpoint follows the export endpoint exactly.
        assert_eq!(checkpoint_height(100, None), 100);
    }

    #[test]
    fn depth_holds_the_checkpoint_below_the_unconfirmed_tip() {
        assert_eq!(safe_tip(100, 10, None), Some(90));
        // A page complete past the safe tip must not advance the checkpoint
        // past it, or the deferred txs would never be ingested.
        assert_eq!(checkpoint_height(100, Some(90)), 90);
        // A page that hasn't reached the safe tip is unaffected.
        assert_eq!(checkpoint_height(80, Some(90)), 80);
    }

    #[test]
    fn depth_beyond_the_tip_does_not_ingest_backwards() {
        // Early in a chain's life the depth can exceed the tip. The result is
        // negative, so nothing is ingested — and checkpoints::set max-merges,
        // so the stored checkpoint never rewinds.
        assert_eq!(safe_tip(5, 10, None), Some(-5));
        assert_eq!(checkpoint_height(5, Some(-5)), -5);
    }

    /// A bounded run stops at its end height and the checkpoint stops there
    /// too, so an unbounded run afterwards resumes exactly at the bound; the
    /// tighter of the two gates wins when both are on.
    #[test]
    fn end_height_bounds_the_run_and_the_checkpoint() {
        assert_eq!(safe_tip(100, 0, Some(50)), Some(50));
        assert_eq!(safe_tip(100, 10, Some(50)), Some(50));
        assert_eq!(safe_tip(100, 60, Some(50)), Some(40));
        assert_eq!(checkpoint_height(100, Some(50)), 50);
    }

    /// A scan mark claims every transaction before its block's time, so it
    /// is never a block the pass deferred: the tip when nothing holds
    /// ingestion below it, else the newest block at or below the safe tip.
    #[test]
    fn the_scan_mark_never_passes_the_safe_tip() {
        let mark = |tip, depth, end| mark_below(tip, safe_tip(tip, depth, end));
        assert_eq!(mark(100, 0, None), None);
        assert_eq!(mark(100, 10, None), Some(90));
        assert_eq!(mark(100, 0, Some(50)), Some(50));
        assert_eq!(mark(100, 0, Some(100)), None);
        // A bound past the tip ingests to the tip, so the tip is the mark.
        assert_eq!(mark(100, 0, Some(150)), None);
        assert_eq!(mark(100, 10, Some(150)), Some(90));
    }
}
