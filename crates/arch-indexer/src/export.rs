//! Day exports: the pieces a host composes into one day of facts served as a
//! single consistent snapshot (arch-prime-analytics' rewards export,
//! `GET /v1/export/days/{day}`).
//!
//! The host opens one [`snapshot`] transaction and does every read in it, so
//! the body is the state the completeness checks passed on:
//! 1. [`Completeness::load`], then [`day_incomplete`]: the runtime's checks
//!    (below), then each indexer's `day_incomplete` in registration order;
//!    the first reason wins.
//! 2. [`day_facts`]: each indexer's `day_facts`, merged by key.
//! 3. [`day_holdings`]: each indexer's `day_holdings`, merged in
//!    `(wallet, venue, address, side)` order and cut to the cap.
//!
//! The runtime's checks prove the day ending at `end` is scanned:
//! - no replay a migration requested is still walking history;
//! - something was ingested at all (an empty database proves nothing);
//! - every checkpoint has a scan mark (written once a pass reaches the
//!   program's last page) whose block is stamped at or after `end` and whose
//!   checkpoint the current one has not rewound below. A checkpoint alone
//!   cannot say this: it moves only when its program has a transaction, and a
//!   quiet program can go days without one;
//! - no checkpoint is above its mark's indexer tip: after a chain restarts
//!   from genesis, each pass pages nothing and still stamps a fresh mark.
//!
//! They read every row of `replay_requests`, `checkpoints` and `scan_marks`,
//! not only this host's keys, as analytics' export does.

use crate::db::{PgPool, PgTransaction};
use crate::indexer::DynIndexer;
use bigdecimal::BigDecimal;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One `REPEATABLE READ, READ ONLY` transaction: every read of a day export
/// sees the same snapshot.
pub async fn snapshot(pool: &PgPool) -> anyhow::Result<PgTransaction<'static>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow, Serialize)]
pub struct Checkpoint {
    pub name: String,
    pub block_height: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow, Serialize)]
pub struct Scan {
    pub name: String,
    pub checkpoint_height: i64,
    pub tip_height: i64,
    pub tip_ts: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The runtime state a day's completeness is proven from. `checkpoints` and
/// `scans` are also what an export reports under `ingest`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Completeness {
    /// `replay_requests.checkpoint`, ascending.
    pub replays: Vec<String>,
    /// Ascending by name.
    pub checkpoints: Vec<Checkpoint>,
    /// Ascending by name.
    pub scans: Vec<Scan>,
}

impl Completeness {
    pub async fn load(tx: &mut PgTransaction<'_>) -> anyhow::Result<Self> {
        let replays: Vec<String> =
            sqlx::query_scalar("SELECT checkpoint FROM replay_requests ORDER BY checkpoint")
                .fetch_all(&mut **tx)
                .await?;
        let checkpoints: Vec<Checkpoint> =
            sqlx::query_as("SELECT name, block_height, updated_at FROM checkpoints ORDER BY name")
                .fetch_all(&mut **tx)
                .await?;
        let scans: Vec<Scan> = sqlx::query_as(
            "SELECT name, checkpoint_height, tip_height, tip_ts, updated_at
             FROM scan_marks ORDER BY name",
        )
        .fetch_all(&mut **tx)
        .await?;
        Ok(Self {
            replays,
            checkpoints,
            scans,
        })
    }

    /// Why the day ending at `end` cannot be proven complete by the runtime's
    /// checks, if it cannot (module doc).
    pub fn incomplete(&self, end: DateTime<Utc>) -> Option<String> {
        let Self {
            replays,
            checkpoints,
            scans,
        } = self;
        if !replays.is_empty() {
            return Some(format!(
                "day not complete: replay pending for {}",
                replays.join(", ")
            ));
        }
        // Nothing ingested (a fresh or just-truncated database) proves nothing:
        // the loop below would pass every day.
        if checkpoints.is_empty() {
            return Some("day not complete: nothing ingested".into());
        }
        // A mark whose checkpoint row is gone was rewound (a missing row reads as
        // height 0).
        if let Some(s) = scans
            .iter()
            .find(|s| !checkpoints.iter().any(|c| c.name == s.name))
        {
            return Some(format!(
                "day not complete: {} scanned through never",
                s.name
            ));
        }
        for c in checkpoints {
            // A checkpoint rewound below its mark (a truncate-and-replay
            // migration) voids the mark: the history it vouched for is gone.
            let scan = scans
                .iter()
                .find(|s| s.name == c.name && c.block_height >= s.checkpoint_height);
            let Some(s) = scan.filter(|s| s.tip_ts >= end) else {
                let through = scan.map_or("never".into(), |s| {
                    s.tip_ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
                });
                return Some(format!(
                    "day not complete: {} scanned through {through}",
                    c.name
                ));
            };
            // A cursor above the tip belongs to a chain that restarted from
            // genesis: each pass pages nothing yet still stamps a fresh mark.
            // A transaction landing mid-pass also leaves the checkpoint a few
            // blocks past the tip read at its start, until the next pass.
            if c.block_height > s.tip_height {
                return Some(format!(
                    "day not complete: {} checkpoint {} above the indexer tip {}",
                    c.name, c.block_height, s.tip_height
                ));
            }
        }
        None
    }
}

/// The runtime's reason, else the first indexer's (registration order).
pub async fn day_incomplete(
    tx: &mut PgTransaction<'static>,
    indexers: &[Arc<dyn DynIndexer>],
    completeness: &Completeness,
    end: DateTime<Utc>,
) -> anyhow::Result<Option<String>> {
    if let Some(reason) = completeness.incomplete(end) {
        return Ok(Some(reason));
    }
    for ix in indexers {
        if let Some(reason) = ix.day_incomplete(tx, end).await? {
            return Ok(Some(reason));
        }
    }
    Ok(None)
}

/// One indexer's facts of a day, by top-level key of the export body (e.g.
/// `"swaps"`, `"pools"`). Keys are flat and unique across a host's indexers;
/// where the host nests one (analytics' `ingest.share_marks`) is the host's
/// business.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DayFacts {
    pub facts: BTreeMap<&'static str, Value>,
    /// Rows that count toward the day's row cap: every array inserted with
    /// [`Self::insert_rows`].
    pub counted_rows: usize,
}

impl DayFacts {
    /// A day's rows (events, balances, samples): counted toward the cap.
    pub fn insert_rows<T: Serialize>(
        &mut self,
        key: &'static str,
        rows: &[T],
    ) -> anyhow::Result<()> {
        self.insert(key, serde_json::to_value(rows)?)?;
        self.counted_rows += rows.len();
        Ok(())
    }

    /// Reference data the rows point into (pools, banks, vaults, marks): not
    /// counted toward the cap, as analytics never counted it.
    pub fn insert_reference<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> anyhow::Result<()> {
        self.insert(key, serde_json::to_value(value)?)
    }

    /// Fold `other` in; a key both carry is refused (two indexers would
    /// overwrite each other's facts).
    pub fn merge(&mut self, other: DayFacts) -> anyhow::Result<()> {
        for (key, value) in other.facts {
            self.insert(key, value)?;
        }
        self.counted_rows += other.counted_rows;
        Ok(())
    }

    fn insert(&mut self, key: &'static str, value: Value) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.facts.contains_key(key),
            "day fact {key:?} is already set"
        );
        self.facts.insert(key, value);
        Ok(())
    }
}

/// Every indexer's facts of `[start, end)`, merged; `cap` is each query's
/// `LIMIT` (the host's row cap + 1, so an over-cap day is detectable and
/// refused, never truncated).
pub async fn day_facts(
    tx: &mut PgTransaction<'static>,
    indexers: &[Arc<dyn DynIndexer>],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cap: i64,
) -> anyhow::Result<DayFacts> {
    let mut all = DayFacts::default();
    for ix in indexers {
        let facts = ix.day_facts(tx, start, end, cap).await?;
        all.merge(facts)
            .map_err(|err| anyhow::anyhow!("indexer {}: {err}", ix.id()))?;
    }
    Ok(all)
}

/// What a wallet held in one venue over a day: `close` at the day's end and
/// `average` weighted by time (`trunc(Σ amount × seconds / 86 400)`), in
/// atoms of `mint`. `shares_*` carry share atoms where the venue issues
/// shares (a vault).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow, Serialize)]
pub struct Holding {
    pub wallet: String,
    pub venue: String,
    pub address: String,
    pub mint: Option<String>,
    pub side: String,
    pub close: BigDecimal,
    pub average: BigDecimal,
    pub shares_close: Option<BigDecimal>,
    pub shares_average: Option<BigDecimal>,
}

/// Every indexer's holdings of `[start, end)` in `(wallet, venue, address,
/// side)` order, at most `cap` rows. Each indexer returns its own first
/// `cap` rows in that order, so the merged first `cap` are the union's.
///
/// The order is by bytes. It equals the `ORDER BY` of analytics' single query
/// for the lowercase-hex and lowercase-ASCII values these columns hold; a
/// collation could differ only on mixed case or punctuation.
pub async fn day_holdings(
    tx: &mut PgTransaction<'static>,
    indexers: &[Arc<dyn DynIndexer>],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cap: i64,
) -> anyhow::Result<Vec<Holding>> {
    let mut all = Vec::new();
    for ix in indexers {
        all.extend(ix.day_holdings(tx, start, end, cap).await?);
    }
    Ok(merge_holdings(all, cap))
}

fn merge_holdings(mut all: Vec<Holding>, cap: i64) -> Vec<Holding> {
    all.sort_by(|a, b| {
        (&a.wallet, &a.venue, &a.address, &a.side).cmp(&(&b.wallet, &b.venue, &b.address, &b.side))
    });
    all.truncate(usize::try_from(cap).unwrap_or(0));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn completeness(replays: &[&str], checkpoints: &[Checkpoint], scans: &[Scan]) -> Completeness {
        Completeness {
            replays: replays.iter().map(|r| r.to_string()).collect(),
            checkpoints: checkpoints.to_vec(),
            scans: scans.to_vec(),
        }
    }

    #[test]
    fn an_empty_ingest_proves_no_day() {
        let end = at("2026-10-02T08:00:00Z");
        assert_eq!(
            completeness(&[], &[], &[]).incomplete(end).as_deref(),
            Some("day not complete: nothing ingested")
        );
    }

    #[test]
    fn a_checkpoint_above_the_indexer_tip_proves_no_day() {
        let end = at("2026-10-16T08:00:00Z");
        let checkpoints = |block_height| {
            [Checkpoint {
                name: "clamm".into(),
                block_height,
                updated_at: end,
            }]
        };
        let scans = |checkpoint_height| {
            [Scan {
                name: "clamm".into(),
                checkpoint_height,
                tip_height: 2_981_880,
                tip_ts: end,
                updated_at: end,
            }]
        };
        // The mark a pass stamps over the restarted chain.
        assert_eq!(
            completeness(&[], &checkpoints(30_973_047), &scans(30_973_047))
                .incomplete(end)
                .as_deref(),
            Some("day not complete: clamm checkpoint 30973047 above the indexer tip 2981880")
        );
        assert_eq!(
            completeness(&[], &checkpoints(2_981_880), &scans(2_981_880)).incomplete(end),
            None
        );
    }

    #[test]
    fn replays_marks_and_rewinds_each_refuse_the_day() {
        let end = at("2026-10-02T08:00:00Z");
        let checkpoint = |name: &str, block_height| Checkpoint {
            name: name.into(),
            block_height,
            updated_at: end,
        };
        let scan = |name: &str, checkpoint_height, tip_ts| Scan {
            name: name.into(),
            checkpoint_height,
            tip_height: 100,
            tip_ts,
            updated_at: tip_ts,
        };
        let good = [checkpoint("clamm", 10), checkpoint("arch_lend", 20)];
        let marks = [scan("arch_lend", 20, end), scan("clamm", 10, end)];
        assert_eq!(completeness(&[], &good, &marks).incomplete(end), None);
        assert_eq!(
            completeness(&["clamm", "vaults:ab"], &good, &marks)
                .incomplete(end)
                .as_deref(),
            Some("day not complete: replay pending for clamm, vaults:ab")
        );
        // No mark at all.
        assert_eq!(
            completeness(&[], &good, &marks[..1])
                .incomplete(end)
                .as_deref(),
            Some("day not complete: clamm scanned through never")
        );
        // A mark short of the end.
        let short = [
            scan("arch_lend", 20, end),
            scan("clamm", 10, at("2026-10-02T07:59:59Z")),
        ];
        assert_eq!(
            completeness(&[], &good, &short).incomplete(end).as_deref(),
            Some("day not complete: clamm scanned through 2026-10-02T07:59:59Z")
        );
        // The checkpoint rewound below its mark voids it.
        let rewound = [checkpoint("clamm", 9), checkpoint("arch_lend", 20)];
        assert_eq!(
            completeness(&[], &rewound, &marks)
                .incomplete(end)
                .as_deref(),
            Some("day not complete: clamm scanned through never")
        );
        // A mark whose checkpoint row is gone.
        assert_eq!(
            completeness(&[], &good[..1], &marks)
                .incomplete(end)
                .as_deref(),
            Some("day not complete: arch_lend scanned through never")
        );
    }

    #[test]
    fn facts_count_rows_not_reference_data_and_refuse_a_shared_key() {
        let mut clamm = DayFacts::default();
        clamm.insert_rows("swaps", &[1, 2, 3]).unwrap();
        clamm.insert_reference("pools", &["p1"]).unwrap();
        let mut vaults = DayFacts::default();
        vaults.insert_rows("vault_events", &[1]).unwrap();
        vaults.insert_reference("share_marks", &[0; 4]).unwrap();
        let mut all = DayFacts::default();
        all.merge(clamm.clone()).unwrap();
        all.merge(vaults).unwrap();
        assert_eq!(all.counted_rows, 4);
        assert_eq!(
            all.facts.keys().copied().collect::<Vec<_>>(),
            ["pools", "share_marks", "swaps", "vault_events"]
        );
        assert_eq!(all.facts["swaps"], serde_json::json!([1, 2, 3]));
        assert!(all.merge(clamm).is_err());
    }

    #[test]
    fn holdings_merge_in_export_order_and_stop_at_the_cap() {
        let h = |wallet: &str, venue: &str, address: &str, side: &str| Holding {
            wallet: wallet.into(),
            venue: venue.into(),
            address: address.into(),
            mint: None,
            side: side.into(),
            close: 1.into(),
            average: 1.into(),
            shares_close: None,
            shares_average: None,
        };
        let all = vec![
            h("bb", "lending", "b1", "supplied"),
            h("aa", "vault", "v1", "deposited"),
            h("aa", "lending", "b1", "supplied"),
            h("aa", "lending", "b1", "borrowed"),
        ];
        let merged = merge_holdings(all.clone(), 3);
        assert_eq!(
            merged,
            vec![
                h("aa", "lending", "b1", "borrowed"),
                h("aa", "lending", "b1", "supplied"),
                h("aa", "vault", "v1", "deposited"),
            ]
        );
        assert_eq!(merge_holdings(all, 10).len(), 4);
    }
}
