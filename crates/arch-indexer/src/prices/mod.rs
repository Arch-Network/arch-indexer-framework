//! The price spine: `price_candles`, hourly USD closes per mint, owned by the
//! runtime (migration `0002_price_candles`).
//!
//! Sources, in the order an enrich pass writes them:
//!   1. Coinbase Exchange hourly candles — `coinbase:<product-id>` feeds
//!   2. `const:<usd>` feeds (test stables) — single candle at epoch,
//!      forward-filled at read
//!   3. each indexer's [`crate::ProgramIndexer::derive_prices`] — prices it
//!      derives from its own state (a CLAMM pool route against a feed-priced
//!      mint, a vault share at its NAV), given a [`PriceCtx`]
//!
//! Then each indexer's `enrich` stamps USD from the candles. Events keep raw
//! atoms beside their USD, so a spine repair is "NULL the USD columns, run
//! again", without re-decoding.
//!
//! The read side is [`Prices`] and [`Spine`] over series already loaded, with
//! the at-or-before rules [`obs_at`], [`obs_at_or_first`] and [`cum_at`].

pub mod coinbase;
pub mod feeds;

pub use feeds::{
    earliest_activity, parse_price_feeds, FeedSync, PriceFeed, DEFAULT_COINBASE_API_URL,
};

use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

/// What an indexer's `derive_prices` must respect so no two writers fight
/// over one mint's candles (the `(mint, interval, bucket_ts)` key and the
/// resume cursors).
#[derive(Debug, Clone, Default)]
pub struct PriceCtx {
    /// The mints `PRICE_FEEDS` prices, lowercased. Never derive these; they
    /// are the priced side a derivation routes through.
    pub feed_mints: HashSet<String>,
    /// The mints the host's OTHER indexers price themselves (their
    /// [`crate::ProgramIndexer::priced_mints`]), e.g. vault share mints. Never
    /// derive these either.
    pub claimed_mints: HashSet<String>,
}

impl PriceCtx {
    /// Whether `mint` has a feed. Compared lowercased, as `PRICE_FEEDS` keys
    /// are.
    pub fn is_feed_priced(&self, mint: &str) -> bool {
        self.feed_mints.contains(&mint.to_lowercase())
    }

    /// Whether another indexer prices `mint`.
    pub fn is_claimed(&self, mint: &str) -> bool {
        self.claimed_mints.contains(mint)
    }

    /// `feed_mints`, sorted, for binding as a `text[]`.
    pub fn feed_mint_list(&self) -> Vec<String> {
        sorted(&self.feed_mints)
    }

    /// `claimed_mints`, sorted, for binding as a `text[]`.
    pub fn claimed_mint_list(&self) -> Vec<String> {
        sorted(&self.claimed_mints)
    }
}

fn sorted(set: &HashSet<String>) -> Vec<String> {
    let mut v: Vec<String> = set.iter().cloned().collect();
    v.sort();
    v
}

/// Hourly price spine per mint (`(bucket_ts, close_usd)` ascending),
/// forward-filled at read time: what a portfolio leg is valued with.
#[derive(Debug, Clone, Default)]
pub struct Prices(pub HashMap<String, Vec<(DateTime<Utc>, f64)>>);

impl Prices {
    /// The latest close strictly before `at`; see [`obs_at`].
    pub fn at(&self, mint: &str, at: DateTime<Utc>) -> Option<f64> {
        obs_at(self.0.get(mint)?, at)
    }
}

/// The hourly price spine (`price_candles`, `1h` closes) of the mints a
/// ledger involves, forward-filled at read.
#[derive(Debug, Clone, Default)]
pub struct Spine(pub HashMap<String, Vec<(DateTime<Utc>, f64)>>);

impl Spine {
    /// The latest close before `at` and its bucket. The bucket is `None` for
    /// a constant feed, whose one candle sits at epoch and has no age.
    pub fn at(&self, mint: &str, at: DateTime<Utc>) -> Option<(f64, Option<DateTime<Utc>>)> {
        let series = self.0.get(mint)?;
        let idx = series.partition_point(|(t, _)| *t < at);
        let (ts, close) = *series.get(idx.checked_sub(1)?)?;
        Some((close, (ts.timestamp() > 0).then_some(ts)))
    }
}

/// Latest observation strictly before `at` (series ascending by ts). `None`
/// before the first observation — used for level series (price, sqrt_price,
/// rate) that simply don't exist yet before their first sample. `at` is an
/// exclusive bucket-end instant, so a point stamped exactly at the boundary
/// belongs to the next bucket — one convention across every leg.
pub fn obs_at<T: Copy>(series: &[(DateTime<Utc>, T)], at: DateTime<Utc>) -> Option<T> {
    let idx = series.partition_point(|(t, _)| *t < at);
    (idx > 0).then(|| series[idx - 1].1)
}

/// `obs_at`, but instants before the first observation get the FIRST
/// observation instead of `None`. Only correct for series with a known
/// starting anchor and slow drift — an Arch Lend share value starts at
/// exactly 1 and accrues monotonically (a socialized loss aside), so
/// backfilling the first sample bounds the error by the interest accrued
/// before sampling began. Not for prices.
pub fn obs_at_or_first<T: Copy>(series: &[(DateTime<Utc>, T)], at: DateTime<Utc>) -> Option<T> {
    obs_at(series, at).or_else(|| series.first().map(|(_, v)| *v))
}

/// Running total strictly before `at`. `zero` before the first checkpoint —
/// used for cumulative state (balances, shares, liquidity) that is
/// definitionally 0 before the first observed change.
pub fn cum_at<T: Copy + Default>(series: &[(DateTime<Utc>, T)], at: DateTime<Utc>) -> T {
    let idx = series.partition_point(|(t, _)| *t < at);
    if idx == 0 {
        T::default()
    } else {
        series[idx - 1].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(h: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(h * 3600, 0).unwrap()
    }

    #[test]
    fn cum_at_is_zero_before_first_and_step_holds() {
        // `at` is an exclusive bucket-end instant: a point stamped exactly at
        // the boundary belongs to the NEXT bucket.
        let series = vec![(ts(2), 100i128), (ts(5), 250i128)];
        assert_eq!(cum_at(&series, ts(1)), 0); // before first
        assert_eq!(cum_at(&series, ts(2)), 0); // at the boundary → next bucket
        assert_eq!(cum_at(&series, ts(3)), 100); // strictly after
        assert_eq!(cum_at(&series, ts(4)), 100); // holds between
        assert_eq!(cum_at(&series, ts(9)), 250); // holds after last
    }

    #[test]
    fn obs_at_is_none_before_first() {
        let series = vec![(ts(3), 1.5f64), (ts(6), 2.0f64)];
        assert_eq!(obs_at(&series, ts(2)), None);
        assert_eq!(obs_at(&series, ts(3)), None); // boundary is exclusive
        assert_eq!(obs_at(&series, ts(4)), Some(1.5));
        assert_eq!(obs_at(&series, ts(7)), Some(2.0));
    }

    #[test]
    fn obs_at_or_first_backfills_only_before_first() {
        let series = vec![(ts(3), 1.5f64), (ts(6), 2.0f64)];
        assert_eq!(obs_at_or_first(&series, ts(1)), Some(1.5)); // backfilled
        assert_eq!(obs_at_or_first(&series, ts(4)), Some(1.5));
        assert_eq!(obs_at_or_first(&series, ts(7)), Some(2.0));
        assert_eq!(obs_at_or_first::<f64>(&[], ts(1)), None); // empty stays None
    }

    #[test]
    fn prices_forward_fill_and_know_only_their_mints() {
        let prices = Prices(HashMap::from([(
            "btc".to_string(),
            vec![(ts(1), 100.0), (ts(3), 110.0)],
        )]));
        assert_eq!(prices.at("btc", ts(1)), None);
        assert_eq!(prices.at("btc", ts(2)), Some(100.0));
        assert_eq!(prices.at("btc", ts(9)), Some(110.0));
        assert_eq!(prices.at("eth", ts(9)), None);
    }

    #[test]
    fn a_spine_dates_its_close_except_a_constant_feeds_epoch_candle() {
        let epoch = DateTime::from_timestamp(0, 0).unwrap();
        let spine = Spine(HashMap::from([
            ("usd".to_string(), vec![(epoch, 1.0)]),
            ("btc".to_string(), vec![(ts(2), 100.0), (ts(4), 120.0)]),
        ]));
        assert_eq!(spine.at("usd", ts(100)), Some((1.0, None)));
        assert_eq!(spine.at("btc", ts(2)), None);
        assert_eq!(spine.at("btc", ts(3)), Some((100.0, Some(ts(2)))));
        assert_eq!(spine.at("btc", ts(5)), Some((120.0, Some(ts(4)))));
        assert_eq!(spine.at("eth", ts(5)), None);
    }

    #[test]
    fn a_price_ctx_matches_feeds_case_insensitively_and_claims_exactly() {
        let ctx = PriceCtx {
            feed_mints: HashSet::from(["ab".to_string(), "01".to_string()]),
            claimed_mints: HashSet::from(["cd".to_string()]),
        };
        assert!(ctx.is_feed_priced("AB"));
        assert!(!ctx.is_feed_priced("cd"));
        assert!(ctx.is_claimed("cd"));
        assert!(!ctx.is_claimed("ab"));
        assert_eq!(
            ctx.feed_mint_list(),
            vec!["01".to_string(), "ab".to_string()]
        );
        assert_eq!(ctx.claimed_mint_list(), vec!["cd".to_string()]);
    }
}
