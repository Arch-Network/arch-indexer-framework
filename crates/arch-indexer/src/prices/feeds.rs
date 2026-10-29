//! The feed-priced part of the spine: `PRICE_FEEDS` (Coinbase hourly candles
//! and constants), written by the runtime before any indexer derives prices.

use super::coinbase;
use crate::db::PgPool;
use crate::indexer::DynIndexer;
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// Coinbase Exchange's public API, the default `COINBASE_API_URL`.
pub const DEFAULT_COINBASE_API_URL: &str = "https://api.exchange.coinbase.com";

/// Coinbase serves at most 300 candles per request; ten days of hourly
/// buckets (241, both ends inclusive) stays under that.
const COINBASE_CHUNK_DAYS: i64 = 10;
/// Cold-start lookback when there are no events yet to anchor history.
const DEFAULT_LOOKBACK_DAYS: i64 = 7;

/// How a mint gets its USD price (`PRICE_FEEDS` env, JSON object):
///   `{"<mint_hex>": "coinbase:BTC-USD", "<mint_hex>": "const:1.0"}`
/// A mint absent from the map has no feed; an indexer may price it from its
/// own state in `derive_prices` (a CLAMM pool route, a vault's NAV).
#[derive(Debug, Clone, PartialEq)]
pub enum PriceFeed {
    /// Coinbase Exchange product id (`BTC-USD`): public hourly candles.
    Coinbase(String),
    Const(f64),
}

/// `PRICE_FEEDS`, keyed by lowercased mint.
pub fn parse_price_feeds(raw: &str) -> anyhow::Result<BTreeMap<String, PriceFeed>> {
    let map: BTreeMap<String, String> =
        serde_json::from_str(raw).context("PRICE_FEEDS must be a JSON object of mint -> feed")?;
    map.into_iter()
        .map(|(mint, spec)| {
            let feed = if let Some(product_id) = spec.strip_prefix("coinbase:") {
                anyhow::ensure!(
                    !product_id.is_empty(),
                    "coinbase feed needs a product id, e.g. coinbase:BTC-USD"
                );
                PriceFeed::Coinbase(product_id.to_string())
            } else if let Some(value) = spec.strip_prefix("const:") {
                PriceFeed::Const(value.parse().context("const feed value must be a number")?)
            } else {
                anyhow::bail!(
                    "unknown feed spec '{spec}' (expected coinbase:<product-id> or const:<usd>; \
                     pyth: feeds were retired when Pyth Benchmarks withdrew its TradingView shim)"
                );
            };
            Ok((mint.to_lowercase(), feed))
        })
        .collect()
}

/// Writes the feed-priced mints' candles. Built by the host from
/// [`crate::Config`]; [`crate::sampler::Enricher`] runs it first in every pass.
#[derive(Clone)]
pub struct FeedSync {
    pub feeds: Arc<BTreeMap<String, PriceFeed>>,
    pub coinbase_api_url: String,
    pub http: reqwest::Client,
}

impl FeedSync {
    pub fn new(feeds: BTreeMap<String, PriceFeed>, coinbase_api_url: String) -> Self {
        // No outbound call may hang forever: a stalled feed must not
        // wedge the enrich loop. Connect + total timeout on every request.
        // Coinbase rejects any request without a User-Agent header (400
        // "User-Agent header is required"); reqwest sends none by default.
        let http = reqwest::Client::builder()
            .user_agent(concat!("arch-indexer/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("build price feed http client");
        Self {
            feeds: Arc::new(feeds),
            coinbase_api_url,
            http,
        }
    }

    pub fn from_config(config: &crate::Config) -> Self {
        Self::new(config.price_feeds.clone(), config.coinbase_api_url.clone())
    }

    /// The mints `PRICE_FEEDS` prices, lowercased.
    pub fn feed_mints(&self) -> HashSet<String> {
        self.feeds.keys().cloned().collect()
    }

    /// Every Coinbase feed, each mint on its own: one feed failing is logged
    /// and must not starve the others. `indexers` anchor a cold start
    /// ([`earliest_activity`]).
    pub async fn sync_coinbase(
        &self,
        pool: &PgPool,
        indexers: &[Arc<dyn DynIndexer>],
    ) -> anyhow::Result<()> {
        for (mint, feed) in self.feeds.iter() {
            let PriceFeed::Coinbase(product_id) = feed else {
                continue;
            };
            if let Err(err) = self
                .sync_one_coinbase_mint(pool, indexers, mint, product_id)
                .await
            {
                tracing::error!(mint, product_id, %err, "coinbase sync failed for mint");
            }
        }
        Ok(())
    }

    async fn sync_one_coinbase_mint(
        &self,
        pool: &PgPool,
        indexers: &[Arc<dyn DynIndexer>],
        mint: &str,
        product_id: &str,
    ) -> anyhow::Result<()> {
        // Resume from the newest FEED candle whichever feed wrote it: the rows
        // Pyth produced before its shim was withdrawn (2026-08-26) stay as
        // published history, and Coinbase takes over from that bucket on.
        let last: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"SELECT max(bucket_ts) FROM price_candles
               WHERE mint = $1 AND "interval" = '1h' AND source IN ('pyth', 'coinbase')"#,
        )
        .bind(mint)
        .fetch_one(pool)
        .await?;

        // Resume from `last` INCLUSIVE: the most recent candle was written
        // mid-hour and must be refreshed until its hour closes.
        let start = match last {
            Some(t) => t,
            None => earliest_activity(pool, indexers)
                .await?
                .map(|t| t - Duration::days(1)) // margin so the first event has a candle behind it
                .unwrap_or_else(|| Utc::now() - Duration::days(DEFAULT_LOOKBACK_DAYS)),
        };

        let now = Utc::now();
        let mut from = start;
        while from < now {
            let to = (from + Duration::days(COINBASE_CHUNK_DAYS)).min(now);
            let candles =
                coinbase::fetch_hourly(&self.http, &self.coinbase_api_url, product_id, from, to)
                    .await?;
            for c in &candles {
                sqlx::query(
                    r#"INSERT INTO price_candles (mint, "interval", bucket_ts, close_usd, source)
                       VALUES ($1, '1h', $2, $3, 'coinbase')
                       ON CONFLICT (mint, "interval", bucket_ts)
                       DO UPDATE SET close_usd = EXCLUDED.close_usd, source = EXCLUDED.source"#,
                )
                .bind(mint)
                .bind(c.bucket_ts)
                .bind(c.close_usd)
                .execute(pool)
                .await?;
            }
            tracing::debug!(mint, product_id, candles = candles.len(), %from, %to, "coinbase chunk");
            from = to + Duration::seconds(1);
        }
        Ok(())
    }

    /// Constant-price feeds (test stables): one candle at epoch is enough —
    /// every price lookup is "latest candle at-or-before the bucket".
    pub async fn ensure_const_candles(&self, pool: &PgPool) -> anyhow::Result<()> {
        for (mint, feed) in self.feeds.iter() {
            let PriceFeed::Const(value) = feed else {
                continue;
            };
            sqlx::query(
                r#"INSERT INTO price_candles (mint, "interval", bucket_ts, close_usd, source)
                   VALUES ($1, '1h', 'epoch'::timestamptz, $2, 'const')
                   ON CONFLICT (mint, "interval", bucket_ts)
                   DO UPDATE SET close_usd = EXCLUDED.close_usd"#,
            )
            .bind(mint)
            .bind(value)
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

/// The earliest activity any indexer has stored (each one's
/// `earliest_activity`), the anchor of a feed's cold start. An indexer that
/// cannot answer fails the call, as one unreadable table failed analytics'
/// `LEAST(...)` query.
pub async fn earliest_activity(
    pool: &PgPool,
    indexers: &[Arc<dyn DynIndexer>],
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let mut earliest: Option<DateTime<Utc>> = None;
    for ix in indexers {
        let ts = ix
            .earliest_activity(pool)
            .await
            .with_context(|| format!("earliest activity of indexer {}", ix.id()))?;
        earliest = match (earliest, ts) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    Ok(earliest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_coinbase_and_const_feeds_by_lowercased_mint() {
        let feeds = parse_price_feeds(r#"{"ABCD":"coinbase:BTC-USD","ef01":"const:1.0"}"#).unwrap();
        assert!(matches!(feeds.get("abcd"), Some(PriceFeed::Coinbase(p)) if p == "BTC-USD"));
        assert!(matches!(feeds.get("ef01"), Some(PriceFeed::Const(v)) if *v == 1.0));
    }

    #[test]
    fn rejects_retired_and_malformed_specs() {
        assert!(parse_price_feeds(r#"{"ab":"pyth:Crypto.BTC/USD"}"#).is_err());
        assert!(parse_price_feeds(r#"{"ab":"coinbase:"}"#).is_err());
        assert!(parse_price_feeds(r#"{"ab":"const:one"}"#).is_err());
        assert!(parse_price_feeds("not json").is_err());
    }

    #[test]
    fn an_unset_feed_map_is_empty() {
        assert!(parse_price_feeds("{}").unwrap().is_empty());
    }
}
