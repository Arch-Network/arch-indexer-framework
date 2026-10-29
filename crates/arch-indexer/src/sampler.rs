//! The periodic hook loops: every indexer's `sample` hook every
//! `SAMPLE_INTERVAL_SECS`, and the enrich pass every `ENRICH_INTERVAL_SECS`
//! (see [`Enricher::enrich_once`]), in registration order. Event-triggered
//! sampling is the runner's `on_activity`, on the ingest cadence.

use crate::indexer::{Ctx, DynIndexer};
use crate::prices::{FeedSync, PriceCtx};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub struct Sampler {
    pub ctx: Ctx,
    pub indexers: Vec<Arc<dyn DynIndexer>>,
}

impl Sampler {
    /// One sweep. Each indexer is best-effort: one that fails is logged and
    /// the others still sample.
    pub async fn sample_once(&self) {
        for ix in &self.indexers {
            if let Err(err) = ix.sample(&self.ctx).await {
                tracing::error!(indexer = ix.id(), %err, "sample failed");
            }
        }
    }

    pub async fn run_forever(&self, interval: Duration) -> anyhow::Result<()> {
        loop {
            self.sample_once().await;
            tokio::time::sleep(interval).await;
        }
    }
}

pub struct Enricher {
    pub ctx: Ctx,
    pub indexers: Vec<Arc<dyn DynIndexer>>,
    pub feeds: FeedSync,
}

impl Enricher {
    /// One pass, in three phases, each step best-effort (a failure is logged
    /// and the rest still run):
    /// 1. the runtime's feed candles: Coinbase, then constants;
    /// 2. each indexer's `derive_prices`, told the feed mints and the mints
    ///    every other indexer claims (`priced_mints`);
    /// 3. each indexer's `enrich`, which stamps USD from the candles above.
    pub async fn enrich_once(&self) {
        if let Err(err) = self
            .feeds
            .sync_coinbase(&self.ctx.pool, &self.indexers)
            .await
        {
            tracing::error!(%err, "feed candle sync failed");
        }
        if let Err(err) = self.feeds.ensure_const_candles(&self.ctx.pool).await {
            tracing::error!(%err, "const candle sync failed");
        }
        self.derive_prices().await;
        for ix in &self.indexers {
            if let Err(err) = ix.enrich(&self.ctx).await {
                tracing::error!(indexer = ix.id(), %err, "enrich failed");
            }
        }
    }

    /// Phase 2. An indexer that cannot report its claims stops the phase for
    /// this pass: without them another indexer could price a claimed mint
    /// and fight its owner over the candles.
    async fn derive_prices(&self) {
        let mut claims: Vec<HashSet<String>> = Vec::with_capacity(self.indexers.len());
        for ix in &self.indexers {
            match ix.priced_mints(&self.ctx).await {
                Ok(mints) => claims.push(mints.into_iter().collect()),
                Err(err) => {
                    tracing::error!(
                        indexer = ix.id(),
                        %err,
                        "priced_mints failed; no indexer derives prices this pass"
                    );
                    return;
                }
            }
        }
        for (i, a) in claims.iter().enumerate() {
            for (j, b) in claims.iter().enumerate().skip(i + 1) {
                for mint in a.intersection(b) {
                    tracing::warn!(
                        mint,
                        first = self.indexers[i].id(),
                        second = self.indexers[j].id(),
                        "two indexers claim one mint; both derive its candles"
                    );
                }
            }
        }
        let feed_mints = self.feeds.feed_mints();
        for (i, ix) in self.indexers.iter().enumerate() {
            let prices = PriceCtx {
                feed_mints: feed_mints.clone(),
                claimed_mints: claims
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .flat_map(|(_, c)| c.iter().cloned())
                    .collect(),
            };
            if let Err(err) = ix.derive_prices(&self.ctx, &prices).await {
                tracing::error!(indexer = ix.id(), %err, "price derivation failed");
            }
        }
    }

    pub async fn run_forever(&self, interval: Duration) -> anyhow::Result<()> {
        loop {
            self.enrich_once().await;
            tokio::time::sleep(interval).await;
        }
    }
}
