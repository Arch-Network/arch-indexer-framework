//! The periodic hook loops: every indexer's `sample` hook every
//! `SAMPLE_INTERVAL_SECS`, and its `enrich` hook every `ENRICH_INTERVAL_SECS`,
//! in registration order. Event-triggered sampling is the runner's
//! `on_activity`, on the ingest cadence.

use crate::indexer::{Ctx, DynIndexer};
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
}

impl Enricher {
    /// One pass. Each indexer is best-effort: one that fails is logged and
    /// the others still enrich.
    pub async fn enrich_once(&self) {
        for ix in &self.indexers {
            if let Err(err) = ix.enrich(&self.ctx).await {
                tracing::error!(indexer = ix.id(), %err, "enrich failed");
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
