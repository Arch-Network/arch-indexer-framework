//! Helpers for an indexer's `enrich` hook.
//!
//! USD stamping is batched and idempotent: a batch touches only rows whose
//! USD column is still NULL and whose mint has a candle at or before the
//! row's hour, so re-running is free and a price-spine repair is "NULL the
//! columns, run again". [`drain`] runs one such batch until it stamps
//! nothing.

use std::future::Future;

/// Rows one stamping batch updates (`LIMIT $1` of its unstamped subquery).
pub const BATCH: i64 = 10_000;

/// Run `batch` until it reports 0 rows, logging each nonzero batch as
/// `what` (e.g. `"stamped usd on swaps"`). Returns the rows stamped in all.
///
/// A batch must make progress on every nonzero return (stamp the rows it
/// counts), or this never ends. An error stops the drain and is returned, so
/// a later batch in the caller's sequence can still run if it chooses.
pub async fn drain<F, Fut>(what: &str, mut batch: F) -> anyhow::Result<u64>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<u64>>,
{
    let mut total = 0;
    loop {
        let n = batch().await?;
        if n == 0 {
            return Ok(total);
        }
        tracing::info!(rows = n, "{what}");
        total += n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn drains_until_a_batch_stamps_nothing() {
        let left = AtomicU64::new(25_000);
        let calls = AtomicU64::new(0);
        let stamped = drain("stamped usd on test rows", || async {
            calls.fetch_add(1, Ordering::Relaxed);
            let n = left.load(Ordering::Relaxed).min(BATCH as u64);
            left.fetch_sub(n, Ordering::Relaxed);
            Ok(n)
        })
        .await
        .unwrap();
        assert_eq!(stamped, 25_000);
        // 10 000 + 10 000 + 5 000, then the empty batch that ends it.
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn an_error_stops_the_drain() {
        let calls = AtomicU64::new(0);
        let err = drain("stamped", || async {
            if calls.fetch_add(1, Ordering::Relaxed) == 1 {
                anyhow::bail!("no candles table");
            }
            Ok(1)
        })
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "no candles table");
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
