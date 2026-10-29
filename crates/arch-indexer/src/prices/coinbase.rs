//! Coinbase Exchange public candles — the source behind `coinbase:<product-id>`
//! feeds (e.g. `coinbase:BTC-USD`). Keyless, and the hourly closes are the
//! same spot most consumers already sanity-check against.
//!
//!   GET {base}/products/{product_id}/candles
//!       ?granularity=3600&start=<rfc3339>&end=<rfc3339>
//!   → [[time, low, high, open, close, volume], …]
//!
//! Rows arrive newest first; `start`/`end` are both inclusive; a window wider
//! than 300 candles is a 400 (callers chunk, see `feeds.rs`), and so is a
//! `start` in the future or a request without a User-Agent (set on the
//! shared client, [`super::FeedSync::new`]). The in-progress hour is included
//! with its close-so-far, which is why callers resume from the last bucket
//! inclusive.

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};

pub struct Candle {
    pub bucket_ts: DateTime<Utc>,
    pub close_usd: f64,
}

/// `[time, low, high, open, close, volume]` as Coinbase serializes it.
type Row = (i64, f64, f64, f64, f64, f64);

/// Hourly closes for `product_id` over [from, to], oldest first.
pub async fn fetch_hourly(
    http: &reqwest::Client,
    base_url: &str,
    product_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let url = format!(
        "{}/products/{product_id}/candles",
        base_url.trim_end_matches('/')
    );
    let resp = http
        .get(url)
        .query(&[
            ("granularity", "3600"),
            ("start", &from.to_rfc3339_opts(SecondsFormat::Secs, true)),
            ("end", &to.to_rfc3339_opts(SecondsFormat::Secs, true)),
        ])
        .send()
        .await?;
    if !resp.status().is_success() {
        // The body carries the reason ("granularity too small…", "NotFound"),
        // which the status alone hides.
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("coinbase candles {status} for {product_id}: {body}");
    }
    let rows: Vec<Row> = resp.json().await?;
    Ok(candles(rows))
}

/// Newest-first rows → oldest-first candles, dropping anything that is not a
/// finite close on a valid timestamp.
fn candles(mut rows: Vec<Row>) -> Vec<Candle> {
    rows.sort_by_key(|r| r.0);
    rows.into_iter()
        .filter_map(|(t, _low, _high, _open, close, _volume)| {
            let bucket_ts = Utc.timestamp_opt(t, 0).single()?;
            close.is_finite().then_some(Candle {
                bucket_ts,
                close_usd: close,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_wire_shape_oldest_first() {
        // Verbatim shape from GET /products/BTC-USD/candles?granularity=3600.
        let body = r#"[[1788350400,76563.51,77013,76778.49,76563.52,139.33363502],
                       [1788346800,76305,76801.67,76636.76,76778.49,223.51385516]]"#;
        let rows: Vec<Row> = serde_json::from_str(body).unwrap();
        let c = candles(rows);
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].bucket_ts.timestamp(), 1788346800);
        assert_eq!(c[0].close_usd, 76778.49);
        assert_eq!(c[1].bucket_ts.timestamp(), 1788350400);
        assert_eq!(c[1].close_usd, 76563.52);
    }

    #[test]
    fn empty_window_is_no_candles() {
        let rows: Vec<Row> = serde_json::from_str("[]").unwrap();
        assert!(candles(rows).is_empty());
    }
}
