//! Query-time chart helpers shared by every indexer's chart routes: bucketed
//! windows capped at [`MAX_BUCKETS`], the `{points, meta}` response and the
//! named-column merge.

use super::ApiError;
use axum::http::StatusCode;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct ChartParams {
    /// Defaulted because candle endpoints take no metric.
    #[serde(default)]
    pub metric: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

fn default_interval() -> String {
    "1h".into()
}

/// Cap the requested window so a caller can't coerce `generate_series` into
/// emitting millions of buckets (a DoS vector on a public endpoint). At most
/// MAX_BUCKETS points per request; an over-wide window is clamped forward from
/// `to`.
pub const MAX_BUCKETS: i32 = 1500;

/// [`clamp_window`] for the `1h`/`1d` chart intervals; any other is refused.
pub fn clamp_range(
    interval: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ApiError> {
    let bucket = match interval {
        "1h" => Duration::hours(1),
        "1d" => Duration::days(1),
        _ => return Err((StatusCode::BAD_REQUEST, "interval must be 1h or 1d".into())),
    };
    clamp_window(bucket, from, to)
}

pub fn clamp_window(
    bucket: Duration,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ApiError> {
    if to <= from {
        return Err((StatusCode::BAD_REQUEST, "`to` must be after `from`".into()));
    }
    let earliest = to - bucket * MAX_BUCKETS;
    Ok((from.max(earliest), to))
}

/// The SQL bucket expression over a `ts` column for a chart interval.
pub fn bucket_expr(interval: &str) -> Result<&'static str, ApiError> {
    match interval {
        "1h" => Ok("date_trunc('hour', ts)"),
        "1d" => Ok("date_trunc('day', ts)"),
        _ => Err((StatusCode::BAD_REQUEST, "interval must be 1h or 1d".into())),
    }
}

/// Buckets align to the Unix epoch, so every interval that divides a day
/// opens on UTC midnight.
pub fn bucket_open(ts: DateTime<Utc>, step: Duration) -> DateTime<Utc> {
    let secs = step.num_seconds();
    DateTime::from_timestamp(ts.timestamp().div_euclid(secs) * secs, 0).unwrap_or(ts)
}

/// `lo`/`hi` are the bucket's true extremes where the series computes them;
/// series read straight from SQL leave them absent and serve `{t, v}`.
#[derive(Debug, sqlx::FromRow)]
pub struct Point {
    pub t: DateTime<Utc>,
    pub v: Option<BigDecimal>,
    #[sqlx(default)]
    pub lo: Option<BigDecimal>,
    #[sqlx(default)]
    pub hi: Option<BigDecimal>,
}

pub fn chart_response(
    points: Vec<Point>,
    p: &ChartParams,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Value {
    json!({
        "points": points
            .into_iter()
            .map(|pt| {
                let mut point = json!({ "t": pt.t, "v": pt.v });
                if let (Some(lo), Some(hi)) = (pt.lo, pt.hi) {
                    point["lo"] = json!(lo);
                    point["hi"] = json!(hi);
                }
                point
            })
            .collect::<Vec<_>>(),
        "meta": { "metric": p.metric, "interval": p.interval, "from": from, "to": to }
    })
}

/// Merge single-value series into one row per bucket with named columns.
/// Missing buckets become explicit nulls so every row carries every column —
/// consumers forward-fill levels and zero-fill flows exactly as they do for
/// the single-series charts.
pub fn merge_series(series: Vec<(&'static str, Vec<Point>)>) -> Vec<Value> {
    use std::collections::BTreeMap;
    let names: Vec<&'static str> = series.iter().map(|(n, _)| *n).collect();
    let mut by_t: BTreeMap<DateTime<Utc>, serde_json::Map<String, Value>> = BTreeMap::new();
    for (name, points) in series {
        for p in points {
            by_t.entry(p.t).or_default().insert(name.into(), json!(p.v));
        }
    }
    by_t.into_iter()
        .map(|(t, mut cols)| {
            let mut obj = serde_json::Map::new();
            obj.insert("t".into(), json!(t));
            for n in &names {
                obj.insert((*n).into(), cols.remove(*n).unwrap_or(Value::Null));
            }
            Value::Object(obj)
        })
        .collect()
}

#[derive(Debug, Deserialize)]
pub struct SeriesParams {
    /// `1h` (the default) or `1d`.
    pub interval: Option<String>,
    /// Default: the bucket of the earliest action.
    pub from: Option<DateTime<Utc>>,
    /// Default, and at most: now.
    pub to: Option<DateTime<Utc>>,
}

pub struct Window<'a> {
    pub interval: &'a str,
    pub step: Duration,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// The window asked for, `from` defaulting to `first`'s bucket: `from`
/// aligned to the bucket, at most `MAX_BUCKETS` buckets back from `to`, `to`
/// at most now.
pub fn series_window(
    p: &SeriesParams,
    first: Option<DateTime<Utc>>,
) -> Result<Window<'_>, ApiError> {
    let interval = p.interval.as_deref().unwrap_or("1h");
    let step = match interval {
        "1h" => Duration::hours(1),
        "1d" => Duration::days(1),
        _ => return Err((StatusCode::BAD_REQUEST, "interval must be 1h or 1d".into())),
    };
    // Past now a bucket would repeat the current state as history.
    let to = p.to.unwrap_or_else(Utc::now).min(Utc::now());
    let from = p.from.or(first.filter(|f| *f < to)).unwrap_or(to - step);
    let (from, to) = clamp_window(step, from, to)?;
    Ok(Window {
        interval,
        step,
        from: bucket_open(from, step),
        to,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_bounds_wide_ranges_and_rejects_bad_input() {
        let to = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        // A 100-year window at 1h is clamped to MAX_BUCKETS hours, anchored to `to`.
        let from = to - Duration::days(365 * 100);
        let (f, t) = clamp_range("1h", from, to).unwrap();
        assert_eq!(t, to);
        assert_eq!(t - f, Duration::hours(MAX_BUCKETS as i64));
        // A window already within budget is left untouched.
        let small = to - Duration::hours(10);
        assert_eq!(clamp_range("1h", small, to).unwrap().0, small);
        // Bad interval and inverted range are rejected.
        assert!(clamp_range("1w", from, to).is_err());
        assert!(clamp_range("1h", to, from).is_err());
        // The candle intervals share the same cap.
        let (f, _) = clamp_window(Duration::minutes(5), from, to).unwrap();
        assert_eq!(to - f, Duration::minutes(5 * MAX_BUCKETS as i64));
    }

    /// A 4h candle opens on the UTC grid (00:00, 04:00, …), not at its first
    /// swap, and a day opens at midnight.
    #[test]
    fn buckets_open_on_the_utc_grid() {
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        let open = |s, step| bucket_open(at(s), step);
        assert_eq!(
            open("2026-09-29T05:59:59Z", Duration::hours(4)),
            at("2026-09-29T04:00:00Z")
        );
        assert_eq!(
            open("2026-09-29T08:00:00Z", Duration::hours(4)),
            at("2026-09-29T08:00:00Z")
        );
        assert_eq!(
            open("2026-09-29T00:14:59.999Z", Duration::minutes(15)),
            at("2026-09-29T00:00:00Z")
        );
        assert_eq!(
            open("2026-09-29T23:59:59Z", Duration::days(1)),
            at("2026-09-29T00:00:00Z")
        );
    }

    /// Every merged row must carry every column (missing buckets as explicit
    /// nulls) in bucket order — a stacked chart reads the rows positionally,
    /// and a silently absent column would shift a stream into the wrong band.
    #[test]
    fn merge_series_aligns_buckets_and_nulls_gaps() {
        let t0 = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let t1 = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let pt = |t, v: i64| Point {
            t,
            v: Some(BigDecimal::from(v)),
            lo: None,
            hi: None,
        };
        // "a" covers both buckets; "b" only the second and arrives unsorted.
        let rows = merge_series(vec![
            ("a", vec![pt(t0, 1), pt(t1, 2)]),
            ("b", vec![pt(t1, 30)]),
        ]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["t"], json!(t0));
        assert_eq!(rows[0]["a"], json!(BigDecimal::from(1)));
        assert_eq!(rows[0]["b"], Value::Null);
        assert_eq!(rows[1]["a"], json!(BigDecimal::from(2)));
        assert_eq!(rows[1]["b"], json!(BigDecimal::from(30)));
    }
}
