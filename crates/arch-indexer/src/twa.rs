//! Time-weighted bucketing of piecewise-constant series.
//!
//! A lending bank's state is a step function: it holds each observed value
//! until the next observation (an event-sourced point at its block time, or a
//! sampler poll). A bucket that serves whichever step was last before its
//! close misdraws transients twice over — a two-minute deposit-and-withdraw
//! becomes a full bucket when it straddles the close and becomes nothing when
//! it does not. Each bucket here is instead the time-weighted average of the
//! steps inside it — the quantity whose integral is what the bank actually
//! held and accrued over the bucket — beside the true instantaneous extremes,
//! so a transient always shows, weighted by how long it actually stood.

use chrono::{DateTime, Duration, Utc};

/// One observation of the series: the value that holds from `ts` until the
/// next sample. `None` is a gap — a stretch where the series is undefined
/// (a bank with zero assets has no utilization) — which contributes neither
/// weight nor extremes.
pub struct Sample {
    pub ts: DateTime<Utc>,
    pub value: Option<f64>,
}

/// One bucket: its open `t`, the time-weighted average `v` over the covered
/// part of the bucket, and the extremes `lo`/`hi` of every value that stood
/// inside it — including values that stood for zero seconds (two samples in
/// the same second: the sharpest deposit-and-withdraw transient there is).
/// `v` is clamped into `[lo, hi]`: the mean of the steps lies there by
/// construction, and the clamp keeps the served literals honoring it where
/// float accumulation would drift the mean an ulp past an extreme.
pub struct Bucket {
    pub t: DateTime<Utc>,
    pub v: f64,
    pub lo: f64,
    pub hi: f64,
}

/// Bucket `samples` (ascending by `ts`; at most one before `grid_start`
/// matters, the carried-in boundary) over buckets opening at `grid_start`,
/// `grid_start + step`, … while the open is at or before `to`. Each bucket
/// integrates over `[open, min(open + step, to))` — the still-open bucket
/// averages what has elapsed, not a full width — and divides by the covered
/// time, so a bucket that starts mid-way through the first-ever observation
/// averages the observed part alone. A bucket nothing covers is absent, as
/// the close-sampled series left it.
pub fn twa_buckets(
    samples: &[Sample],
    grid_start: DateTime<Utc>,
    to: DateTime<Utc>,
    step: Duration,
) -> Vec<Bucket> {
    // Segment i holds samples[i].value over [samples[i].ts, next.ts or `to`).
    let ends = samples
        .iter()
        .skip(1)
        .map(|s| s.ts)
        .chain(std::iter::once(to));
    let segments: Vec<(DateTime<Utc>, DateTime<Utc>, Option<f64>)> = samples
        .iter()
        .zip(ends)
        .map(|(s, end)| (s.ts, end.max(s.ts), s.value))
        .collect();

    let mut buckets = Vec::new();
    let mut first = 0usize;
    let mut open = grid_start;
    while open <= to {
        let close = (open + step).min(to);
        while first < segments.len() && segments[first].1 < open {
            first += 1;
        }
        let mut weighted = 0f64;
        let mut covered = 0f64;
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for &(start, end, value) in &segments[first..] {
            if start >= close {
                break;
            }
            let Some(value) = value else { continue };
            let w = ((end.min(close) - start.max(open)).num_milliseconds() as f64) / 1000.0;
            let instant = start == end && start >= open;
            if w <= 0.0 && !instant {
                continue;
            }
            weighted += value * w.max(0.0);
            covered += w.max(0.0);
            lo = lo.min(value);
            hi = hi.max(value);
        }
        if covered > 0.0 {
            buckets.push(Bucket {
                t: open,
                v: (weighted / covered).clamp(lo, hi),
                lo,
                hi,
            });
        }
        open += step;
    }
    buckets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(minute: i64) -> DateTime<Utc> {
        "2026-09-24T00:00:00Z".parse::<DateTime<Utc>>().unwrap() + Duration::minutes(minute)
    }

    fn sample(minute: i64, value: f64) -> Sample {
        Sample {
            ts: at(minute),
            value: Some(value),
        }
    }

    fn buckets(samples: &[Sample], to_minute: i64) -> Vec<Bucket> {
        twa_buckets(samples, at(0), at(to_minute), Duration::hours(1))
    }

    /// A $1M deposit two minutes before a bucket close, withdrawn two minutes
    /// after it: close-sampling drew a full $1M hour; the average weighs it
    /// at 2/60ths in each bucket, and the extremes still carry the peak.
    #[test]
    fn a_transient_straddling_the_close_weighs_its_minutes() {
        let s = [sample(0, 10.0), sample(58, 1_000_010.0), sample(62, 10.0)];
        let b = buckets(&s, 120);
        assert_eq!(b.len(), 2);
        let expected0 = (10.0 * 58.0 + 1_000_010.0 * 2.0) / 60.0;
        assert!((b[0].v - expected0).abs() < 1e-6, "v: {}", b[0].v);
        assert_eq!((b[0].lo, b[0].hi), (10.0, 1_000_010.0));
        let expected1 = (1_000_010.0 * 2.0 + 10.0 * 58.0) / 60.0;
        assert!((b[1].v - expected1).abs() < 1e-6, "v: {}", b[1].v);
        assert_eq!((b[1].lo, b[1].hi), (10.0, 1_000_010.0));
    }

    /// The same transient wholly inside one bucket: close-sampling never saw
    /// it at all; the average and the extremes both do.
    #[test]
    fn a_transient_inside_a_bucket_is_never_invisible() {
        let s = [sample(0, 10.0), sample(20, 1_000_010.0), sample(24, 10.0)];
        let b = buckets(&s, 60);
        assert_eq!(b.len(), 1);
        let expected = (10.0 * 20.0 + 1_000_010.0 * 4.0 + 10.0 * 36.0) / 60.0;
        assert!((b[0].v - expected).abs() < 1e-6, "v: {}", b[0].v);
        assert_eq!((b[0].lo, b[0].hi), (10.0, 1_000_010.0));
    }

    /// Two samples in the same second — a deposit and its withdrawal in one
    /// transaction — give the middle value zero width; it still reaches the
    /// extremes, and only the last value of the instant carries forward.
    #[test]
    fn a_zero_width_instant_reaches_the_extremes_only() {
        let s = [
            sample(0, 10.0),
            Sample {
                ts: at(30),
                value: Some(1_000_010.0),
            },
            Sample {
                ts: at(30),
                value: Some(10.0),
            },
        ];
        let b = buckets(&s, 60);
        assert_eq!(b.len(), 1);
        assert!((b[0].v - 10.0).abs() < 1e-9, "v: {}", b[0].v);
        assert_eq!((b[0].lo, b[0].hi), (10.0, 1_000_010.0));
    }

    /// A quiet bucket carries the boundary value at full weight: the average
    /// of a constant is the constant, exactly as the close-sampled series
    /// forward-filled it.
    #[test]
    fn a_quiet_bucket_reads_the_carried_value() {
        let s = [Sample {
            ts: at(-90),
            value: Some(42.0),
        }];
        // Buckets open at 0 and 60; the bucket opening exactly at `to` has no
        // width and is absent.
        let b = buckets(&s, 120);
        assert_eq!(b.len(), 2);
        for bucket in &b {
            assert!((bucket.v - 42.0).abs() < 1e-9);
            assert_eq!((bucket.lo, bucket.hi), (42.0, 42.0));
        }
    }

    /// Buckets before the first observation are absent — the chart honestly
    /// starts at the first on-chain observation — and a bucket the first
    /// observation starts mid-way through averages the observed part alone.
    #[test]
    fn coverage_starts_at_the_first_observation() {
        let s = [sample(90, 7.0)];
        let b = buckets(&s, 180);
        assert_eq!(b.len(), 2);
        assert_eq!(b[0].t, at(60));
        assert!((b[0].v - 7.0).abs() < 1e-9);
        assert_eq!(b[1].t, at(120));
    }

    /// The still-open bucket averages the elapsed part, not a full width: a
    /// value that just changed moves the last point by its elapsed weight.
    #[test]
    fn the_open_bucket_averages_what_has_elapsed() {
        let s = [sample(0, 10.0), sample(70, 20.0)];
        let b = buckets(&s, 80);
        assert_eq!(b.len(), 2);
        let expected = (10.0 * 10.0 + 20.0 * 10.0) / 20.0;
        assert!((b[1].v - expected).abs() < 1e-9, "v: {}", b[1].v);
    }

    /// A gap (`None`) contributes neither weight nor extremes: a bucket that
    /// is all gap is absent, and one that is half gap averages the defined
    /// half.
    #[test]
    fn gaps_contribute_nothing() {
        let s = [
            Sample {
                ts: at(0),
                value: None,
            },
            sample(90, 5.0),
        ];
        let b = buckets(&s, 120);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].t, at(60));
        assert!((b[0].v - 5.0).abs() < 1e-9);
        assert_eq!((b[0].lo, b[0].hi), (5.0, 5.0));
    }

    /// A constant series must average to exactly itself, whatever the
    /// segment weights: accumulating value × seconds in floats can drift the
    /// mean an ulp past the constant on large values (observed at ~1e-8 on a
    /// hundred-million-unit bank), and the clamp into [lo, hi] repairs it.
    #[test]
    fn a_constant_series_averages_to_exactly_itself() {
        let value = 100_000_100.000_428_f64;
        let samples: Vec<Sample> = (0..12)
            .map(|i| Sample {
                ts: at(0) + Duration::seconds(i * 300 + 7),
                value: Some(value),
            })
            .collect();
        let b = twa_buckets(
            &samples,
            at(0),
            at(60) + Duration::seconds(29),
            Duration::hours(1),
        );
        assert_eq!(b.len(), 2);
        for bucket in &b {
            assert_eq!(bucket.v, value, "v must equal the constant to the bit");
            assert_eq!((bucket.lo, bucket.hi), (value, value));
        }
    }

    /// A sample exactly on a bucket's open belongs to that bucket, not the
    /// one before it — the strict bound the close-sampled series kept.
    #[test]
    fn a_sample_on_the_boundary_opens_the_next_bucket() {
        let s = [sample(0, 1.0), sample(60, 2.0)];
        let b = buckets(&s, 120);
        assert_eq!(b.len(), 2);
        assert!((b[0].v - 1.0).abs() < 1e-9);
        assert_eq!((b[0].lo, b[0].hi), (1.0, 1.0));
        assert!((b[1].v - 2.0).abs() < 1e-9);
        assert_eq!((b[1].lo, b[1].hi), (2.0, 2.0));
    }
}
